//! Integration tests for ReadPrecompileCalls storage operations
//! 
//! Tests the storage layer to ensure:
//! - ReadPrecompileCalls data persists correctly
//! - HlExtras serialization/deserialization works
//! - Storage operations maintain data integrity
//! - Concurrent access is handled properly

use std::{collections::HashMap, sync::Arc};

use alloy_primitives::{Address, Bytes, B256};
use eyre::Result;
use reth_db::{
    cursor::{DbCursorRO, DbCursorRW},
    transaction::{DbTx, DbTxMut},
    DatabaseEnv,
};
use reth_provider::{
    providers::{DatabaseProvider, StaticFileProvider},
    BlockBodyReader, BlockBodyWriter, ProviderResult, ReadBodyInput, StorageLocation,
    test_utils::create_test_rw_db,
};
use tempfile::TempDir;
use tokio::sync::Mutex;

use nanoreth::{
    node::{
        storage::{tables::BlockReadPrecompileCalls, HlStorage},
        types::{HlExtras, ReadPrecompileCall, ReadPrecompileCalls, ReadPrecompileInput, ReadPrecompileResult},
    },
    HlBlock, HlBlockBody, HlPrimitives,
};

fn create_test_precompile_calls() -> ReadPrecompileCalls {
    let addr1 = Address::from([0x09; 20]);
    let addr2 = Address::from([0x0a; 20]);
    
    let input1 = ReadPrecompileInput {
        input: Bytes::from_static(b"blake2f_input"),
        gas_limit: 12,
    };
    let result1 = ReadPrecompileResult::Ok {
        gas_used: 10,
        bytes: Bytes::from_static(b"blake2f_output"),
    };
    
    let input2 = ReadPrecompileInput {
        input: Bytes::from_static(b"modexp_input"),
        gas_limit: 200000,
    };
    let result2 = ReadPrecompileResult::OutOfGas;
    
    ReadPrecompileCalls(vec![
        (addr1, vec![(input1.clone(), result1), (input1, ReadPrecompileResult::Error)]),
        (addr2, vec![(input2, result2)]),
    ])
}

fn create_test_hl_extras() -> HlExtras {
    HlExtras {
        read_precompile_calls: Some(create_test_precompile_calls()),
        highest_precompile_address: Some(Address::from([0x0a; 20])),
    }
}

#[tokio::test]
async fn test_write_and_read_precompile_calls() -> Result<()> {
    let db = create_test_rw_db();
    let storage = HlStorage::default();
    
    let provider = DatabaseProvider::new(db.tx_mut()?, Default::default());
    
    // Test data
    let block_number = 1000000u64;
    let extras = create_test_hl_extras();
    let inputs = vec![(block_number, extras.clone())];
    
    // Write precompile calls
    storage.write_precompile_calls(&provider, inputs)?;
    provider.commit()?;
    
    // Read back and verify
    let read_provider = DatabaseProvider::new(db.tx()?, Default::default());
    let mock_block = create_mock_hl_block(block_number);
    let read_inputs = vec![(mock_block.header(), vec![])];
    
    let read_extras = storage.read_precompile_calls(&read_provider, &read_inputs)?;
    
    assert_eq!(read_extras.len(), 1);
    assert_eq!(read_extras[0], extras);
    
    Ok(())
}

#[tokio::test]
async fn test_write_block_bodies_with_precompile_calls() -> Result<()> {
    let db = create_test_rw_db();
    let storage = HlStorage::default();
    
    let provider = DatabaseProvider::new(db.tx_mut()?, Default::default());
    
    // Create test block body with precompile calls
    let block_number = 1000001u64;
    let extras = create_test_hl_extras();
    
    let block_body = HlBlockBody {
        inner: reth_primitives::BlockBody {
            transactions: vec![],
            withdrawals: None,
            ommers: vec![],
        },
        sidecars: None,
        read_precompile_calls: extras.read_precompile_calls.clone(),
        highest_precompile_address: extras.highest_precompile_address,
    };
    
    let bodies = vec![(block_number, Some(block_body))];
    
    // Write block bodies
    storage.write_block_bodies(&provider, bodies, StorageLocation::Database)?;
    provider.commit()?;
    
    // Read back and verify
    let read_provider = DatabaseProvider::new(db.tx()?, Default::default());
    let mock_block = create_mock_hl_block(block_number);
    let read_inputs = vec![(mock_block.header(), vec![])];
    
    let read_bodies = storage.read_block_bodies(&read_provider, read_inputs)?;
    
    assert_eq!(read_bodies.len(), 1);
    assert_eq!(read_bodies[0].read_precompile_calls, extras.read_precompile_calls);
    assert_eq!(read_bodies[0].highest_precompile_address, extras.highest_precompile_address);
    
    Ok(())
}

#[tokio::test]
async fn test_remove_block_bodies_above() -> Result<()> {
    let db = create_test_rw_db();
    let storage = HlStorage::default();
    
    let provider = DatabaseProvider::new(db.tx_mut()?, Default::default());
    
    // Write multiple blocks
    let extras1 = create_test_hl_extras();
    let extras2 = HlExtras {
        read_precompile_calls: Some(ReadPrecompileCalls(vec![])),
        highest_precompile_address: None,
    };
    
    let inputs = vec![
        (1000000u64, extras1.clone()),
        (1000001u64, extras2.clone()),
        (1000002u64, extras1.clone()),
    ];
    
    storage.write_precompile_calls(&provider, inputs)?;
    provider.commit()?;
    
    // Remove blocks above 1000000
    let remove_provider = DatabaseProvider::new(db.tx_mut()?, Default::default());
    storage.remove_block_bodies_above(&remove_provider, 1000000, StorageLocation::Database)?;
    remove_provider.commit()?;
    
    // Verify only block 1000000 remains
    let read_provider = DatabaseProvider::new(db.tx()?, Default::default());
    let mut cursor = read_provider.tx_ref().cursor_read::<BlockReadPrecompileCalls>()?;
    
    let mut remaining_blocks = vec![];
    for result in cursor.walk(None)? {
        let (block_num, _) = result?;
        remaining_blocks.push(block_num);
    }
    
    assert_eq!(remaining_blocks, vec![1000000u64]);
    
    Ok(())
}

#[tokio::test]
async fn test_concurrent_storage_operations() -> Result<()> {
    let db = Arc::new(create_test_rw_db());
    let storage = HlStorage::default();
    
    // Spawn multiple concurrent write operations
    let mut handles = vec![];
    
    for i in 0..10 {
        let db_clone = db.clone();
        let storage_clone = storage.clone();
        
        let handle = tokio::spawn(async move {
            let provider = DatabaseProvider::new(db_clone.tx_mut().unwrap(), Default::default());
            
            let block_number = 1000000 + i;
            let extras = create_test_hl_extras();
            let inputs = vec![(block_number, extras)];
            
            storage_clone.write_precompile_calls(&provider, inputs).unwrap();
            provider.commit().unwrap();
        });
        
        handles.push(handle);
    }
    
    // Wait for all writes to complete
    for handle in handles {
        handle.await?;
    }
    
    // Verify all blocks were written
    let read_provider = DatabaseProvider::new(db.tx()?, Default::default());
    let mut cursor = read_provider.tx_ref().cursor_read::<BlockReadPrecompileCalls>()?;
    
    let mut block_count = 0;
    for result in cursor.walk(None)? {
        let _ = result?;
        block_count += 1;
    }
    
    assert_eq!(block_count, 10);
    
    Ok(())
}

#[tokio::test]
async fn test_large_precompile_calls_serialization() -> Result<()> {
    let db = create_test_rw_db();
    let storage = HlStorage::default();
    
    let provider = DatabaseProvider::new(db.tx_mut()?, Default::default());
    
    // Create large precompile calls data
    let mut large_calls = vec![];
    for i in 0..100 {
        let addr = Address::from([i as u8; 20]);
        let mut calls = vec![];
        
        for j in 0..50 {
            let input = ReadPrecompileInput {
                input: Bytes::from(vec![i as u8, j as u8; 1000]), // 1KB input each
                gas_limit: 100000,
            };
            let result = ReadPrecompileResult::Ok {
                gas_used: 50000,
                bytes: Bytes::from(vec![j as u8; 2000]), // 2KB output each
            };
            calls.push((input, result));
        }
        
        large_calls.push((addr, calls));
    }
    
    let large_precompile_calls = ReadPrecompileCalls(large_calls);
    let large_extras = HlExtras {
        read_precompile_calls: Some(large_precompile_calls.clone()),
        highest_precompile_address: Some(Address::from([99; 20])),
    };
    
    let block_number = 1000000u64;
    let inputs = vec![(block_number, large_extras.clone())];
    
    // Write large data
    let start = std::time::Instant::now();
    storage.write_precompile_calls(&provider, inputs)?;
    let write_time = start.elapsed();
    
    provider.commit()?;
    
    // Read back and verify
    let read_provider = DatabaseProvider::new(db.tx()?, Default::default());
    let mock_block = create_mock_hl_block(block_number);
    let read_inputs = vec![(mock_block.header(), vec![])];
    
    let start = std::time::Instant::now();
    let read_extras = storage.read_precompile_calls(&read_provider, &read_inputs)?;
    let read_time = start.elapsed();
    
    assert_eq!(read_extras.len(), 1);
    assert_eq!(read_extras[0], large_extras);
    
    // Performance assertions
    assert!(write_time.as_millis() < 1000, "Write took too long: {}ms", write_time.as_millis());
    assert!(read_time.as_millis() < 500, "Read took too long: {}ms", read_time.as_millis());
    
    Ok(())
}

#[tokio::test]
async fn test_precompile_calls_serialization_formats() -> Result<()> {
    let original_calls = create_test_precompile_calls();
    
    // Test msgpack serialization (used in storage)
    let msgpack_data = rmp_serde::to_vec(&original_calls.0)?;
    let deserialized: Vec<ReadPrecompileCall> = rmp_serde::from_slice(&msgpack_data)?;
    let restored_calls = ReadPrecompileCalls(deserialized);
    
    assert_eq!(original_calls, restored_calls);
    
    // Test RLP encoding/decoding
    use alloy_rlp::{Encodable, Decodable};
    let mut rlp_buf = vec![];
    original_calls.encode(&mut rlp_buf);
    
    let mut slice = rlp_buf.as_slice();
    let rlp_decoded = ReadPrecompileCalls::decode(&mut slice)?;
    
    assert_eq!(original_calls, rlp_decoded);
    
    Ok(())
}

#[tokio::test]
async fn test_storage_error_handling() -> Result<()> {
    let db = create_test_rw_db();
    let storage = HlStorage::default();
    
    // Test reading from non-existent block
    let read_provider = DatabaseProvider::new(db.tx()?, Default::default());
    let mock_block = create_mock_hl_block(9999999);
    let read_inputs = vec![(mock_block.header(), vec![])];
    
    let read_extras = storage.read_precompile_calls(&read_provider, &read_inputs)?;
    
    // Should return default values for non-existent data
    assert_eq!(read_extras.len(), 1);
    assert_eq!(read_extras[0].read_precompile_calls, None);
    assert_eq!(read_extras[0].highest_precompile_address, None);
    
    Ok(())
}

fn create_mock_hl_block(number: u64) -> HlBlock {
    use alloy_consensus::Header;
    use alloy_primitives::{Bloom, B64, U256};
    
    HlBlock {
        header: Header {
            parent_hash: B256::ZERO,
            ommers_hash: B256::ZERO,
            beneficiary: Address::ZERO,
            state_root: B256::ZERO,
            transactions_root: B256::ZERO,
            receipts_root: B256::ZERO,
            logs_bloom: Bloom::ZERO,
            difficulty: U256::ZERO,
            number,
            gas_limit: 30_000_000,
            gas_used: 0,
            timestamp: 1722633600,
            extra_data: Bytes::from_static(b"test"),
            mix_hash: B256::ZERO,
            nonce: B64::ZERO,
            base_fee_per_gas: Some(1000000000),
            withdrawals_root: None,
            blob_gas_used: None,
            excess_blob_gas: None,
            parent_beacon_block_root: None,
            requests_hash: None,
        },
        body: HlBlockBody {
            inner: reth_primitives::BlockBody {
                transactions: vec![],
                withdrawals: None,
                ommers: vec![],
            },
            sidecars: None,
            read_precompile_calls: None,
            highest_precompile_address: None,
        },
    }
}

#[cfg(test)]
mod stress_tests {
    use super::*;
    
    #[tokio::test]
    async fn test_storage_under_load() -> Result<()> {
        let db = Arc::new(create_test_rw_db());
        let storage = HlStorage::default();
        
        // Create many concurrent readers and writers
        let mut handles = vec![];
        
        // Spawn writers
        for i in 0..50 {
            let db_clone = db.clone();
            let storage_clone = storage.clone();
            
            let handle = tokio::spawn(async move {
                for j in 0..20 {
                    let provider = DatabaseProvider::new(db_clone.tx_mut().unwrap(), Default::default());
                    
                    let block_number = i * 1000 + j;
                    let extras = create_test_hl_extras();
                    let inputs = vec![(block_number, extras)];
                    
                    storage_clone.write_precompile_calls(&provider, inputs).unwrap();
                    provider.commit().unwrap();
                }
            });
            
            handles.push(handle);
        }
        
        // Wait for all operations
        for handle in handles {
            handle.await?;
        }
        
        // Verify all data was written correctly
        let read_provider = DatabaseProvider::new(db.tx()?, Default::default());
        let mut cursor = read_provider.tx_ref().cursor_read::<BlockReadPrecompileCalls>()?;
        
        let mut total_blocks = 0;
        for result in cursor.walk(None)? {
            let (_, data) = result?;
            let extras: HlExtras = rmp_serde::from_slice(&data)?;
            assert!(extras.read_precompile_calls.is_some());
            total_blocks += 1;
        }
        
        assert_eq!(total_blocks, 50 * 20);
        
        Ok(())
    }
}
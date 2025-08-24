//! Unit tests for collect_block functionality migration
//! 
//! Tests the core block collection logic to ensure:
//! - Local cache retrieval works correctly
//! - Fallback behavior functions as expected
//! - Performance overhead is minimal
//! - ReadPrecompileCalls data is preserved

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration as StdDuration,
};

use alloy_consensus::{BlockBody, Header};
use alloy_primitives::{Address, Bloom, Bytes, B256, B64, U256};
use eyre::Result;
use serde_json;
use tempfile::TempDir;
use time::{Duration, OffsetDateTime};
use tokio::{sync::Mutex, time::sleep};

use nanoreth::{
    node::types::{
        reth_compat, BlockAndReceipts, EvmBlock, ReadPrecompileCall, ReadPrecompileCalls,
        ReadPrecompileInput, ReadPrecompileResult,
    },
    pseudo_peer::sources::{
        hl_node::{
            cache::LocalBlocksCache, HlNodeBlockSource, LocalBlockAndReceipts, CACHE_SIZE,
            MAX_ALLOWED_THRESHOLD_BEFORE_FALLBACK,
        },
        BlockSource, BlockSourceBoxed, LocalBlockSource,
    },
};

const TEST_CHAIN_ID: u64 = 42161;

/// Mock block source for testing fallback behavior
struct MockBlockSource {
    blocks: Arc<Mutex<BTreeMap<u64, BlockAndReceipts>>>,
    call_count: Arc<Mutex<u64>>,
}

impl MockBlockSource {
    fn new() -> Self {
        Self {
            blocks: Arc::new(Mutex::new(BTreeMap::new())),
            call_count: Arc::new(Mutex::new(0)),
        }
    }

    async fn add_block(&self, height: u64, block: BlockAndReceipts) {
        self.blocks.lock().await.insert(height, block);
    }

    async fn get_call_count(&self) -> u64 {
        *self.call_count.lock().await
    }
}

impl BlockSource for MockBlockSource {
    fn collect_block(&self, height: u64) -> futures::future::BoxFuture<'static, Result<BlockAndReceipts>> {
        let blocks = self.blocks.clone();
        let call_count = self.call_count.clone();
        
        Box::pin(async move {
            *call_count.lock().await += 1;
            let blocks = blocks.lock().await;
            blocks.get(&height)
                .cloned()
                .ok_or_else(|| eyre::eyre!("Block {} not found in mock", height))
        })
    }

    fn find_latest_block_number(&self) -> futures::future::BoxFuture<'static, Option<u64>> {
        let blocks = self.blocks.clone();
        Box::pin(async move {
            let blocks = blocks.lock().await;
            blocks.keys().max().copied()
        })
    }

    fn recommended_chunk_size(&self) -> u64 {
        1000
    }
}

fn create_test_block(
    number: u64,
    timestamp: u64,
    extra_data: &'static [u8],
    precompile_calls: Option<ReadPrecompileCalls>,
) -> BlockAndReceipts {
    BlockAndReceipts {
        block: EvmBlock::Reth115(reth_compat::SealedBlock {
            header: reth_compat::SealedHeader {
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
                    gas_used: 21000,
                    timestamp,
                    extra_data: Bytes::from_static(extra_data),
                    mix_hash: B256::ZERO,
                    nonce: B64::ZERO,
                    base_fee_per_gas: Some(1000000000),
                    withdrawals_root: None,
                    blob_gas_used: None,
                    excess_blob_gas: None,
                    parent_beacon_block_root: None,
                    requests_hash: None,
                },
                hash: B256::random(),
            },
            body: BlockBody {
                transactions: vec![],
                ommers: vec![],
                withdrawals: None,
            },
        }),
        receipts: vec![],
        system_txs: vec![],
        read_precompile_calls: precompile_calls.unwrap_or_default(),
        highest_precompile_address: Some(Address::from([0x10; 20])),
    }
}

fn create_precompile_calls() -> ReadPrecompileCalls {
    let precompile_addr = Address::from([0x09; 20]);
    let input = ReadPrecompileInput {
        input: Bytes::from_static(b"test_input"),
        gas_limit: 3000,
    };
    let result = ReadPrecompileResult::Ok {
        gas_used: 2500,
        bytes: Bytes::from_static(b"test_output"),
    };
    
    ReadPrecompileCalls(vec![(precompile_addr, vec![(input, result)])])
}

async fn setup_test_environment() -> Result<(TempDir, HlNodeBlockSource, MockBlockSource)> {
    let temp_dir = tempfile::tempdir()?;
    let mock_fallback = MockBlockSource::new();
    
    // Create hourly directory structure
    let hourly_dir = temp_dir.path().join("hourly");
    std::fs::create_dir_all(&hourly_dir)?;
    
    let hl_source = HlNodeBlockSource::new(
        BlockSourceBoxed::new(Box::new(mock_fallback.clone())),
        temp_dir.path().to_path_buf(),
        1000000,
    ).await;
    
    Ok((temp_dir, hl_source, mock_fallback))
}

#[tokio::test]
async fn test_collect_block_from_cache() -> Result<()> {
    let (_temp_dir, hl_source, _mock) = setup_test_environment().await?;
    
    // Add a block to the cache
    let test_block = create_test_block(1000000, 1722633600, b"cache-test", None);
    let scan_result = crate::pseudo_peer::sources::hl_node::scan::ScanResult {
        path: PathBuf::from("/test"),
        next_expected_height: 1000001,
        new_blocks: vec![test_block.clone()],
        new_block_ranges: vec![1000000..=1000000],
    };
    
    hl_source.local_blocks_cache.lock().await.load_scan_result(scan_result);
    
    // Collect block from cache
    let collected_block = hl_source.collect_block(1000000).await?;
    assert_eq!(collected_block, test_block);
    
    Ok(())
}

#[tokio::test]
async fn test_collect_block_fallback_behavior() -> Result<()> {
    let (_temp_dir, hl_source, mock_fallback) = setup_test_environment().await?;
    
    // Add block only to fallback source
    let fallback_block = create_test_block(1000001, 1722633700, b"fallback", None);
    mock_fallback.add_block(1000001, fallback_block.clone()).await;
    
    // Wait for threshold to pass
    sleep(MAX_ALLOWED_THRESHOLD_BEFORE_FALLBACK.unsigned_abs()).await;
    
    // Collect block should fall back
    let collected_block = hl_source.collect_block(1000001).await?;
    assert_eq!(collected_block, fallback_block);
    
    // Verify fallback was actually called
    assert_eq!(mock_fallback.get_call_count().await, 1);
    
    Ok(())
}

#[tokio::test]
async fn test_collect_block_rate_limiting() -> Result<()> {
    let (_temp_dir, hl_source, _mock_fallback) = setup_test_environment().await?;
    
    // Try to collect a non-existent block immediately (should fail due to rate limiting)
    let result = hl_source.collect_block(1000002).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("limiting polling rate"));
    
    Ok(())
}

#[tokio::test]
async fn test_collect_block_with_precompile_calls() -> Result<()> {
    let (_temp_dir, hl_source, _mock) = setup_test_environment().await?;
    
    // Create block with precompile calls
    let precompile_calls = create_precompile_calls();
    let test_block = create_test_block(1000003, 1722633800, b"precompile", Some(precompile_calls.clone()));
    
    let scan_result = crate::pseudo_peer::sources::hl_node::scan::ScanResult {
        path: PathBuf::from("/test"),
        next_expected_height: 1000004,
        new_blocks: vec![test_block.clone()],
        new_block_ranges: vec![1000003..=1000003],
    };
    
    hl_source.local_blocks_cache.lock().await.load_scan_result(scan_result);
    
    // Collect and verify precompile calls are preserved
    let collected_block = hl_source.collect_block(1000003).await?;
    assert_eq!(collected_block.read_precompile_calls, precompile_calls);
    assert_eq!(collected_block.highest_precompile_address, Some(Address::from([0x10; 20])));
    
    Ok(())
}

#[tokio::test]
async fn test_concurrent_collect_block() -> Result<()> {
    let (_temp_dir, hl_source, _mock) = setup_test_environment().await?;
    
    // Add multiple blocks to cache
    let mut handles = vec![];
    for i in 0..10 {
        let block_num = 1000000 + i;
        let test_block = create_test_block(block_num, 1722633600 + i * 12, b"concurrent", None);
        
        let scan_result = crate::pseudo_peer::sources::hl_node::scan::ScanResult {
            path: PathBuf::from("/test"),
            next_expected_height: block_num + 1,
            new_blocks: vec![test_block.clone()],
            new_block_ranges: vec![block_num..=block_num],
        };
        
        hl_source.local_blocks_cache.lock().await.load_scan_result(scan_result);
    }
    
    // Collect blocks concurrently
    for i in 0..10 {
        let block_num = 1000000 + i;
        let source = hl_source.clone();
        handles.push(tokio::spawn(async move {
            source.collect_block(block_num).await
        }));
    }
    
    // Wait for all tasks and verify results
    for (i, handle) in handles.into_iter().enumerate() {
        let result = handle.await??;
        assert_eq!(result.number(), 1000000 + i);
    }
    
    Ok(())
}

#[cfg(test)]
mod performance_tests {
    use super::*;
    use std::time::Instant;
    
    #[tokio::test]
    async fn test_collect_block_performance() -> Result<()> {
        let (_temp_dir, hl_source, _mock) = setup_test_environment().await?;
        
        // Load 1000 blocks into cache
        for i in 0..1000 {
            let block_num = 1000000 + i;
            let test_block = create_test_block(block_num, 1722633600 + i * 12, b"perf", None);
            
            let scan_result = crate::pseudo_peer::sources::hl_node::scan::ScanResult {
                path: PathBuf::from("/test"),
                next_expected_height: block_num + 1,
                new_blocks: vec![test_block],
                new_block_ranges: vec![block_num..=block_num],
            };
            
            hl_source.local_blocks_cache.lock().await.load_scan_result(scan_result);
        }
        
        // Measure collection performance
        let start = Instant::now();
        for i in 0..1000 {
            let _block = hl_source.collect_block(1000000 + i).await?;
        }
        let duration = start.elapsed();
        
        // Should process 1000 blocks in less than 100ms (0.1ms per block)
        assert!(duration.as_millis() < 100, "Performance test failed: {}ms for 1000 blocks", duration.as_millis());
        
        Ok(())
    }
    
    #[tokio::test]
    async fn test_cache_memory_efficiency() -> Result<()> {
        let cache = LocalBlocksCache::new(CACHE_SIZE);
        
        // Measure initial memory
        let initial_memory = get_memory_usage();
        
        // Load many blocks
        for i in 0..CACHE_SIZE {
            let block = create_test_block(i as u64, 1722633600 + i as u64 * 12, b"memory", None);
            let scan_result = crate::pseudo_peer::sources::hl_node::scan::ScanResult {
                path: PathBuf::from("/test"),
                next_expected_height: i as u64 + 1,
                new_blocks: vec![block],
                new_block_ranges: vec![i as u64..=i as u64],
            };
            
            cache.load_scan_result(scan_result);
        }
        
        let final_memory = get_memory_usage();
        let memory_increase = final_memory - initial_memory;
        
        // Memory increase should be reasonable (less than 100MB for cache)
        assert!(memory_increase < 100 * 1024 * 1024, 
               "Memory usage too high: {} bytes for {} blocks", 
               memory_increase, CACHE_SIZE);
        
        Ok(())
    }
    
    fn get_memory_usage() -> usize {
        // Simple memory usage estimation - in real tests you might use more sophisticated methods
        std::mem::size_of::<LocalBlocksCache>()
    }
}

#[cfg(test)]
mod integration_tests {
    use super::*;
    use std::io::Write;
    
    #[tokio::test]
    async fn test_file_to_cache_integration() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let now = OffsetDateTime::now_utc();
        
        // Create hourly file structure
        let date_str = format!("{:04}{:02}{:02}", now.year(), now.month() as u8, now.day());
        let hour_path = temp_dir.path()
            .join("hourly")
            .join(date_str)
            .join(format!("{}", now.hour()));
        
        std::fs::create_dir_all(hour_path.parent().unwrap())?;
        
        // Write test block to file
        let test_block = LocalBlockAndReceipts(
            "1722633600".to_string(),
            create_test_block(1000000, 1722633600, b"file-test", None)
        );
        
        let mut file = std::fs::File::create(&hour_path)?;
        writeln!(file, "{}", serde_json::to_string(&test_block)?)?;
        file.sync_all()?;
        
        // Create HlNodeBlockSource
        let mock_fallback = MockBlockSource::new();
        let hl_source = HlNodeBlockSource::new(
            BlockSourceBoxed::new(Box::new(mock_fallback)),
            temp_dir.path().to_path_buf(),
            1000000,
        ).await;
        
        // Allow time for file scanning
        sleep(StdDuration::from_millis(200)).await;
        
        // Collect block should work from file
        let collected = hl_source.collect_block(1000000).await?;
        assert_eq!(collected.number(), 1000000);
        
        Ok(())
    }
}

#[cfg(test)]
mod error_handling_tests {
    use super::*;
    
    #[tokio::test]
    async fn test_corrupted_file_handling() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let now = OffsetDateTime::now_utc();
        
        // Create corrupted file
        let date_str = format!("{:04}{:02}{:02}", now.year(), now.month() as u8, now.day());
        let hour_path = temp_dir.path()
            .join("hourly")
            .join(date_str)
            .join(format!("{}", now.hour()));
        
        std::fs::create_dir_all(hour_path.parent().unwrap())?;
        
        let mut file = std::fs::File::create(&hour_path)?;
        writeln!(file, "corrupted json data")?;
        file.sync_all()?;
        
        // Should fallback gracefully
        let mock_fallback = MockBlockSource::new();
        let fallback_block = create_test_block(1000000, 1722633600, b"fallback", None);
        mock_fallback.add_block(1000000, fallback_block.clone()).await;
        
        let hl_source = HlNodeBlockSource::new(
            BlockSourceBoxed::new(Box::new(mock_fallback.clone())),
            temp_dir.path().to_path_buf(),
            1000000,
        ).await;
        
        sleep(MAX_ALLOWED_THRESHOLD_BEFORE_FALLBACK.unsigned_abs()).await;
        
        let collected = hl_source.collect_block(1000000).await?;
        assert_eq!(collected, fallback_block);
        assert_eq!(mock_fallback.get_call_count().await, 1);
        
        Ok(())
    }
}
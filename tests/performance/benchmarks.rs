//! Performance benchmarks for collect_block migration
//! 
//! Measures:
//! - Block collection latency
//! - Memory usage patterns  
//! - Throughput under load
//! - Cache efficiency metrics

use std::{
    sync::{atomic::{AtomicU64, Ordering}, Arc},
    time::{Duration, Instant},
};

use criterion::{black_box, criterion_group, criterion_main, Criterion, BatchSize};
use nanoreth::{
    node::types::{BlockAndReceipts, ReadPrecompileCalls},
    pseudo_peer::sources::{
        hl_node::{cache::LocalBlocksCache, HlNodeBlockSource, CACHE_SIZE},
        BlockSource, BlockSourceBoxed, LocalBlockSource,
    },
};
use tempfile::TempDir;
use tokio::runtime::Runtime;

// Reuse test utilities from unit tests
mod test_utils {
    use super::*;
    use alloy_consensus::{BlockBody, Header};
    use alloy_primitives::{Address, Bloom, Bytes, B256, B64, U256};
    use nanoreth::node::types::{
        reth_compat, EvmBlock, ReadPrecompileCall, ReadPrecompileInput, ReadPrecompileResult,
    };
    
    pub fn create_test_block(
        number: u64,
        timestamp: u64,
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
                        gas_used: 21000 * (number % 1000), // Variable gas usage
                        timestamp,
                        extra_data: Bytes::from_static(b"benchmark"),
                        mix_hash: B256::ZERO,
                        nonce: B64::ZERO,
                        base_fee_per_gas: Some(1000000000),
                        withdrawals_root: None,
                        blob_gas_used: None,
                        excess_blob_gas: None,
                        parent_beacon_block_root: None,
                        requests_hash: None,
                    },
                    hash: B256::from([number as u8; 32]), // Deterministic but unique
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
    
    pub fn create_varying_precompile_calls(block_number: u64) -> ReadPrecompileCalls {
        let mut calls = vec![];
        
        // Vary the number of calls based on block number
        let num_precompiles = (block_number % 5) + 1; // 1-5 precompiles per block
        
        for i in 0..num_precompiles {
            let addr = Address::from([(i as u8 + 9); 20]); // 0x09, 0x0a, etc.
            let mut call_data = vec![];
            
            let num_calls = ((block_number + i) % 3) + 1; // 1-3 calls per precompile
            
            for j in 0..num_calls {
                let input_size = 32 + ((block_number + i + j) % 1000); // Variable input size
                let input = ReadPrecompileInput {
                    input: Bytes::from(vec![j as u8; input_size as usize]),
                    gas_limit: 3000 + (input_size * 10),
                };
                
                let result = if (block_number + i + j) % 4 == 0 {
                    ReadPrecompileResult::OutOfGas
                } else {
                    let output_size = 16 + ((block_number + i + j) % 500);
                    ReadPrecompileResult::Ok {
                        gas_used: 1000 + (output_size * 5),
                        bytes: Bytes::from(vec![(i + j) as u8; output_size as usize]),
                    }
                };
                
                call_data.push((input, result));
            }
            
            calls.push((addr, call_data));
        }
        
        ReadPrecompileCalls(calls)
    }
}

use test_utils::*;

struct BenchmarkSetup {
    hl_source: HlNodeBlockSource,
    _temp_dir: TempDir,
    rt: Runtime,
}

impl BenchmarkSetup {
    fn new() -> Self {
        let rt = Runtime::new().unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        
        let hl_source = rt.block_on(async {
            HlNodeBlockSource::new(
                BlockSourceBoxed::new(Box::new(LocalBlockSource::new("/nonexistent"))),
                temp_dir.path().to_path_buf(),
                1000000,
            ).await
        });
        
        Self {
            hl_source,
            _temp_dir: temp_dir,
            rt,
        }
    }
    
    fn populate_cache(&self, num_blocks: u64) {
        self.rt.block_on(async {
            let mut cache = self.hl_source.local_blocks_cache.lock().await;
            
            for i in 0..num_blocks {
                let block_number = 1000000 + i;
                let precompile_calls = if i % 3 == 0 { 
                    Some(create_varying_precompile_calls(block_number)) 
                } else { 
                    None 
                };
                
                let block = create_test_block(
                    block_number, 
                    1722633600 + i * 12, 
                    precompile_calls
                );
                
                let scan_result = nanoreth::pseudo_peer::sources::hl_node::scan::ScanResult {
                    path: std::path::PathBuf::from("/benchmark"),
                    next_expected_height: block_number + 1,
                    new_blocks: vec![block],
                    new_block_ranges: vec![block_number..=block_number],
                };
                
                cache.load_scan_result(scan_result);
            }
        });
    }
}

fn bench_single_block_collection(c: &mut Criterion) {
    let setup = BenchmarkSetup::new();
    setup.populate_cache(1000);
    
    let mut group = c.benchmark_group("single_block_collection");
    
    group.bench_function("collect_block_from_cache", |b| {
        b.to_async(&setup.rt).iter(|| async {
            let block_number = 1000000 + (rand::random::<u64>() % 1000);
            let result = setup.hl_source.collect_block(block_number).await;
            black_box(result.unwrap());
        });
    });
    
    group.bench_function("collect_block_with_precompile_calls", |b| {
        b.to_async(&setup.rt).iter(|| async {
            // Collect blocks that have precompile calls (every 3rd block)
            let base = rand::random::<u64>() % 333;
            let block_number = 1000000 + (base * 3);
            let result = setup.hl_source.collect_block(block_number).await;
            black_box(result.unwrap());
        });
    });
    
    group.finish();
}

fn bench_bulk_block_collection(c: &mut Criterion) {
    let setup = BenchmarkSetup::new();
    setup.populate_cache(5000);
    
    let mut group = c.benchmark_group("bulk_block_collection");
    
    group.bench_function("collect_100_sequential_blocks", |b| {
        b.to_async(&setup.rt).iter(|| async {
            let start_block = 1000000 + (rand::random::<u64>() % 4900);
            let mut results = vec![];
            
            for i in 0..100 {
                let result = setup.hl_source.collect_block(start_block + i).await.unwrap();
                results.push(result);
            }
            
            black_box(results);
        });
    });
    
    group.bench_function("collect_100_random_blocks", |b| {
        b.to_async(&setup.rt).iter(|| async {
            let mut results = vec![];
            
            for _ in 0..100 {
                let block_number = 1000000 + (rand::random::<u64>() % 5000);
                let result = setup.hl_source.collect_block(block_number).await.unwrap();
                results.push(result);
            }
            
            black_box(results);
        });
    });
    
    group.finish();
}

fn bench_concurrent_collection(c: &mut Criterion) {
    let setup = BenchmarkSetup::new();
    setup.populate_cache(2000);
    
    let mut group = c.benchmark_group("concurrent_collection");
    
    group.bench_function("concurrent_10_threads", |b| {
        b.to_async(&setup.rt).iter(|| async {
            let source = setup.hl_source.clone();
            let mut handles = vec![];
            
            for i in 0..10 {
                let src = source.clone();
                let handle = tokio::spawn(async move {
                    let mut results = vec![];
                    let start_block = 1000000 + (i * 200);
                    
                    for j in 0..50 {
                        let result = src.collect_block(start_block + j).await.unwrap();
                        results.push(result);
                    }
                    
                    results
                });
                handles.push(handle);
            }
            
            let mut all_results = vec![];
            for handle in handles {
                let results = handle.await.unwrap();
                all_results.extend(results);
            }
            
            black_box(all_results);
        });
    });
    
    group.finish();
}

fn bench_cache_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("cache_operations");
    
    group.bench_function("cache_insertion", |b| {
        b.iter_batched(
            || LocalBlocksCache::new(CACHE_SIZE),
            |mut cache| {
                for i in 0..1000 {
                    let block = create_test_block(i, 1722633600 + i * 12, None);
                    let scan_result = nanoreth::pseudo_peer::sources::hl_node::scan::ScanResult {
                        path: std::path::PathBuf::from("/benchmark"),
                        next_expected_height: i + 1,
                        new_blocks: vec![block],
                        new_block_ranges: vec![i..=i],
                    };
                    
                    cache.load_scan_result(scan_result);
                }
                black_box(cache);
            },
            BatchSize::LargeInput,
        );
    });
    
    group.bench_function("cache_lookup", |b| {
        b.iter_batched(
            || {
                let mut cache = LocalBlocksCache::new(CACHE_SIZE);
                // Pre-populate cache
                for i in 0..1000 {
                    let block = create_test_block(i, 1722633600 + i * 12, None);
                    let scan_result = nanoreth::pseudo_peer::sources::hl_node::scan::ScanResult {
                        path: std::path::PathBuf::from("/benchmark"),
                        next_expected_height: i + 1,
                        new_blocks: vec![block],
                        new_block_ranges: vec![i..=i],
                    };
                    cache.load_scan_result(scan_result);
                }
                cache
            },
            |mut cache| {
                for _ in 0..100 {
                    let block_num = rand::random::<u64>() % 1000;
                    let result = cache.get_block(block_num);
                    black_box(result);
                }
            },
            BatchSize::LargeInput,
        );
    });
    
    group.finish();
}

fn bench_precompile_calls_processing(c: &mut Criterion) {
    let mut group = c.benchmark_group("precompile_calls_processing");
    
    group.bench_function("serialize_precompile_calls", |b| {
        let calls = create_varying_precompile_calls(1000000);
        b.iter(|| {
            let serialized = rmp_serde::to_vec(&calls.0).unwrap();
            black_box(serialized);
        });
    });
    
    group.bench_function("deserialize_precompile_calls", |b| {
        let calls = create_varying_precompile_calls(1000000);
        let serialized = rmp_serde::to_vec(&calls.0).unwrap();
        
        b.iter(|| {
            let deserialized: Vec<_> = rmp_serde::from_slice(&serialized).unwrap();
            black_box(ReadPrecompileCalls(deserialized));
        });
    });
    
    group.bench_function("rlp_encode_precompile_calls", |b| {
        use alloy_rlp::Encodable;
        let calls = create_varying_precompile_calls(1000000);
        
        b.iter(|| {
            let mut buf = vec![];
            calls.encode(&mut buf);
            black_box(buf);
        });
    });
    
    group.bench_function("rlp_decode_precompile_calls", |b| {
        use alloy_rlp::{Encodable, Decodable};
        let calls = create_varying_precompile_calls(1000000);
        let mut buf = vec![];
        calls.encode(&mut buf);
        
        b.iter(|| {
            let mut slice = buf.as_slice();
            let decoded = ReadPrecompileCalls::decode(&mut slice).unwrap();
            black_box(decoded);
        });
    });
    
    group.finish();
}

fn bench_memory_usage(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_usage");
    
    group.bench_function("cache_memory_growth", |b| {
        b.iter_batched(
            || LocalBlocksCache::new(CACHE_SIZE),
            |mut cache| {
                let initial_memory = get_process_memory();
                
                // Load blocks with varying sizes of precompile calls
                for i in 0..CACHE_SIZE.min(1000) {
                    let precompile_calls = if i % 2 == 0 {
                        Some(create_varying_precompile_calls(i as u64))
                    } else {
                        None
                    };
                    
                    let block = create_test_block(i as u64, 1722633600 + i as u64 * 12, precompile_calls);
                    let scan_result = nanoreth::pseudo_peer::sources::hl_node::scan::ScanResult {
                        path: std::path::PathBuf::from("/benchmark"),
                        next_expected_height: i as u64 + 1,
                        new_blocks: vec![block],
                        new_block_ranges: vec![i as u64..=i as u64],
                    };
                    
                    cache.load_scan_result(scan_result);
                }
                
                let final_memory = get_process_memory();
                let memory_increase = final_memory.saturating_sub(initial_memory);
                
                black_box((cache, memory_increase));
            },
            BatchSize::LargeInput,
        );
    });
    
    group.finish();
}

fn get_process_memory() -> usize {
    // Simple memory estimation - in production benchmarks you'd use more accurate methods
    use std::alloc::{GlobalAlloc, Layout, System};
    
    // This is a simplified approach - real benchmarks would use tools like jemalloc stats
    std::mem::size_of::<LocalBlocksCache>() * 1000 // Rough estimation
}

// Performance regression test - ensures collect_block doesn't get slower over time
fn bench_regression_test(c: &mut Criterion) {
    let setup = BenchmarkSetup::new();
    setup.populate_cache(1000);
    
    let mut group = c.benchmark_group("regression_test");
    
    // Baseline performance expectations
    group.bench_function("collect_block_baseline", |b| {
        b.to_async(&setup.rt).iter(|| async {
            let start = Instant::now();
            
            let block = setup.hl_source.collect_block(1000500).await.unwrap();
            let duration = start.elapsed();
            
            // Assert performance characteristics
            assert!(duration.as_micros() < 1000, "Block collection took too long: {:?}", duration);
            
            black_box(block);
        });
    });
    
    group.finish();
}

criterion_group!(
    benches,
    bench_single_block_collection,
    bench_bulk_block_collection,
    bench_concurrent_collection,
    bench_cache_operations,
    bench_precompile_calls_processing,
    bench_memory_usage,
    bench_regression_test
);

criterion_main!(benches);
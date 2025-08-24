//! Regression tests for HlNodeBlockSource fallback behavior
//! 
//! Ensures that the migration maintains:
//! - Proper fallback to remote sources when local fails
//! - Correct rate limiting behavior
//! - Fallback priority ordering
//! - Error handling consistency

use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::Arc,
    time::Duration as StdDuration,
};

use eyre::Result;
use tempfile::TempDir;
use time::{Duration, OffsetDateTime};
use tokio::{sync::Mutex, time::sleep};

use nanoreth::{
    node::types::BlockAndReceipts,
    pseudo_peer::sources::{
        hl_node::{HlNodeBlockSource, MAX_ALLOWED_THRESHOLD_BEFORE_FALLBACK},
        BlockSource, BlockSourceBoxed,
    },
};

// Reuse test utilities
mod test_utils {
    use super::*;
    use alloy_consensus::{BlockBody, Header};
    use alloy_primitives::{Address, Bloom, Bytes, B256, B64, U256};
    use nanoreth::node::types::{reth_compat, EvmBlock, ReadPrecompileCalls};
    
    pub fn create_test_block(number: u64, timestamp: u64, source: &'static str) -> BlockAndReceipts {
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
                        extra_data: Bytes::from_static(source.as_bytes()),
                        mix_hash: B256::ZERO,
                        nonce: B64::ZERO,
                        base_fee_per_gas: Some(1000000000),
                        withdrawals_root: None,
                        blob_gas_used: None,
                        excess_blob_gas: None,
                        parent_beacon_block_root: None,
                        requests_hash: None,
                    },
                    hash: B256::from([number as u8; 32]),
                },
                body: BlockBody {
                    transactions: vec![],
                    ommers: vec![],
                    withdrawals: None,
                },
            }),
            receipts: vec![],
            system_txs: vec![],
            read_precompile_calls: ReadPrecompileCalls(vec![]),
            highest_precompile_address: None,
        }
    }
}

use test_utils::*;

/// Mock fallback source that tracks call statistics
struct StatefulMockBlockSource {
    blocks: Arc<Mutex<BTreeMap<u64, BlockAndReceipts>>>,
    call_history: Arc<Mutex<Vec<(u64, std::time::Instant)>>>,
    failure_mode: Arc<Mutex<FailureMode>>,
}

#[derive(Debug, Clone)]
enum FailureMode {
    None,
    AlwaysFail,
    FailAfterDelay(StdDuration),
    FailRandom(f64), // Probability of failure
}

impl StatefulMockBlockSource {
    fn new() -> Self {
        Self {
            blocks: Arc::new(Mutex::new(BTreeMap::new())),
            call_history: Arc::new(Mutex::new(vec![])),
            failure_mode: Arc::new(Mutex::new(FailureMode::None)),
        }
    }
    
    async fn add_block(&self, height: u64, block: BlockAndReceipts) {
        self.blocks.lock().await.insert(height, block);
    }
    
    async fn set_failure_mode(&self, mode: FailureMode) {
        *self.failure_mode.lock().await = mode;
    }
    
    async fn get_call_history(&self) -> Vec<(u64, std::time::Instant)> {
        self.call_history.lock().await.clone()
    }
    
    async fn get_call_count(&self) -> usize {
        self.call_history.lock().await.len()
    }
}

impl BlockSource for StatefulMockBlockSource {
    fn collect_block(&self, height: u64) -> futures::future::BoxFuture<'static, Result<BlockAndReceipts>> {
        let blocks = self.blocks.clone();
        let call_history = self.call_history.clone();
        let failure_mode = self.failure_mode.clone();
        
        Box::pin(async move {
            call_history.lock().await.push((height, std::time::Instant::now()));
            
            let failure_mode = failure_mode.lock().await.clone();
            match failure_mode {
                FailureMode::AlwaysFail => {
                    return Err(eyre::eyre!("Mock configured to always fail"));
                }
                FailureMode::FailAfterDelay(delay) => {
                    tokio::time::sleep(delay).await;
                    return Err(eyre::eyre!("Mock failed after delay"));
                }
                FailureMode::FailRandom(prob) => {
                    if rand::random::<f64>() < prob {
                        return Err(eyre::eyre!("Mock random failure"));
                    }
                }
                FailureMode::None => {}
            }
            
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

async fn setup_fallback_test() -> Result<(TempDir, HlNodeBlockSource, StatefulMockBlockSource)> {
    let temp_dir = tempfile::tempdir()?;
    let mock_fallback = StatefulMockBlockSource::new();
    
    let hl_source = HlNodeBlockSource::new(
        BlockSourceBoxed::new(Box::new(mock_fallback.clone())),
        temp_dir.path().to_path_buf(),
        1000000,
    ).await;
    
    Ok((temp_dir, hl_source, mock_fallback))
}

#[tokio::test]
async fn test_immediate_fallback_after_threshold() -> Result<()> {
    let (_temp_dir, hl_source, mock_fallback) = setup_fallback_test().await?;
    
    // Add block to fallback source
    let fallback_block = create_test_block(1000001, 1722633700, "fallback");
    mock_fallback.add_block(1000001, fallback_block.clone()).await;
    
    // First attempt should fail due to rate limiting
    let result = hl_source.collect_block(1000001).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("limiting polling rate"));
    
    // Wait for threshold to pass
    sleep(MAX_ALLOWED_THRESHOLD_BEFORE_FALLBACK.unsigned_abs()).await;
    
    // Second attempt should succeed via fallback
    let collected_block = hl_source.collect_block(1000001).await?;
    assert_eq!(collected_block, fallback_block);
    
    // Verify fallback was called
    assert_eq!(mock_fallback.get_call_count().await, 1);
    
    Ok(())
}

#[tokio::test]
async fn test_fallback_priority_ordering() -> Result<()> {
    let (_temp_dir, hl_source, primary_fallback) = setup_fallback_test().await?;
    
    // Add blocks with different timestamps to test ordering
    let block1 = create_test_block(1000010, 1722633000, "primary");
    let block2 = create_test_block(1000011, 1722633100, "primary");
    
    primary_fallback.add_block(1000010, block1.clone()).await;
    primary_fallback.add_block(1000011, block2.clone()).await;
    
    // Wait for threshold
    sleep(MAX_ALLOWED_THRESHOLD_BEFORE_FALLBACK.unsigned_abs()).await;
    
    // Collect multiple blocks and verify order
    let collected1 = hl_source.collect_block(1000010).await?;
    let collected2 = hl_source.collect_block(1000011).await?;
    
    assert_eq!(collected1, block1);
    assert_eq!(collected2, block2);
    
    // Verify calls were made in order
    let call_history = primary_fallback.get_call_history().await;
    assert_eq!(call_history.len(), 2);
    assert_eq!(call_history[0].0, 1000010);
    assert_eq!(call_history[1].0, 1000011);
    
    Ok(())
}

#[tokio::test]
async fn test_fallback_failure_handling() -> Result<()> {
    let (_temp_dir, hl_source, mock_fallback) = setup_fallback_test().await?;
    
    // Configure fallback to always fail
    mock_fallback.set_failure_mode(FailureMode::AlwaysFail).await;
    
    // Wait for threshold
    sleep(MAX_ALLOWED_THRESHOLD_BEFORE_FALLBACK.unsigned_abs()).await;
    
    // Collect block should fail with fallback error
    let result = hl_source.collect_block(1000020).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Mock configured to always fail"));
    
    // Verify fallback was attempted
    assert_eq!(mock_fallback.get_call_count().await, 1);
    
    Ok(())
}

#[tokio::test]
async fn test_rate_limiting_behavior() -> Result<()> {
    let (_temp_dir, hl_source, _mock_fallback) = setup_fallback_test().await?;
    
    // Make rapid requests for non-existent blocks
    let mut results = vec![];
    for i in 0..5 {
        let result = hl_source.collect_block(2000000 + i).await;
        results.push(result.is_err());
    }
    
    // All should fail due to rate limiting
    assert!(results.iter().all(|&failed| failed));
    
    // Wait for threshold to pass
    sleep(MAX_ALLOWED_THRESHOLD_BEFORE_FALLBACK.unsigned_abs()).await;
    
    // Now requests should attempt fallback (and fail since we haven't added blocks)
    let result = hl_source.collect_block(2000000).await;
    assert!(result.is_err());
    // Error should NOT be about rate limiting anymore
    assert!(!result.unwrap_err().to_string().contains("limiting polling rate"));
    
    Ok(())
}

#[tokio::test]
async fn test_concurrent_fallback_requests() -> Result<()> {
    let (_temp_dir, hl_source, mock_fallback) = setup_fallback_test().await?;
    
    // Add blocks to fallback
    for i in 0..10 {
        let block = create_test_block(3000000 + i, 1722633000 + i * 12, "concurrent");
        mock_fallback.add_block(3000000 + i, block).await;
    }
    
    // Wait for threshold
    sleep(MAX_ALLOWED_THRESHOLD_BEFORE_FALLBACK.unsigned_abs()).await;
    
    // Make concurrent requests
    let mut handles = vec![];
    for i in 0..10 {
        let source = hl_source.clone();
        let handle = tokio::spawn(async move {
            source.collect_block(3000000 + i).await
        });
        handles.push(handle);
    }
    
    // All should succeed
    for (i, handle) in handles.into_iter().enumerate() {
        let result = handle.await??;
        assert_eq!(result.number(), 3000000 + i);
    }
    
    // All fallback calls should have been made
    assert_eq!(mock_fallback.get_call_count().await, 10);
    
    Ok(())
}

#[tokio::test]
async fn test_fallback_timeout_behavior() -> Result<()> {
    let (_temp_dir, hl_source, mock_fallback) = setup_fallback_test().await?;
    
    // Configure fallback with delay longer than reasonable timeout
    mock_fallback.set_failure_mode(FailureMode::FailAfterDelay(StdDuration::from_secs(10))).await;
    
    // Wait for threshold
    sleep(MAX_ALLOWED_THRESHOLD_BEFORE_FALLBACK.unsigned_abs()).await;
    
    let start = std::time::Instant::now();
    let result = hl_source.collect_block(4000000).await;
    let duration = start.elapsed();
    
    // Should fail within reasonable time (not wait full 10 seconds)
    assert!(result.is_err());
    assert!(duration < StdDuration::from_secs(8), "Fallback took too long: {:?}", duration);
    
    Ok(())
}

#[tokio::test]
async fn test_local_cache_preference_over_fallback() -> Result<()> {
    let (_temp_dir, hl_source, mock_fallback) = setup_fallback_test().await?;
    
    // Add different blocks to cache and fallback
    let cache_block = create_test_block(5000000, 1722633000, "cache");
    let fallback_block = create_test_block(5000000, 1722633000, "fallback");
    
    // Add to fallback
    mock_fallback.add_block(5000000, fallback_block).await;
    
    // Add to cache
    let scan_result = nanoreth::pseudo_peer::sources::hl_node::scan::ScanResult {
        path: PathBuf::from("/test"),
        next_expected_height: 5000001,
        new_blocks: vec![cache_block.clone()],
        new_block_ranges: vec![5000000..=5000000],
    };
    hl_source.local_blocks_cache.lock().await.load_scan_result(scan_result);
    
    // Collect block should prefer cache
    let collected = hl_source.collect_block(5000000).await?;
    assert_eq!(collected, cache_block);
    
    // Fallback should not have been called
    assert_eq!(mock_fallback.get_call_count().await, 0);
    
    Ok(())
}

#[tokio::test]
async fn test_fallback_with_partial_failures() -> Result<()> {
    let (_temp_dir, hl_source, mock_fallback) = setup_fallback_test().await?;
    
    // Configure 30% failure rate
    mock_fallback.set_failure_mode(FailureMode::FailRandom(0.3)).await;
    
    // Add blocks
    for i in 0..100 {
        let block = create_test_block(6000000 + i, 1722633000 + i * 12, "partial");
        mock_fallback.add_block(6000000 + i, block).await;
    }
    
    // Wait for threshold
    sleep(MAX_ALLOWED_THRESHOLD_BEFORE_FALLBACK.unsigned_abs()).await;
    
    let mut success_count = 0;
    let mut failure_count = 0;
    
    // Make many requests
    for i in 0..100 {
        let result = hl_source.collect_block(6000000 + i).await;
        if result.is_ok() {
            success_count += 1;
        } else {
            failure_count += 1;
        }
    }
    
    // Should have mix of successes and failures
    assert!(success_count > 50, "Too few successes: {}", success_count);
    assert!(failure_count > 10, "Too few failures: {}", failure_count);
    
    // Total calls should equal success + failure attempts
    let total_calls = mock_fallback.get_call_count().await;
    assert_eq!(total_calls, 100);
    
    Ok(())
}

#[tokio::test]
async fn test_fallback_state_consistency() -> Result<()> {
    let (_temp_dir, hl_source, mock_fallback) = setup_fallback_test().await?;
    
    // Test that internal state remains consistent across fallback scenarios
    
    // Add block to fallback
    let block = create_test_block(7000000, 1722633000, "consistency");
    mock_fallback.add_block(7000000, block.clone()).await;
    
    // First attempt (should fail due to rate limiting)
    let result1 = hl_source.collect_block(7000000).await;
    assert!(result1.is_err());
    
    // Check that last_local_fetch was updated correctly
    let last_fetch = hl_source.last_local_fetch.lock().await.clone();
    assert!(last_fetch.is_some());
    let (last_height, _) = last_fetch.unwrap();
    assert_eq!(last_height, 7000000);
    
    // Wait for threshold
    sleep(MAX_ALLOWED_THRESHOLD_BEFORE_FALLBACK.unsigned_abs()).await;
    
    // Second attempt (should succeed via fallback)
    let result2 = hl_source.collect_block(7000000).await?;
    assert_eq!(result2, block);
    
    // Check state was updated again
    let last_fetch2 = hl_source.last_local_fetch.lock().await.clone();
    assert!(last_fetch2.is_some());
    let (last_height2, last_time2) = last_fetch2.unwrap();
    assert_eq!(last_height2, 7000000);
    
    // Time should have been updated
    let (_, first_time) = last_fetch.unwrap();
    assert!(last_time2 > first_time);
    
    Ok(())
}
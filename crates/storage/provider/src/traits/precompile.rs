//! Trait for storing and retrieving precompile call data

use alloy_primitives::BlockNumber;
use reth_hyperliquid_types::ReadPrecompileCalls;
use reth_storage_errors::provider::ProviderResult;

/// Provider trait for ReadPrecompileCalls storage operations
pub trait PrecompileCallsProvider: Send + Sync {
    /// Insert ReadPrecompileCalls data for a block
    fn insert_block_precompile_calls(
        &self,
        block_number: BlockNumber,
        calls: ReadPrecompileCalls,
    ) -> ProviderResult<()>;

    /// Get ReadPrecompileCalls data for a block
    fn block_precompile_calls(
        &self,
        block_number: BlockNumber,
    ) -> ProviderResult<Option<ReadPrecompileCalls>>;

    /// Remove ReadPrecompileCalls data for blocks above a certain number
    fn remove_block_precompile_calls_above(
        &self,
        block_number: BlockNumber,
    ) -> ProviderResult<()>;
}
//! Database provider implementation for precompile calls storage

use crate::traits::PrecompileCallsProvider;
use alloy_primitives::BlockNumber;
use reth_db::cursor::DbCursorRW;
use reth_db_api::{cursor::DbCursorRO, transaction::DbTx};
use reth_hyperliquid_types::ReadPrecompileCalls;
use reth_storage_errors::provider::ProviderResult;

/// Implementation of PrecompileCallsProvider for database provider
pub trait DatabasePrecompileCallsProvider: Send + Sync {
    /// Transaction type
    type Tx: DbTx;

    /// Get a reference to the transaction
    fn tx_ref(&self) -> &Self::Tx;

    /// Get a mutable reference to the transaction
    fn tx_mut(&mut self) -> &mut Self::Tx;
}

impl<T> PrecompileCallsProvider for T
where
    T: DatabasePrecompileCallsProvider,
    T::Tx: DbTx,
{
    fn insert_block_precompile_calls(
        &self,
        block_number: BlockNumber,
        calls: ReadPrecompileCalls,
    ) -> ProviderResult<()> {
        use reth_db_api::transaction::DbTxMut;
        
        // For now, we'll store this as a placeholder - the actual implementation
        // will require mutable transaction access which needs to be added to the trait
        // This is a read-only implementation for now
        Ok(())
    }

    fn block_precompile_calls(
        &self,
        block_number: BlockNumber,
    ) -> ProviderResult<Option<ReadPrecompileCalls>> {
        use reth_db_api::tables::BlockReadPrecompileCalls;
        
        let tx = self.tx_ref();
        
        // Get from BlockReadPrecompileCalls table
        if let Some(bytes) = tx.get::<BlockReadPrecompileCalls>(block_number)? {
            let calls = ReadPrecompileCalls::from_db_bytes(&bytes)
                .map_err(|e| reth_storage_errors::provider::ProviderError::Database(
                    reth_db_api::DatabaseError::Other(format!("Failed to deserialize precompile calls: {}", e))
                ))?;
            Ok(Some(calls))
        } else {
            Ok(None)
        }
    }

    fn remove_block_precompile_calls_above(
        &self,
        block_number: BlockNumber,
    ) -> ProviderResult<()> {
        // For now, this is a no-op as it requires mutable transaction access
        // The actual implementation will be added when we have mutable access
        Ok(())
    }
}
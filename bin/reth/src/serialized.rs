use alloy_primitives::{Address, Log};
use reth_hyperliquid_types::{ReadPrecompileInput, ReadPrecompileResult, ReadPrecompileCalls};
use reth_primitives::{SealedBlock, Transaction};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct BlockAndReceipts {
    pub block: EvmBlock,
    pub receipts: Vec<LegacyReceipt>,
    #[serde(default)]
    pub system_txs: Vec<SystemTx>,
    #[serde(default)]
    pub read_precompile_calls: ReadPrecompileCalls,
    pub highest_precompile_address: Option<Address>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum EvmBlock {
    Reth115(SealedBlock),
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct LegacyReceipt {
    tx_type: LegacyTxType,
    success: bool,
    cumulative_gas_used: u64,
    logs: Vec<Log>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum LegacyTxType {
    Legacy = 0,
    Eip2930 = 1,
    Eip1559 = 2,
    Eip4844 = 3,
    Eip7702 = 4,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SystemTx {
    pub tx: Transaction,
    pub receipt: Option<LegacyReceipt>,
}

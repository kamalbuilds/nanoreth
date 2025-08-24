use std::{collections::BTreeMap, sync::Arc};

use alloy_primitives::{Address, Bytes};
use alloy_rlp::{Decodable, Encodable};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash)]
pub struct ReadPrecompileInput {
    pub input: Bytes,
    pub gas_limit: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ReadPrecompileResult {
    Ok { gas_used: u64, bytes: Bytes },
    OutOfGas,
    Error,
    UnexpectedError,
}

/// ReadPrecompileCalls represents a collection of precompile calls with their results
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ReadPrecompileCalls(pub Vec<(Address, Vec<(ReadPrecompileInput, ReadPrecompileResult)>)>);

impl ReadPrecompileCalls {
    /// Create an empty ReadPrecompileCalls
    pub fn new() -> Self {
        Self(Vec::new())
    }

    /// Create from a vector of precompile calls
    pub fn from_vec(calls: Vec<(Address, Vec<(ReadPrecompileInput, ReadPrecompileResult)>)>) -> Self {
        Self(calls)
    }

    /// Get the inner vector
    pub fn into_inner(self) -> Vec<(Address, Vec<(ReadPrecompileInput, ReadPrecompileResult)>)> {
        self.0
    }
    
    /// Serialize to bytes using MessagePack for database storage
    pub fn to_db_bytes(&self) -> Result<Vec<u8>, rmp_serde::encode::Error> {
        rmp_serde::to_vec(&self.0)
    }
    
    /// Deserialize from bytes using MessagePack from database storage
    pub fn from_db_bytes(bytes: &[u8]) -> Result<Self, rmp_serde::decode::Error> {
        let data = rmp_serde::from_slice(bytes)?;
        Ok(Self(data))
    }
}

impl Encodable for ReadPrecompileCalls {
    fn encode(&self, out: &mut dyn alloy_rlp::BufMut) {
        // Encode as MessagePack bytes wrapped in RLP
        let buf = self.to_db_bytes().unwrap_or_default();
        buf.encode(out);
    }

    fn length(&self) -> usize {
        let buf = self.to_db_bytes().unwrap_or_default();
        buf.length()
    }
}

impl Decodable for ReadPrecompileCalls {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let bytes = Vec::<u8>::decode(buf)?;
        Self::from_db_bytes(&bytes)
            .map_err(|_| alloy_rlp::Error::Custom("Failed to decode ReadPrecompileCalls"))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrecompileData {
    pub precompiles: Vec<(Address, Vec<(ReadPrecompileInput, ReadPrecompileResult)>)>,
    pub highest_precompile_address: Option<Address>,
}

pub type PrecompilesCache = Arc<Mutex<BTreeMap<u64, PrecompileData>>>;

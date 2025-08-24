use std::{collections::BTreeMap, sync::Arc};

use alloy_primitives::{Address, Bytes};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash)]
pub struct ReadPrecompileInput {
    pub input: Bytes,
    pub gas_limit: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrecompileData {
    pub precompiles: Vec<(Address, Vec<(ReadPrecompileInput, ReadPrecompileResult)>)>,
    pub highest_precompile_address: Option<Address>,
}

pub type PrecompilesCache = Arc<Mutex<BTreeMap<u64, PrecompileData>>>;

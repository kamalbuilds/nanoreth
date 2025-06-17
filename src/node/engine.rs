use std::sync::Arc;

use crate::{
    node::{rpc::engine_api::payload::HlPayloadTypes, HlNode},
    HlBlock, HlPrimitives,
};
use alloy_eips::eip7685::Requests;
use alloy_primitives::U256;
use reth::{
    api::FullNodeTypes,
    builder::{components::PayloadServiceBuilder, BuilderContext},
    payload::{PayloadBuilderHandle, PayloadServiceCommand},
    transaction_pool::TransactionPool,
};
use reth_evm::ConfigureEvm;
use reth_payload_primitives::BuiltPayload;
use reth_primitives::SealedBlock;
use tokio::sync::{broadcast, mpsc};
use tracing::warn;

/// Built payload for Hl. This is similar to [`EthBuiltPayload`] but without sidecars as those
/// included into [`HlBlock`].
#[derive(Debug, Clone)]
pub struct HlBuiltPayload {
    /// The built block
    pub(crate) block: Arc<SealedBlock<HlBlock>>,
    /// The fees of the block
    pub(crate) fees: U256,
    /// The requests of the payload
    pub(crate) requests: Option<Requests>,
}

impl BuiltPayload for HlBuiltPayload {
    type Primitives = HlPrimitives;

    fn block(&self) -> &SealedBlock<HlBlock> {
        self.block.as_ref()
    }

    fn fees(&self) -> U256 {
        self.fees
    }

    fn requests(&self) -> Option<Requests> {
        self.requests.clone()
    }
}

#[derive(Debug, Clone, Copy, Default)]
#[non_exhaustive]
pub struct HlPayloadServiceBuilder;

impl<Node, Pool, Evm> PayloadServiceBuilder<Node, Pool, Evm> for HlPayloadServiceBuilder
where
    Node: FullNodeTypes<Types = HlNode>,
    Pool: TransactionPool,
    Evm: ConfigureEvm,
{
    async fn spawn_payload_builder_service(
        self,
        ctx: &BuilderContext<Node>,
        _pool: Pool,
        _evm_config: Evm,
    ) -> eyre::Result<PayloadBuilderHandle<HlPayloadTypes>> {
        let (tx, mut rx) = mpsc::unbounded_channel();

        ctx.task_executor().spawn_critical("payload builder", async move {
            let mut subscriptions = Vec::new();

            while let Some(message) = rx.recv().await {
                match message {
                    PayloadServiceCommand::Subscribe(tx) => {
                        let (events_tx, events_rx) = broadcast::channel(100);
                        // Retain senders to make sure that channels are not getting closed
                        subscriptions.push(events_tx);
                        let _ = tx.send(events_rx);
                    }
                    message => warn!(?message, "Noop payload service received a message"),
                }
            }
        });

        Ok(PayloadBuilderHandle::new(tx))
    }
}

// impl From<EthBuiltPayload> for HlBuiltPayload {
//     fn from(value: EthBuiltPayload) -> Self {
//         let EthBuiltPayload { id, block, fees, sidecars, requests } = value;
//         HlBuiltPayload {
//             id,
//             block: block.into(),
//             fees,
//             requests,
//         }
//     }
// }

// pub struct HlPayloadBuilder<Inner> {
//     inner: Inner,
// }

// impl<Inner> PayloadBuilder for HlPayloadBuilder<Inner>
// where
//     Inner: PayloadBuilder<BuiltPayload = EthBuiltPayload>,
// {
//     type Attributes = Inner::Attributes;
//     type BuiltPayload = HlBuiltPayload;
//     type Error = Inner::Error;

//     fn try_build(
//         &self,
//         args: BuildArguments<Self::Attributes, Self::BuiltPayload>,
//     ) -> Result<BuildOutcome<Self::BuiltPayload>, PayloadBuilderError> {
//         let outcome = self.inner.try_build(args)?;
//     }
// }

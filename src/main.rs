use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use matrix_sdk::ruma::RoomId;
use tokio::task::JoinSet;
use tracing_subscriber::EnvFilter;

use irc_bridge::adapters::clock::SystemClock;
use irc_bridge::adapters::irc::IrcMessageSender;
use irc_bridge::adapters::matrix::MatrixMessageSender;
use irc_bridge::adapters::sqlite::SqliteMessageRepository;
use irc_bridge::adapters::waker::NotifyWaker;
use irc_bridge::application::commands::deliver_pending::DeliverPendingHandler;
use irc_bridge::application::commands::store_received_message::StoreReceivedMessageHandler;
use irc_bridge::application::ports::{Clock, DeliveryWaker, MessageRepository, MessageSender};
use irc_bridge::config::{Config, irc_network, matrix_network};
use irc_bridge::domain::delivery::RetryPolicy;
use irc_bridge::domain::endpoint::NetworkId;
use irc_bridge::entrypoints::delivery_worker;
use irc_bridge::entrypoints::irc::IrcEntrypoint;
use irc_bridge::entrypoints::matrix::MatrixEntrypoint;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| {
            EnvFilter::new("info,matrix_sdk=warn,matrix_sdk_crypto=warn,matrix_sdk_base=warn")
        }))
        .init();

    let config_path = std::env::args()
        .nth(1)
        .map_or_else(|| PathBuf::from("config.toml"), PathBuf::from);
    let config = Config::load(&config_path)?;
    let topology = Arc::new(config.topology()?);

    let repository: Arc<dyn MessageRepository> =
        Arc::new(SqliteMessageRepository::open(&config.database)?);
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);

    let matrix_net = matrix_network();
    let mut networks = vec![matrix_net.clone()];
    networks.extend(config.irc.keys().map(|name| irc_network(name)));
    let notify_waker = Arc::new(NotifyWaker::new(&networks));
    let waker: Arc<dyn DeliveryWaker> = notify_waker.clone();

    let store_received_message = Arc::new(StoreReceivedMessageHandler::new(
        topology.clone(),
        repository.clone(),
        waker.clone(),
        clock.clone(),
    ));

    let matrix = irc_bridge::adapters::matrix::connect(&config.matrix).await?;
    let mut senders: HashMap<NetworkId, Arc<dyn MessageSender>> = HashMap::new();
    senders.insert(
        matrix_net.clone(),
        Arc::new(MatrixMessageSender::new(matrix.client.clone())),
    );

    let mut tasks = JoinSet::new();
    for (name, settings) in &config.irc {
        let network = irc_network(name);
        let sender = Arc::new(IrcMessageSender::default());
        senders.insert(network.clone(), sender.clone());
        tasks.spawn(
            IrcEntrypoint {
                channels: topology.channels_on(&network),
                network,
                settings: settings.clone(),
                store_received_message: store_received_message.clone(),
                sender,
                waker: waker.clone(),
            }
            .run(),
        );
    }

    let deliver = Arc::new(DeliverPendingHandler::new(
        repository,
        senders,
        clock.clone(),
        RetryPolicy::default(),
    ));
    for network in &networks {
        let wakeup = notify_waker
            .notifier(network)
            .with_context(|| format!("no notifier for {network}"))?;
        tasks.spawn(delivery_worker::run(
            network.clone(),
            deliver.clone(),
            wakeup,
            clock.clone(),
        ));
    }

    let rooms = topology
        .channels_on(&matrix_net)
        .iter()
        .map(|room| RoomId::parse(room))
        .collect::<Result<Vec<_>, _>>()?;
    tasks.spawn(
        MatrixEntrypoint {
            client: matrix.client,
            network: matrix_net,
            rooms,
            skip_backlog: matrix.fresh_login,
            store_received_message,
            waker,
        }
        .run(),
    );

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("shutting down");
            Ok(())
        }
        Some(result) = tasks.join_next() => {
            result?;
            anyhow::bail!("a task exited unexpectedly")
        }
    }
}

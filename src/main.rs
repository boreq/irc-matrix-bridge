use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use matrix_sdk::ruma::RoomId;
use tokio::task::JoinSet;
use tracing_subscriber::EnvFilter;

use irc_bridge::adapters::clock::SystemClock;
use irc_bridge::adapters::config::TomlConfigLoader;
use irc_bridge::adapters::connection_health::InMemoryConnectionHealth;
use irc_bridge::adapters::irc::IrcMessageSender;
use irc_bridge::adapters::matrix::MatrixMessageSender;
use irc_bridge::adapters::sqlite::SqliteMessageRepository;
use irc_bridge::adapters::waker::NotifyWaker;
use irc_bridge::application::commands::relay_stored_message::RelayStoredMessageHandler;
use irc_bridge::application::commands::store_received_message::StoreReceivedMessageHandler;
use irc_bridge::application::ports::{Clock, ConnectionHealth, DeliveryWaker, MessageSender};
use irc_bridge::application::queries::bridge_metrics::BridgeMetricsHandler;
use irc_bridge::application::queries::pending_destinations::PendingDestinationsHandler;
use irc_bridge::domain::{Network, RetryPolicy};
use irc_bridge::entrypoints::delivery_worker;
use irc_bridge::entrypoints::irc::IrcEntrypoint;
use irc_bridge::entrypoints::matrix::MatrixEntrypoint;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config_path = std::env::args()
        .nth(1)
        .map_or_else(|| PathBuf::from("config.toml"), PathBuf::from);
    let config = TomlConfigLoader::new(config_path).load()?;

    let mut filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new("info,matrix_sdk=warn,matrix_sdk_crypto=warn,matrix_sdk_base=warn")
    });
    if config.debug() {
        filter = filter
            .add_directive("irc::client::transport=trace".parse()?)
            .add_directive("irc_bridge::entrypoints::irc=debug".parse()?);
    }
    tracing_subscriber::fmt().with_env_filter(filter).init();
    let bridges = Arc::new(config.bridges().clone());

    let repository = Arc::new(SqliteMessageRepository::open(config.database())?);
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let health: Arc<dyn ConnectionHealth> = Arc::new(InMemoryConnectionHealth::new(clock.clone()));
    let mut tasks = JoinSet::new();
    if let Some(address) = config.metrics_listen() {
        let listener = tokio::net::TcpListener::bind(address)
            .await
            .with_context(|| format!("binding metrics listener {address}"))?;
        tracing::info!(address = %listener.local_addr()?, "metrics endpoint listening at /metrics");
        let handler = Arc::new(BridgeMetricsHandler::new(
            bridges.clone(),
            repository.clone(),
            health.clone(),
            clock.clone(),
        ));
        tasks.spawn(irc_bridge::entrypoints::metrics::run(listener, handler));
    }

    let mut networks = vec![Network::Matrix];
    networks.extend(
        config
            .irc_networks()
            .iter()
            .map(|irc| Network::Irc(irc.name().clone())),
    );
    let notify_waker = Arc::new(NotifyWaker::new(&networks));
    let waker: Arc<dyn DeliveryWaker> = notify_waker.clone();

    let store_received_message = Arc::new(StoreReceivedMessageHandler::new(
        bridges.clone(),
        repository.clone(),
        waker.clone(),
        clock.clone(),
    ));

    let matrix = irc_bridge::adapters::matrix::connect(config.matrix()).await?;
    let mut senders: HashMap<Network, Arc<dyn MessageSender>> = HashMap::new();
    senders.insert(
        Network::Matrix,
        Arc::new(MatrixMessageSender::new(matrix.client.clone())),
    );

    for irc in config.irc_networks() {
        let sender = Arc::new(IrcMessageSender::default());
        senders.insert(Network::Irc(irc.name().clone()), sender.clone());
        tasks.spawn(
            IrcEntrypoint {
                channels: bridges
                    .irc_channels(irc.name())
                    .map(|channel| channel.name().clone())
                    .collect(),
                client_cert: config
                    .database()
                    .with_file_name(format!("irc-{}.pem", irc.name())),
                config: irc.clone(),
                store_received_message: store_received_message.clone(),
                sender,
                waker: waker.clone(),
                health: health.clone(),
            }
            .run(),
        );
    }

    let pending_destinations = Arc::new(PendingDestinationsHandler::new(repository.clone()));
    let relay_stored_message = Arc::new(RelayStoredMessageHandler::new(
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
            pending_destinations.clone(),
            relay_stored_message.clone(),
            wakeup,
            clock.clone(),
        ));
    }

    let rooms = bridges
        .matrix_rooms()
        .map(|room| RoomId::parse(room.id()))
        .collect::<Result<Vec<_>, _>>()?;
    tasks.spawn(
        MatrixEntrypoint {
            client: matrix.client,
            rooms,
            skip_backlog: matrix.fresh_login,
            store_received_message,
            waker,
            health,
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

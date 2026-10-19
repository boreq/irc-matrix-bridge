use std::path::Path;

use anyhow::{Context, bail};
use async_trait::async_trait;
use matrix_sdk::authentication::matrix::MatrixSession;
use matrix_sdk::ruma::api::client::session::login;
use matrix_sdk::ruma::events::room::message::RoomMessageEventContent;
use matrix_sdk::ruma::{OwnedTransactionId, RoomId};
use matrix_sdk::store::RoomLoadSettings;
use matrix_sdk::utils::UrlOrQuery;
use matrix_sdk::{Client, RoomState};

use crate::application::ports::MessageSender;
use crate::config::MatrixConfig;
use crate::domain::channel::Channel;
use crate::domain::delivery::Delivery;

pub struct MatrixConnection {
    pub client: Client,
    pub fresh_login: bool,
}

pub async fn connect(config: &MatrixConfig) -> anyhow::Result<MatrixConnection> {
    std::fs::create_dir_all(config.data_dir())?;
    let session_path = config.data_dir().join("session.json");

    let client = Client::builder()
        .homeserver_url(config.homeserver())
        .sqlite_store(config.data_dir().join("store"), config.store_passphrase())
        .build()
        .await?;

    let fresh_login = if session_path.exists() {
        let session: MatrixSession = serde_json::from_str(&std::fs::read_to_string(&session_path)?)
            .context("reading saved matrix session")?;
        client
            .matrix_auth()
            .restore_session(session, RoomLoadSettings::default())
            .await?;
        tracing::info!(device = ?client.device_id(), "restored matrix session");
        false
    } else {
        let response = login(&client, config).await.context("matrix login")?;
        save_session(&session_path, &MatrixSession::from(&response))?;
        tracing::info!(device = %response.device_id, "logged in to matrix");
        true
    };

    setup_encryption(&client, config).await?;
    Ok(MatrixConnection {
        client,
        fresh_login,
    })
}

/// The page never loads; the user copies the address into `bootstrap_link`.
const SSO_REDIRECT_URL: &str = "http://localhost/";

async fn login(client: &Client, config: &MatrixConfig) -> anyhow::Result<login::v3::Response> {
    let auth = client.matrix_auth();
    if let Some(link) = config.bootstrap_link() {
        let query = link.split_once('?').map_or(link, |(_, query)| query);
        let result = auth
            .login_with_sso_callback(UrlOrQuery::Query(query.to_owned()))?
            .initial_device_display_name(config.device_name())
            .send()
            .await;
        match result {
            Ok(response) => return Ok(response),
            Err(error) => tracing::warn!(
                "bootstrap_link didn't work, it works once and expires 2 minutes after login: {error}"
            ),
        }
    }

    let url = auth.get_sso_login_url(SSO_REDIRECT_URL, None).await?;
    tracing::info!("open this link to log in to matrix: {url}");
    tracing::info!(
        "then copy the address the browser ends up on (the page won't load) into `bootstrap_link` under [matrix] and restart within 2 minutes"
    );
    bail!("not logged in to matrix");
}

fn save_session(path: &Path, session: &MatrixSession) -> anyhow::Result<()> {
    std::fs::write(path, serde_json::to_string(session)?)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

async fn setup_encryption(client: &Client, config: &MatrixConfig) -> anyhow::Result<()> {
    let encryption = client.encryption();

    if let Some(key) = config.recovery_key() {
        encryption
            .recovery()
            .recover(key)
            .await
            .context("recovering secrets with recovery key")?;
        tracing::info!("imported cross-signing secrets from recovery key");
        return Ok(());
    }

    encryption
        .bootstrap_cross_signing_if_needed(None)
        .await
        .context("bootstrapping cross-signing, set recovery_key if the account already has it")?;
    Ok(())
}

pub struct MatrixMessageSender {
    client: Client,
}

impl MatrixMessageSender {
    pub fn new(client: Client) -> Self {
        Self { client }
    }
}

#[async_trait]
impl MessageSender for MatrixMessageSender {
    async fn send(&self, delivery: &Delivery) -> anyhow::Result<()> {
        let Channel::Matrix(room) = delivery.destination() else {
            bail!("{} is not a matrix room", delivery.destination());
        };
        let room_id = RoomId::parse(room.id())?;
        let Some(room) = self.client.get_room(&room_id) else {
            bail!("unknown room {room_id}");
        };
        if room.state() != RoomState::Joined {
            bail!("not joined to {room_id}");
        }

        let txn_id = OwnedTransactionId::from(format!("irc-bridge-{}", delivery.id()));
        room.send(RoomMessageEventContent::text_plain(
            delivery.message().relay_text(),
        ))
        .with_transaction_id(txn_id)
        .await?;
        Ok(())
    }
}

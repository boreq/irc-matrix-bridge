use std::path::{Path, PathBuf};

use anyhow::{Context, bail};
use async_trait::async_trait;
use matrix_sdk::authentication::matrix::MatrixSession;
use matrix_sdk::ruma::api::client::uiaa;
use matrix_sdk::ruma::events::room::message::RoomMessageEventContent;
use matrix_sdk::ruma::{OwnedTransactionId, RoomId};
use matrix_sdk::store::RoomLoadSettings;
use matrix_sdk::{Client, RoomState};
use serde::Deserialize;

use crate::application::ports::MessageSender;
use crate::domain::channel::Channel;
use crate::domain::delivery::Delivery;

#[derive(Clone, Debug, Deserialize)]
pub struct MatrixSettings {
    pub homeserver: String,
    pub user: String,
    pub password: String,
    pub data_dir: PathBuf,
    pub store_passphrase: Option<String>,
    pub recovery_key: Option<String>,
    #[serde(default = "default_device_name")]
    pub device_name: String,
}

fn default_device_name() -> String {
    "irc-bridge".into()
}

pub struct MatrixConnection {
    pub client: Client,
    pub fresh_login: bool,
}

pub async fn connect(settings: &MatrixSettings) -> anyhow::Result<MatrixConnection> {
    std::fs::create_dir_all(&settings.data_dir)?;
    let session_path = settings.data_dir.join("session.json");

    let client = Client::builder()
        .homeserver_url(&settings.homeserver)
        .sqlite_store(
            settings.data_dir.join("store"),
            settings.store_passphrase.as_deref(),
        )
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
        let response = client
            .matrix_auth()
            .login_username(&settings.user, &settings.password)
            .initial_device_display_name(&settings.device_name)
            .send()
            .await
            .context("matrix login")?;
        save_session(&session_path, &MatrixSession::from(&response))?;
        tracing::info!(device = %response.device_id, "logged in to matrix");
        true
    };

    setup_encryption(&client, settings).await?;
    Ok(MatrixConnection {
        client,
        fresh_login,
    })
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

async fn setup_encryption(client: &Client, settings: &MatrixSettings) -> anyhow::Result<()> {
    let encryption = client.encryption();

    if let Some(key) = &settings.recovery_key {
        encryption
            .recovery()
            .recover(key)
            .await
            .context("recovering secrets with recovery key")?;
        tracing::info!("imported cross-signing secrets from recovery key");
        return Ok(());
    }

    if let Err(error) = encryption.bootstrap_cross_signing_if_needed(None).await {
        let Some(response) = error.as_uiaa_response() else {
            return Err(error).context("bootstrapping cross-signing");
        };
        let mut password = uiaa::Password::new(
            uiaa::UserIdentifier::Matrix(uiaa::MatrixUserIdentifier::new(settings.user.clone())),
            settings.password.clone(),
        );
        password.session = response.session.clone();
        encryption
            .bootstrap_cross_signing(Some(uiaa::AuthData::Password(password)))
            .await
            .context("bootstrapping cross-signing")?;
        tracing::info!("created cross-signing identity");
    }
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
        let Channel::Matrix(room) = &delivery.destination else {
            bail!("{} is not a matrix room", delivery.destination);
        };
        let room_id = RoomId::parse(&room.id)?;
        let Some(room) = self.client.get_room(&room_id) else {
            bail!("unknown room {room_id}");
        };
        if room.state() != RoomState::Joined {
            bail!("not joined to {room_id}");
        }

        let txn_id = OwnedTransactionId::from(format!("irc-bridge-{}", delivery.id));
        room.send(RoomMessageEventContent::text_plain(
            delivery.message.relay_text(),
        ))
        .with_transaction_id(txn_id)
        .await?;
        Ok(())
    }
}

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use matrix_sdk::config::SyncSettings;
use matrix_sdk::ruma::OwnedRoomId;
use matrix_sdk::ruma::events::room::member::StrippedRoomMemberEvent;
use matrix_sdk::ruma::events::room::message::{
    MessageType, OriginalSyncRoomMessageEvent, RoomMessageEventContent,
};
use matrix_sdk::{Client, Room, RoomState};

use crate::application::commands::store_received_message::{
    StoreReceivedMessage, StoreReceivedMessageHandler,
};
use crate::application::ports::DeliveryWaker;
use crate::application::queries::who_is_on_the_other_side::{
    WhoIsOnTheOtherSide, WhoIsOnTheOtherSideHandler,
};
use crate::domain::{
    BotCommand, Channel, MatrixEventId, MatrixRoom, Message, MessageBody, MessageKind, Network,
};

pub struct MatrixEntrypoint {
    pub client: Client,
    pub rooms: Vec<OwnedRoomId>,
    pub skip_backlog: bool,
    pub store_received_message: Arc<StoreReceivedMessageHandler>,
    pub who_is_on_the_other_side: Arc<WhoIsOnTheOtherSideHandler>,
    pub waker: Arc<dyn DeliveryWaker>,
}

impl MatrixEntrypoint {
    pub async fn run(self) {
        if self.skip_backlog
            && let Err(error) = self.client.sync_once(SyncSettings::default()).await
        {
            tracing::warn!("initial matrix sync failed: {error}");
        }

        self.join_rooms().await;
        self.register_handlers();

        loop {
            if let Err(error) = self.client.sync(SyncSettings::default()).await {
                tracing::warn!("matrix sync stopped: {error}, restarting");
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }

    async fn join_rooms(&self) {
        for room_id in &self.rooms {
            let joined = self
                .client
                .get_room(room_id)
                .is_some_and(|room| room.state() == RoomState::Joined);
            if joined {
                continue;
            }
            match self.client.join_room_by_id(room_id).await {
                Ok(_) => tracing::info!(%room_id, "joined matrix room"),
                Err(error) => tracing::warn!(
                    %room_id,
                    "can't join matrix room yet, invite the bot: {error}"
                ),
            }
        }
        self.waker.wake(&Network::Matrix);
    }

    fn register_handlers(&self) {
        let rooms: Arc<HashSet<OwnedRoomId>> = Arc::new(self.rooms.iter().cloned().collect());

        let store = self.store_received_message.clone();
        let who = self.who_is_on_the_other_side.clone();
        let bridged = rooms.clone();
        self.client.add_event_handler(
            move |event: OriginalSyncRoomMessageEvent, room: Room, client: Client| {
                let store = store.clone();
                let who = who.clone();
                let bridged = bridged.clone();
                async move {
                    if !bridged.contains(room.room_id()) || client.user_id() == Some(&event.sender)
                    {
                        return;
                    }
                    on_room_message(event, room, &store, &who).await;
                }
            },
        );

        let waker = self.waker.clone();
        self.client.add_event_handler(
            move |event: StrippedRoomMemberEvent, room: Room, client: Client| {
                let waker = waker.clone();
                let bridged = rooms.clone();
                async move {
                    if Some(event.state_key.as_ref()) != client.user_id()
                        || !bridged.contains(room.room_id())
                    {
                        return;
                    }
                    match room.join().await {
                        Ok(()) => {
                            tracing::info!(room_id = %room.room_id(), "accepted invite");
                            waker.wake(&Network::Matrix);
                        }
                        Err(error) => {
                            tracing::warn!(room_id = %room.room_id(), "joining failed: {error}")
                        }
                    }
                }
            },
        );
    }
}

async fn on_room_message(
    event: OriginalSyncRoomMessageEvent,
    room: Room,
    store: &StoreReceivedMessageHandler,
    who: &WhoIsOnTheOtherSideHandler,
) {
    let (kind, body) = match &event.content.msgtype {
        MessageType::Emote(emote) => (MessageKind::Action, emote.body.clone()),
        msgtype @ (MessageType::Image(_)
        | MessageType::File(_)
        | MessageType::Video(_)
        | MessageType::Audio(_)) => (
            MessageKind::Normal,
            format!(
                "[{}] {}",
                msgtype.msgtype().trim_start_matches("m."),
                msgtype.body()
            ),
        ),
        msgtype => (MessageKind::Normal, msgtype.body().to_owned()),
    };
    let Ok(body) = MessageBody::new(&body) else {
        return;
    };

    let sender = match room.get_member_no_sync(&event.sender).await {
        Ok(Some(member)) => member.name().to_owned(),
        _ => event.sender.localpart().to_owned(),
    };

    let ids = MatrixRoom::new(room.room_id().as_str())
        .and_then(|source| Ok((source, MatrixEventId::new(event.event_id.as_str())?)));
    let (source, event_id) = match ids {
        Ok(ids) => ids,
        Err(error) => {
            tracing::error!(room_id = %room.room_id(), "{error:#}");
            return;
        }
    };
    let message = Message::new(Channel::Matrix(source), Some(event_id), sender, kind, body);
    match message.command() {
        Some(BotCommand::WhoIsOnTheOtherSide) => {
            who_is_on_the_other_side(message.source().clone(), &room, who).await
        }
        None => {
            if let Err(error) = store.handle(StoreReceivedMessage { message }).await {
                tracing::error!(room_id = %room.room_id(), "storing message failed: {error:#}");
            }
        }
    }
}

async fn who_is_on_the_other_side(
    channel: Channel,
    room: &Room,
    handler: &WhoIsOnTheOtherSideHandler,
) {
    let members = match handler.handle(WhoIsOnTheOtherSide { channel }).await {
        Ok(Some(members)) => members,
        Ok(None) => return,
        Err(error) => {
            tracing::warn!(room_id = %room.room_id(), "listing the other side failed: {error:#}");
            return;
        }
    };
    let reply = RoomMessageEventContent::notice_plain(members.reply());
    if let Err(error) = room.send(reply).await {
        tracing::warn!(room_id = %room.room_id(), "replying failed: {error:#}");
    }
}

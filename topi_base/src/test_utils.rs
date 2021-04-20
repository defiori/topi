// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use crate::messaging::{
    Channels, Envelope, IpcReceiver, IpcSender, Message, Messenger, Sender, ThreadReceiver,
};
use crate::nodes::{Node, NodeConfig};
use crate::thread::{ThreadCollection, ThreadCollectionBuilder};
use crate::Error;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::HashMap;
use std::convert;

#[derive(Debug, Serialize, Deserialize)]
pub enum TestMessage {
    Ping(String),
    Pong(String),
    Go,
    Quit,
}

pub struct TestMessenger<M>
where
    M: Serialize + DeserializeOwned,
{
    owning_thread_id: String,
    send_peers: HashMap<String, Sender<M>>,
    recv_peers: ThreadReceiver<M>,
    channels: Channels<M>,
}

impl<M> Messenger<M> for TestMessenger<M>
where
    M: Message + Serialize,
{
    fn new(channels: Channels<M>) -> Self {
        TestMessenger {
            owning_thread_id: channels.owning_thread_id(),
            recv_peers: channels.receiver_from_peers(),
            send_peers: channels.send_to_peers(),
            channels,
        }
    }

    fn channels(&mut self) -> &mut Channels<M> {
        &mut self.channels
    }
}

impl<M> TestMessenger<M>
where
    M: Message + Serialize,
{
    pub fn send<S: convert::AsRef<str>>(&self, recv_id: S, msg: M) -> Result<(), Error> {
        if let Some(sender) = self.send_peers.get(recv_id.as_ref()) {
            match sender {
                Sender::ProcLocal(tx) => {
                    tx.send(msg)?;
                }
                Sender::Ipc(tx) => {
                    tx.send(msg)?;
                }
            }
            Ok(())
        } else {
            Err(Error::UnknownSendDestination(String::from(
                recv_id.as_ref(),
            )))
        }
    }

    pub fn recv(&self) -> Result<M, Error> {
        match self.recv_peers.recv()? {
            Envelope::Message(msg) => Ok(msg),
            Envelope::Terminate => panic!(),
        }
    }

    pub fn self_id(&self) -> &str {
        self.owning_thread_id.as_ref()
    }
}

pub struct HoppingNode {
    index: usize,
    next_hop: String,
    should_be: [u8; 3],
}

impl HoppingNode {
    pub fn new(index: usize, next_hop: String, should_be: [u8; 3]) -> HoppingNode {
        HoppingNode {
            index,
            next_hop,
            should_be,
        }
    }

    pub fn from_config(cfg: HoppingConfig) -> Self {
        let HoppingConfig {
            index,
            next_hop,
            should_be,
        } = cfg;
        HoppingNode {
            index,
            next_hop,
            should_be,
        }
    }
}

impl Node<[u8; 3], TestMessenger<[u8; 3]>> for HoppingNode {
    fn entrypoint(&mut self, messenger: &mut TestMessenger<[u8; 3]>) -> Result<(), String> {
        let mut msg = messenger.recv().unwrap();
        assert_eq!(msg, self.should_be);
        msg[self.index] = 1;
        messenger.send(&self.next_hop, msg).unwrap();
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct HoppingConfig {
    pub index: usize,
    pub next_hop: String,
    pub should_be: [u8; 3],
}

impl HoppingConfig {
    pub fn new(index: usize, next_hop: String, should_be: [u8; 3]) -> HoppingConfig {
        HoppingConfig {
            index,
            next_hop,
            should_be,
        }
    }
}

impl NodeConfig<[u8; 3]> for HoppingConfig {
    fn must_be_main_thread(&self) -> bool {
        false
    }

    fn add_node<S: AsRef<str>>(
        self,
        id: S,
        thread_builder: ThreadCollectionBuilder<[u8; 3]>,
    ) -> Result<ThreadCollectionBuilder<[u8; 3]>, Error> {
        thread_builder.add_thread(id, HoppingNode::from_config(self))
    }

    #[allow(clippy::type_complexity)]
    fn add_node_as_main_with_controller<S, F>(
        self,
        id: S,
        thread_builder: ThreadCollectionBuilder<[u8; 3]>,
        ipc_pair: Option<(IpcSender<[u8; 3]>, IpcReceiver<[u8; 3]>)>,
        controller: F,
    ) -> Result<(), Error>
    where
        S: AsRef<str>,
        F: FnOnce(ThreadCollection<[u8; 3]>) -> Result<(), Error> + Send + 'static,
    {
        thread_builder.build_with_main_thread_and_controller(
            id,
            HoppingNode::from_config(self),
            ipc_pair,
            controller,
        )
    }
}

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use serde::{Deserialize, Serialize};
use topi_base::{
    data::{Content, DataMessage, DataMessenger},
    messaging::{Envelope, IpcReceiver, IpcSender},
    nodes::{Node, NodeConfig},
    test_utils::TestMessage,
    thread::{ThreadCollection, ThreadCollectionBuilder},
    Error,
};

#[derive(Clone, Serialize, Deserialize)]
pub enum TestConfig {
    TestPingAll {
        destinations: Vec<String>,
        next: String,
        must_be_main_thread: bool,
    },
    TestSendQuit {
        destinations: Vec<String>,
    },
    BenchTransfer {
        next_hop: String,
        should_be: f64,
    },
}

impl NodeConfig<BenchMessage> for TestConfig {
    fn must_be_main_thread(&self) -> bool {
        use TestConfig::*;
        match self {
            BenchTransfer { .. } => false,
            TestPingAll {
                must_be_main_thread,
                ..
            } => *must_be_main_thread,
            TestSendQuit { .. } => false,
        }
    }

    fn add_node<S: AsRef<str>>(
        self,
        id: S,
        thread_builder: ThreadCollectionBuilder<BenchMessage>,
    ) -> Result<ThreadCollectionBuilder<BenchMessage>, Error> {
        use TestConfig::*;
        match self {
            BenchTransfer { .. } => {
                thread_builder.add_thread(id, TransferBenchNode::from_config(self))
            }
            TestPingAll { .. } => thread_builder.add_thread(id, PingAll::from_config(self)),
            TestSendQuit { .. } => thread_builder.add_thread(id, SendQuit::from_config(self)),
        }
    }

    fn add_node_as_main_with_controller<S, F>(
        self,
        id: S,
        thread_builder: ThreadCollectionBuilder<BenchMessage>,
        ipc_pair: Option<(IpcSender<BenchMessage>, IpcReceiver<BenchMessage>)>,
        controller: F,
    ) -> Result<(), Error>
    where
        S: AsRef<str>,
        F: FnOnce(ThreadCollection<BenchMessage>) -> Result<(), Error> + Send + 'static,
    {
        use TestConfig::*;
        match self {
            BenchTransfer { .. } => thread_builder.build_with_main_thread_and_controller(
                id,
                TransferBenchNode::from_config(self),
                ipc_pair,
                controller,
            ),
            TestPingAll { .. } => thread_builder.build_with_main_thread_and_controller(
                id,
                PingAll::from_config(self),
                ipc_pair,
                controller,
            ),
            TestSendQuit { .. } => thread_builder.build_with_main_thread_and_controller(
                id,
                SendQuit::from_config(self),
                ipc_pair,
                controller,
            ),
        }
    }
}

pub type BenchMessage = DataMessage<TestMessage, ()>;

pub struct TransferBenchNode {
    pub next_hop: String,
    pub should_be: f64,
}

impl TransferBenchNode {
    pub fn from_config(config: TestConfig) -> Self {
        if let TestConfig::BenchTransfer {
            next_hop,
            should_be,
        } = config
        {
            TransferBenchNode {
                next_hop,
                should_be,
            }
        } else {
            panic!("TransferBenchNode received config for wrong language.")
        }
    }
}

impl Node<BenchMessage, DataMessenger<TestMessage, ()>> for TransferBenchNode {
    fn entrypoint(&mut self, messenger: &mut DataMessenger<TestMessage, ()>) -> Result<(), String> {
        loop {
            match messenger.recv().unwrap() {
                Envelope::Terminate => break,
                Envelope::Message(msg) => match msg {
                    DataMessage {
                        content: Content::ArrayMutFloat64(mut array),
                        ..
                    } => {
                        assert!((array[0] - self.should_be).abs() < f64::EPSILON);
                        array[0] += 1.0;
                        messenger.send(&self.next_hop, array, None).unwrap();
                    }
                    DataMessage {
                        content: Content::SharedArrayMutFloat64(mut array),
                        ..
                    } => {
                        {
                            let mut view = array.as_view_mut();
                            assert!((view[0] - self.should_be).abs() < f64::EPSILON);
                            view[0] += 1.0;
                        }
                        messenger.send(&self.next_hop, array, None).unwrap();
                    }
                    DataMessage {
                        content: Content::UserData(TestMessage::Quit),
                        ..
                    } => break,
                    _ => panic!("Unexpected message."),
                },
            }
        }
        Ok(())
    }
}

pub struct PingAll {
    destinations: Vec<String>,
    next: String,
}

impl PingAll {
    pub fn new(destinations: Vec<String>, next: String) -> PingAll {
        PingAll { destinations, next }
    }

    pub fn from_config(config: TestConfig) -> Self {
        if let TestConfig::TestPingAll {
            destinations, next, ..
        } = config
        {
            PingAll { destinations, next }
        } else {
            panic!("PingAll received config for wrong language.")
        }
    }
}

impl Node<BenchMessage, DataMessenger<TestMessage, ()>> for PingAll {
    fn entrypoint(&mut self, messenger: &mut DataMessenger<TestMessage, ()>) -> Result<(), String> {
        loop {
            if let Envelope::Message(DataMessage {
                content: Content::UserData(user_data),
                ..
            }) = messenger.recv().unwrap()
            {
                match user_data {
                    TestMessage::Go => {
                        for dest in &self.destinations {
                            messenger
                                .send_user_data(
                                    dest,
                                    TestMessage::Ping(String::from(messenger.node_id())),
                                    None,
                                )
                                .unwrap();
                            if let Envelope::Message(DataMessage {
                                content: Content::UserData(TestMessage::Pong(ref reply)),
                                ..
                            }) = messenger.recv().unwrap()
                            {
                                assert_eq!(reply, dest);
                            } else {
                                panic!();
                            }
                        }
                        messenger
                            .send_user_data(self.next.as_str(), TestMessage::Go, None)
                            .unwrap();
                    }
                    TestMessage::Ping(from) => {
                        messenger
                            .send_user_data(
                                from,
                                TestMessage::Pong(String::from(messenger.node_id())),
                                None,
                            )
                            .unwrap();
                    }
                    TestMessage::Quit => break,
                    _ => panic!(),
                }
            } else {
                panic!("PingAll node received unexpected message variant.")
            }
        }
        Ok(())
    }
}

pub struct SendQuit {
    destinations: Vec<String>,
}

impl SendQuit {
    pub fn new(destinations: Vec<String>) -> SendQuit {
        SendQuit { destinations }
    }

    pub fn from_config(config: TestConfig) -> Self {
        if let TestConfig::TestSendQuit { destinations } = config {
            SendQuit { destinations }
        } else {
            panic!("SendQuit received config for wrong language.")
        }
    }
}

impl Node<BenchMessage, DataMessenger<TestMessage, ()>> for SendQuit {
    fn entrypoint(&mut self, messenger: &mut DataMessenger<TestMessage, ()>) -> Result<(), String> {
        if let Envelope::Message(DataMessage {
            content: Content::UserData(TestMessage::Go),
            ..
        }) = messenger.recv().unwrap()
        {
            for dest in &self.destinations {
                messenger
                    .send_user_data(dest, TestMessage::Quit, None)
                    .unwrap();
            }
            Ok(())
        } else {
            panic!(
                "SendQuit node '{}' received unexpected message.",
                messenger.node_id()
            )
        }
    }
}

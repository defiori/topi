// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::collections::HashMap;
use topi::test_utils::{PingAll, TestConfig};
use topi_base::{
    data::{Content, DataMessage, DataMessenger},
    messaging::Envelope,
    nodes::{NodeConfig, NodeSender},
    process::ProcessCollectionBuilder,
    test_utils::TestMessage,
    thread::ThreadCollectionBuilder,
};

fn test_node_cfgs<'a>(
    sequence: impl std::iter::Iterator<Item = &'a str> + Clone,
    additional_destinations: impl std::iter::Iterator<Item = &'a str> + Clone,
    make_last_main_thread: bool,
) -> HashMap<String, TestConfig> {
    let mut ping_node_cfgs = HashMap::new();
    let mut sequence_peek = sequence.clone().peekable();
    while let Some(current) = sequence_peek.next() {
        if let Some(&next) = sequence_peek.peek() {
            ping_node_cfgs.insert(
                String::from(current),
                TestConfig::TestPingAll {
                    destinations: sequence
                        .clone()
                        .chain(additional_destinations.clone())
                        .filter(|&id| id != current)
                        .map(String::from)
                        .collect(),
                    next: String::from(next),
                    must_be_main_thread: false,
                },
            );
        } else {
            ping_node_cfgs.insert(
                String::from(current),
                TestConfig::TestPingAll {
                    destinations: sequence
                        .clone()
                        .chain(additional_destinations.clone())
                        .filter(|&id| id != current)
                        .map(String::from)
                        .collect(),
                    next: String::from("Q"),
                    must_be_main_thread: make_last_main_thread,
                },
            );
        }
    }
    ping_node_cfgs.insert(
        String::from("Q"),
        TestConfig::TestSendQuit {
            destinations: sequence.map(String::from).collect(),
        },
    );
    ping_node_cfgs
}

fn make_procs<'a>(
    sequence: impl std::iter::Iterator<Item = &'a str> + Clone,
    mut cfgs: HashMap<String, TestConfig>,
) -> ProcessCollectionBuilder<DataMessage<TestMessage, ()>, TestConfig> {
    let mut procs = ProcessCollectionBuilder::new();
    for &group in ["P1", "P2", "P3"].iter() {
        let mut selection = HashMap::new();
        for node in sequence.clone().filter(|&id| id.starts_with(group)) {
            selection.insert(String::from(node), cfgs.remove(node).unwrap());
        }
        procs = procs
            .add_process(group, selection, Some("topi_proc_test"))
            .unwrap();
    }
    procs
        .add_process("PQ", cfgs, Some("topi_proc_test"))
        .unwrap()
}

#[test]
fn procs() {
    let sequence = ["P1T1", "P2T2", "P2T1", "P3T2", "P1T2", "P3T1"];
    let cfgs = test_node_cfgs(sequence.iter().copied(), std::iter::empty(), false);
    let procs = make_procs(sequence.iter().copied(), cfgs);
    let procs = procs.build().unwrap();
    procs
        .ipc_sender("P1T1")
        .unwrap()
        .send(DataMessage::new_from_user(TestMessage::Go, None))
        .unwrap();
    procs.wait_for_all().unwrap();
}

#[test]
fn procs_with_remote_main_thread() {
    let sequence = ["P1T1", "P2T2", "P2T1", "P3T2", "P1T2", "P3T1"];
    let cfgs = test_node_cfgs(sequence.iter().copied(), std::iter::empty(), true);
    let procs = make_procs(sequence.iter().copied(), cfgs);
    let procs = procs.build().unwrap();
    procs
        .ipc_sender("P1T1")
        .unwrap()
        .send(DataMessage::new_from_user(TestMessage::Go, None))
        .unwrap();
    procs.wait_for_all().unwrap();
}

#[test]
fn procs_with_messenger() {
    let sequence = ["P1T1", "P2T2", "P2T1", "P3T2", "P1T2", "P3T1"];
    let cfgs = test_node_cfgs(
        sequence.iter().copied(),
        ["MESSENGER"].iter().copied(),
        false,
    );
    let procs = make_procs(sequence.iter().copied(), cfgs);
    let (procs, messenger): (_, DataMessenger<TestMessage, ()>) =
        procs.build_with_messenger("MESSENGER").unwrap();
    messenger
        .send_user_data("P1T1", TestMessage::Go, None)
        .unwrap();
    for &expected in sequence.iter() {
        if let Envelope::Message(DataMessage {
            content: Content::UserData(TestMessage::Ping(ref from)),
            ..
        }) = messenger.recv().unwrap()
        {
            assert_eq!(expected, from);
            messenger
                .send_user_data(
                    from,
                    TestMessage::Pong(String::from(messenger.node_id())),
                    None,
                )
                .unwrap();
        } else {
            panic!();
        }
    }
    procs.wait_for_all().unwrap();
}

#[allow(clippy::type_complexity)]
fn make_procs_with_local_threads<'a>(
    sequence: impl std::iter::Iterator<Item = &'a str> + Clone,
    cfgs: &mut HashMap<String, TestConfig>,
    local_nodes: impl std::iter::Iterator<Item = &'a str>,
) -> (
    ProcessCollectionBuilder<DataMessage<TestMessage, ()>, TestConfig>,
    ThreadCollectionBuilder<DataMessage<TestMessage, ()>>,
) {
    let mut procs = ProcessCollectionBuilder::new();
    for &group in ["P2", "P3"].iter() {
        let mut selection = HashMap::new();
        for node in sequence.clone().filter(|&id| id.starts_with(group)) {
            selection.insert(String::from(node), cfgs.remove(node).unwrap());
        }
        procs = procs
            .add_process(group, selection, Some("topi_proc_test"))
            .unwrap();
    }
    let mut local_threads = ThreadCollectionBuilder::new();
    for node in local_nodes {
        local_threads = cfgs
            .remove(node)
            .unwrap()
            .add_node(node, local_threads)
            .unwrap();
    }
    (procs, local_threads)
}

#[test]
fn procs_with_local_threads() {
    let sequence = ["P1T1", "P2T2", "P2T1", "P3T2", "P1T2", "P3T1"];
    let local_nodes = ["P1T1", "P1T2", "Q"];
    let mut cfgs = test_node_cfgs(sequence.iter().copied(), std::iter::empty(), false);
    let (procs, local_threads) = make_procs_with_local_threads(
        sequence.iter().copied(),
        &mut cfgs,
        local_nodes.iter().copied(),
    );
    local_threads
        .ipc_sender("P1T1")
        .unwrap()
        .send(DataMessage::new_from_user(TestMessage::Go, None))
        .unwrap();
    assert!(cfgs.is_empty());
    let procs = procs.build_with_local_threads(local_threads).unwrap();
    procs.wait_for_all().unwrap();
}

#[test]
fn procs_with_local_threads_and_messenger() {
    let sequence = ["P1T1", "P2T2", "P2T1", "P3T2", "P1T2", "P3T1"];
    let local_nodes = ["P1T1", "P1T2", "Q"];
    let mut cfgs = test_node_cfgs(
        sequence.iter().copied(),
        ["MESSENGER"].iter().copied(),
        false,
    );
    let (procs, local_threads) = make_procs_with_local_threads(
        sequence.iter().copied(),
        &mut cfgs,
        local_nodes.iter().copied(),
    );
    assert!(cfgs.is_empty());
    let (procs, messenger): (_, DataMessenger<TestMessage, ()>) = procs
        .build_with_local_threads_and_messenger(local_threads, "MESSENGER")
        .unwrap();
    messenger
        .send_user_data("P1T1", TestMessage::Go, None)
        .unwrap();
    for &expected in sequence.iter() {
        if let Envelope::Message(DataMessage {
            content: Content::UserData(TestMessage::Ping(ref from)),
            ..
        }) = messenger.recv().unwrap()
        {
            assert_eq!(expected, from);
            messenger
                .send_user_data(
                    from,
                    TestMessage::Pong(String::from(messenger.node_id())),
                    None,
                )
                .unwrap();
        } else {
            panic!();
        }
    }
    procs.wait_for_all().unwrap();
}

#[test]
fn procs_with_local_main_thread() {
    let sequence = ["P1T1", "P2T2", "P2T1", "P3T2", "P1T2", "P3T1"];
    let local_nodes = ["P1T1", "Q"];
    let mut cfgs = test_node_cfgs(sequence.iter().copied(), std::iter::empty(), false);
    let (procs, local_threads) = make_procs_with_local_threads(
        sequence.iter().copied(),
        &mut cfgs,
        local_nodes.iter().copied(),
    );
    let main_node = PingAll::from_config(cfgs.remove("P1T2").unwrap());
    local_threads
        .ipc_sender("P1T1")
        .unwrap()
        .send(DataMessage::new_from_user(TestMessage::Go, None))
        .unwrap();
    assert!(cfgs.is_empty());
    procs
        .build_with_local_main_thread(local_threads, "P1T2", main_node)
        .unwrap();
}

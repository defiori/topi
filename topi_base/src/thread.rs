// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use crate::messaging::{
    create_converting_router, create_messenger, ipc_channel, thread_channel, Channels, Envelope,
    IpcReceiver, IpcSender, Message, Messenger, Sender, ThreadSender,
};
use crate::nodes::{Node, NodeInfo, NodeSender};
use crate::{Error, Status};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;
use std::convert::AsRef;
use std::thread;

struct ThreadBuilder<M>
where
    M: Serialize + DeserializeOwned,
{
    send_build: crossbeam_channel::Sender<Channels<M>>,
    handle: ThreadHandle<M>,
}

impl<M> ThreadBuilder<M>
where
    M: Serialize + DeserializeOwned,
{
    fn build(self, channels: Channels<M>) -> ThreadHandle<M> {
        self.send_build.send(channels).unwrap();
        self.handle
    }
}

pub struct ThreadCollectionBuilder<M>
where
    M: Serialize + DeserializeOwned,
{
    channels: Vec<(String, Channels<M>, ThreadBuilder<M>)>,
    messenger_senders: Vec<(String, ThreadSender<M>)>,
    ipc_destinations: HashMap<String, IpcSender<M>>,
    recv_children: Option<crossbeam_channel::Receiver<ThreadMessage>>,
    send_self: crossbeam_channel::Sender<ThreadMessage>,
}

impl<M> ThreadCollectionBuilder<M>
where
    M: Message + Serialize,
{
    #[allow(clippy::new_without_default)]
    pub fn new() -> ThreadCollectionBuilder<M> {
        let (send_self, recv_children) = crossbeam_channel::unbounded();
        ThreadCollectionBuilder {
            channels: Vec::new(),
            messenger_senders: Vec::new(),
            ipc_destinations: HashMap::new(),
            recv_children: Some(recv_children),
            send_self,
        }
    }

    pub fn add_thread<S: AsRef<str>, N: Node<M, MR>, MR: Messenger<M>>(
        mut self,
        // TODO: id can't contain null bytes to work with `thead::Builder::name()`
        id: S,
        mut node: N,
    ) -> Result<ThreadCollectionBuilder<M>, Error> {
        let id = id.as_ref();
        if self
            .channels
            .iter()
            .map(|&(ref existing, _, _)| existing)
            .chain(self.ipc_destinations.keys())
            .any(|existing| existing == id)
        {
            Err(Error::DuplicateNodeName(String::from(id)))
        } else {
            // Spawn thread which owns the node and waits on a Messenger
            let (send_build, recv_build) = crossbeam_channel::bounded(1);
            let handle = thread::Builder::new()
                .name(String::from(id))
                .spawn(move || {
                    let channels: Channels<M> = recv_build.recv().unwrap();
                    let send_parent = channels.send_parent.as_ref().unwrap().clone();
                    let id = channels.owning_thread_id.clone();
                    let mut messenger = MR::new(channels);
                    // TODO panic handling in node
                    match std::panic::catch_unwind(move || node.entrypoint(&mut messenger)) {
                        Ok(ret_value) => match ret_value {
                            Ok(_) => send_parent.send(ThreadMessage::Success(id)).unwrap(),
                            // TODO: process/log error message
                            Err(_) => send_parent.send(ThreadMessage::Failure(id)).unwrap(),
                        },
                        Err(_) => send_parent.send(ThreadMessage::Failure(id)).unwrap(),
                    }
                })?;
            // This is the Messenger tx/rx pair
            let (send_child, recv_peers) = thread_channel()?;
            // Create router to convert ipc messages to crossbeam ones
            let (send_child_ipc, recv_peers_ipc) = ipc_channel()?;
            create_converting_router(id, send_child.clone(), recv_peers_ipc)?;
            let thread_builder = ThreadBuilder {
                send_build,
                handle: ThreadHandle {
                    handle: Some(handle),
                    send_child: send_child.clone(),
                    send_child_ipc: send_child_ipc.clone(),
                    status: Status::Running,
                },
            };
            let channels = Channels {
                owning_thread_id: String::from(id),
                send_parent: Some(self.send_self.clone()),
                recv_peers,
                send_peers: HashMap::new(),
                send_self_ipc: send_child_ipc,
            };
            self.channels
                .push((String::from(id), channels, thread_builder));
            self.messenger_senders.push((String::from(id), send_child));
            Ok(self)
        }
    }

    pub fn add_ipc_destination<S: AsRef<str>>(
        mut self,
        id: S,
        sender: IpcSender<M>,
    ) -> Result<ThreadCollectionBuilder<M>, Error> {
        let id = id.as_ref();
        if self
            .channels
            .iter()
            .map(|&(ref existing, _, _)| existing)
            .chain(self.ipc_destinations.keys())
            .any(|existing| existing == id)
        {
            Err(Error::DuplicateNodeName(String::from(id)))
        } else {
            self.ipc_destinations.insert(String::from(id), sender);
            Ok(self)
        }
    }

    pub fn build(mut self) -> Result<ThreadCollection<M>, Error> {
        // Add intra-process senders to all messengers (not to itself)
        for (i, (_, channels, _)) in self.channels.iter_mut().enumerate() {
            for (id, sender) in self
                .messenger_senders
                .iter()
                .enumerate()
                .filter_map(|(j, t)| if j != i { Some(t) } else { None })
            {
                channels
                    .send_peers
                    .insert(id.clone(), Sender::ProcLocal(sender.clone()))
                    .and(Some(Result::<ThreadCollection<M>, Error>::Err(
                        Error::DuplicateNodeName(String::from(id)),
                    )))
                    .transpose()?;
            }
        }
        // Add inter-process senders to all messengers
        for (id, sender) in self.ipc_destinations.into_iter() {
            for &mut (_, ref mut channels, _) in self.channels.iter_mut() {
                channels
                    .send_peers
                    .insert(id.clone(), Sender::Ipc(sender.clone()))
                    .and(Some(Result::<ThreadCollection<M>, Error>::Err(
                        Error::DuplicateNodeName(id.clone()),
                    )))
                    .transpose()?;
            }
        }
        // Run node entrypoints
        let mut threads = HashMap::new();
        for (id, msg_builder, thread_builder) in self.channels.into_iter() {
            threads.insert(id, thread_builder.build(msg_builder));
        }
        Ok(ThreadCollection {
            recv_children: self.recv_children.take().unwrap(),
            threads,
        })
    }

    pub fn build_with_messenger<S: AsRef<str>, MR: Messenger<M>>(
        self,
        id: S,
    ) -> Result<(ThreadCollection<M>, MR), Error> {
        let (threads, main_messenger, _) = self.internal_build_with_messenger(id, None)?;
        Ok((threads, main_messenger))
    }

    fn internal_build_with_messenger<S: AsRef<str>, MR: Messenger<M>>(
        mut self,
        id: S,
        ipc_pair: Option<(IpcSender<M>, IpcReceiver<M>)>,
    ) -> Result<(ThreadCollection<M>, MR, ThreadSender<M>), Error> {
        let id = id.as_ref();
        // Set up the main messenger
        let (main_messenger, send_main, _) = create_messenger::<_, _, _, _, _, _, MR, M>(
            id,
            self.channels
                .iter()
                .map(|&(ref existing, _, _)| existing)
                .chain(self.ipc_destinations.keys())
                .map(String::as_str),
            self.messenger_senders
                .iter()
                .map(|(id, sender)| (id, sender)),
            self.ipc_destinations.iter(),
            None,
            ipc_pair,
        )?;
        self.messenger_senders
            .push((String::from(id), send_main.clone()));
        let threads = self.build()?;
        Ok((threads, main_messenger, send_main))
    }

    pub fn build_with_main_thread<S, MR, N>(
        self,
        id: S,
        mut node: N,
        ipc_pair: Option<(IpcSender<M>, IpcReceiver<M>)>,
    ) -> Result<(), Error>
    where
        S: AsRef<str>,
        MR: Messenger<M>,
        N: Node<M, MR>,
    {
        let id = id.as_ref();
        let (threads, mut main_messenger, _) = self.internal_build_with_messenger(id, ipc_pair)?;
        // Start main thread
        match std::panic::catch_unwind(move || node.entrypoint(&mut main_messenger)) {
            Ok(Ok(_)) => {
                // TODO: does it make sense for the main thread to wait on all others?
                threads.wait_for_all()
            }
            // Should the main thread signal the others to stop?
            Ok(Err(_)) | Err(_) => {
                let mut failed_ids = vec![String::from(id)];
                match threads.wait_for_all() {
                    Ok(_) => Err(Error::ThreadFailure(failed_ids)),
                    Err(Error::ThreadFailure(mut extra_ids)) => {
                        failed_ids.append(&mut extra_ids);
                        Err(Error::ThreadFailure(failed_ids))
                    }
                    _ => panic!("Received unexpected error from wait_on_completion."),
                }
            }
        }
    }

    pub fn build_with_main_thread_and_controller<S, MR, N, F>(
        self,
        id: S,
        mut node: N,
        ipc_pair: Option<(IpcSender<M>, IpcReceiver<M>)>,
        controller: F,
    ) -> Result<(), Error>
    where
        S: AsRef<str>,
        MR: Messenger<M>,
        N: Node<M, MR>,
        F: FnOnce(ThreadCollection<M>) -> Result<(), Error> + Send + 'static,
    {
        let id = id.as_ref();
        // Get sender to ThreadCollection.
        let send_parent = self.send_self.clone();
        // Build ThreadCollection.
        let (mut threads, mut main_messenger, send_main): (_, MR, _) =
            self.internal_build_with_messenger(id, ipc_pair)?;
        // Add sender to ThreadCollection to main messenger.
        main_messenger.channels().send_parent = Some(send_parent.clone());
        // Create main messenger thread handle, inject into ThreadCollection.
        let main_handle = ThreadHandle {
            handle: None,
            send_child: send_main,
            send_child_ipc: main_messenger.channels().send_self_ipc.clone(),
            status: Status::Running,
        };
        assert!(threads
            .threads
            .insert(String::from(id), main_handle)
            .is_none());
        // Move controller into separate thread.
        let controller_handle = thread::spawn(move || controller(threads));
        // Start main thread.
        match std::panic::catch_unwind(move || node.entrypoint(&mut main_messenger)) {
            Ok(ret_value) => match ret_value {
                Ok(_) => send_parent
                    .send(ThreadMessage::Success(String::from(id)))
                    .unwrap(),
                Err(_) => send_parent
                    .send(ThreadMessage::Failure(String::from(id)))
                    .unwrap(),
            },
            Err(_) => send_parent
                .send(ThreadMessage::Failure(String::from(id)))
                .unwrap(),
        };
        // Wait with return until the controller has finished.
        match controller_handle.join() {
            Ok(result) => result,
            Err(_) => Err(Error::ControllerThreadPanic),
        }
    }
}

#[derive(Debug)]
pub(crate) enum ThreadMessage {
    Success(String),
    Failure(String),
}

#[allow(clippy::type_complexity)]
pub struct ThreadCollectionBuilderIter<'a, M>
where
    M: Message,
{
    inner: std::iter::Map<
        std::slice::Iter<'a, (String, ThreadSender<M>)>,
        fn(&(String, ThreadSender<M>)) -> &str,
    >,
}

impl<'a, M> std::iter::Iterator for ThreadCollectionBuilderIter<'a, M>
where
    M: Message,
{
    type Item = &'a str;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

impl<'a, M> NodeInfo<'a> for ThreadCollectionBuilder<M>
where
    M: Message + Serialize,
{
    type List = ThreadCollectionBuilderIter<'a, M>;

    fn contained_nodes(&'a self) -> Self::List {
        ThreadCollectionBuilderIter {
            inner: self.messenger_senders.iter().map(|(id, _)| id.as_str()),
        }
    }
}

impl<M> NodeSender<M> for ThreadCollectionBuilder<M>
where
    M: Message + Serialize,
{
    fn sender<S: AsRef<str>>(&self, to: S) -> Option<&ThreadSender<M>> {
        self.channels.iter().find_map(|(id_iter, _, builder)| {
            if id_iter.as_str() == to.as_ref() {
                Some(&builder.handle.send_child)
            } else {
                None
            }
        })
    }

    fn ipc_sender<S: AsRef<str>>(&self, to: S) -> Option<&IpcSender<M>> {
        self.channels.iter().find_map(|(id_iter, _, builder)| {
            if id_iter.as_str() == to.as_ref() {
                Some(&builder.handle.send_child_ipc)
            } else {
                None
            }
        })
    }
}

#[allow(dead_code)]
struct ThreadHandle<M> {
    handle: Option<thread::JoinHandle<()>>,
    pub send_child: ThreadSender<M>,
    pub send_child_ipc: IpcSender<M>,
    pub status: Status,
    /* TODO
     * send_stats: crossbeam_channel::Sender<> */
}

impl<M> ThreadHandle<M> {
    fn send_terminate_signal(&self) {
        if let Status::Running = self.status {
            self.send_child.send_internal(Envelope::Terminate).ok();
        }
    }
}

pub struct ThreadCollection<M> {
    // TODO: one `recv_child` per thread and use 'sender dropped' error as indication of
    // thread termination?
    recv_children: crossbeam_channel::Receiver<ThreadMessage>,
    threads: HashMap<String, ThreadHandle<M>>,
}

impl<M> Drop for ThreadCollection<M> {
    fn drop(&mut self) {
        // Send termination signal to all threads.
        for handle in self.threads.values() {
            handle.send_terminate_signal();
        }
        // Wait for threads to return.
        self.internal_wait_for_all().ok();
    }
}

impl<M> ThreadCollection<M> {
    pub fn wait_for_all_or_failure_with_interrupt(
        mut self,
        interrupt: crossbeam_channel::Receiver<()>,
    ) -> Result<(), Error> {
        // Check thread status in case messages from children threads have already been
        // processed.
        let failed: Vec<_> = self
            .threads
            .iter()
            .filter_map(|(id, handle)| {
                if let Status::Failure = handle.status {
                    Some(String::from(id))
                } else {
                    None
                }
            })
            .collect();
        if !failed.is_empty() {
            return Err(Error::ThreadFailure(failed));
        }
        if self
            .threads
            .values()
            .all(|handle| matches!(handle.status, Status::Success))
        {
            return Ok(());
        }
        let mut select = crossbeam_channel::Select::new();
        select.recv(&self.recv_children);
        select.recv(&interrupt);
        loop {
            let oper = select.select();
            if oper.index() == 0 {
                match oper.recv(&self.recv_children)? {
                    ThreadMessage::Success(from) => {
                        // Set thread status
                        self.threads
                            .get_mut(&from)
                            .unwrap_or_else(|| {
                                panic!(
                                    "ThreadCollection received message from unknown child '{}'.",
                                    from
                                )
                            })
                            .status = Status::Success;
                        // Check whether all threads are done
                        if self
                            .threads
                            .values()
                            .all(|handle| matches!(handle.status, Status::Success))
                        {
                            break Ok(());
                        } else {
                            continue;
                        }
                    }
                    ThreadMessage::Failure(from) => {
                        // Set thread status so the Drop impl on `ThreadHandle` doesn't
                        // try to terminate it.
                        self.threads
                            .get_mut(&from)
                            .unwrap_or_else(|| {
                                panic!(
                                    "ThreadCollection received message from unknown child '{}'.",
                                    from
                                )
                            })
                            .status = Status::Failure;
                        // Return on failure
                        break Err(Error::ThreadFailure(vec![from]));
                    }
                }
            } else {
                oper.recv(&interrupt)?;
                // Dropping `self` will send termination signals to all running threads.
                break Err(Error::Interrupted);
            }
        }
    }

    /// Wait for all threads to either complete successfully or fail. The IDs of
    /// any failed threads are contained in [Error::ThreadFailure]. `Ok(())`
    /// indicates all threads have finished without error.
    pub fn wait_for_all(mut self) -> Result<(), Error> {
        self.internal_wait_for_all()
    }

    fn internal_wait_for_all(&mut self) -> Result<(), Error> {
        let completion_check = |handles: &HashMap<String, ThreadHandle<M>>| {
            if handles
                .values()
                .all(|handle| matches!(handle.status, Status::Success))
            {
                Ok(true)
            } else if handles
                .values()
                .all(|handle| matches!(handle.status, Status::Success | Status::Failure))
            {
                Err(Error::ThreadFailure(
                    handles
                        .iter()
                        .filter_map(|(id, handle)| {
                            if let Status::Failure = handle.status {
                                Some(id.clone())
                            } else {
                                None
                            }
                        })
                        .collect(),
                ))
            } else {
                Ok(false)
            }
        };
        // Run the completion check in case the messages from children threads have
        // already been processed, e.g. when the function is called from the
        // Drop impl.
        if completion_check(&self.threads)? {
            return Ok(());
        }
        for msg in self.recv_children.iter() {
            match msg {
                ThreadMessage::Success(from) => {
                    // Set thread status
                    self.threads
                        .get_mut(&from)
                        .unwrap_or_else(|| {
                            panic!(
                                "ThreadCollection received message from unknown child '{}'.",
                                from
                            )
                        })
                        .status = Status::Success;
                    // Check whether all threads are done
                    if completion_check(&self.threads)? {
                        return Ok(());
                    } else {
                        continue;
                    }
                }
                ThreadMessage::Failure(from) => {
                    // Set thread status
                    self.threads
                        .get_mut(&from)
                        .unwrap_or_else(|| {
                            panic!(
                                "ThreadCollection received message from unknown child '{}'.",
                                from
                            )
                        })
                        .status = Status::Failure;
                    // Check whether all threads are done
                    if completion_check(&self.threads)? {
                        return Ok(());
                    } else {
                        continue;
                    }
                }
            }
        }
        Ok(())
    }
}

#[allow(clippy::type_complexity)]
pub struct ThreadCollectionIter<'a, M>
where
    M: Message,
{
    inner: std::iter::Map<
        std::collections::hash_map::Keys<'a, String, ThreadHandle<M>>,
        fn(&String) -> &str,
    >,
}

impl<'a, M> std::iter::Iterator for ThreadCollectionIter<'a, M>
where
    M: Message,
{
    type Item = &'a str;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

impl<'a, M> NodeInfo<'a> for ThreadCollection<M>
where
    M: Message,
{
    type List = ThreadCollectionIter<'a, M>;

    fn contained_nodes(&'a self) -> Self::List {
        ThreadCollectionIter {
            inner: self.threads.keys().map(String::as_str),
        }
    }
}

impl<M> NodeSender<M> for ThreadCollection<M>
where
    M: Message,
{
    fn sender<S: AsRef<str>>(&self, to: S) -> Option<&ThreadSender<M>> {
        self.threads
            .get(to.as_ref())
            .map(|handle| &handle.send_child)
    }

    fn ipc_sender<S: AsRef<str>>(&self, to: S) -> Option<&IpcSender<M>> {
        self.threads
            .get(to.as_ref())
            .map(|handle| &handle.send_child_ipc)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::HoppingNode;

    // TODO: move these to topi/tests?
    // fn create_ping_nodes<'a>(
    //     progression: impl std::iter::Iterator<Item = &'a str>,
    //     replying_nodes: impl std::iter::Iterator<Item = &'a str> + Clone,
    //     last_destination: &str,
    // ) -> ThreadCollectionBuilder<TestMessage> {
    //     let mut threads = ThreadCollectionBuilder::new();
    //     let mut progression_peek = progression.peekable();
    //     loop {
    //         if let Some(current) = progression_peek.next() {
    //             if let Some(&next) = progression_peek.peek() {
    //                 threads = threads
    //                     .add_thread(
    //                         String::from(current),
    //                         PingAll::new(
    //                             replying_nodes
    //                                 .clone()
    //                                 .filter(|&id| id != current)
    //                                 .map(String::from)
    //                                 .collect(),
    //                             String::from(next),
    //                         ),
    //                     )
    //                     .unwrap();
    //             } else {
    //                 threads = threads
    //                     .add_thread(
    //                         String::from(current),
    //                         PingAll::new(
    //                             replying_nodes
    //                                 .clone()
    //                                 .filter(|&id| id != current)
    //                                 .map(String::from)
    //                                 .collect(),
    //                             String::from(last_destination),
    //                         ),
    //                     )
    //                     .unwrap();
    //             }
    //         } else {
    //             break;
    //         }
    //     }
    //     threads
    // }

    // #[test]
    // fn ping_all() {
    //     let sequence = ["1", "2", "3", "4"];
    //     let replying_nodes = ["1", "2", "3", "4", "5"];
    //     let mut threads = create_ping_nodes(
    //         sequence.iter().map(|&item| item),
    //         replying_nodes.iter().map(|&item| item),
    //         "5",
    //     );
    //     let main_node = PingAll::new(
    //         replying_nodes
    //             .iter()
    //             .map(|&item| item)
    //             .filter(|&id| id != "5")
    //             .map(String::from)
    //             .collect(),
    //         String::from("Q"),
    //     );
    //     threads = threads
    //         .add_thread(
    //             String::from("Q"),
    //             SendQuit::new(
    //                 replying_nodes
    //                     .iter()
    //                     .map(|&item| item)
    //                     .map(String::from)
    //                     .collect(),
    //             ),
    //         )
    //         .unwrap();
    //     threads
    //         .get_node_ipc_sender("1")
    //         .unwrap()
    //         .send(TestMessage::Go)
    //         .unwrap();
    //     threads
    //         .build_with_main_thread("5", main_node, None)
    //         .unwrap();
    // }

    #[test]
    fn mut_chain() {
        let (tx, final_rx) = ipc_channel().unwrap();
        let threads = ThreadCollectionBuilder::new()
            .add_ipc_destination("final", tx)
            .unwrap()
            .add_thread(
                "1st_hop",
                HoppingNode::new(0, String::from("2nd_hop"), [0; 3]),
            )
            .unwrap()
            .add_thread(
                "2nd_hop",
                HoppingNode::new(1, String::from("3rd_hop"), [1, 0, 0]),
            )
            .unwrap()
            .add_thread(
                "3rd_hop",
                HoppingNode::new(2, String::from("final"), [1, 1, 0]),
            )
            .unwrap()
            .build()
            .unwrap();
        threads
            .ipc_sender("1st_hop")
            .unwrap()
            .send([0_u8, 0, 0])
            .unwrap();
        assert_eq!(final_rx.recv().unwrap().content(), [1, 1, 1]);
        // This is necessary for all threads to properly signal their termination
        threads.wait_for_all().unwrap();
    }

    #[test]
    fn mut_chain_main_thread() {
        let (tx, final_rx) = ipc_channel().unwrap();
        let thread_builder = ThreadCollectionBuilder::new()
            .add_ipc_destination("final", tx)
            .unwrap()
            .add_thread(
                "1st_hop",
                HoppingNode::new(0, String::from("2nd_hop"), [0; 3]),
            )
            .unwrap()
            .add_thread(
                "2nd_hop",
                HoppingNode::new(1, String::from("main"), [1, 0, 0]),
            )
            .unwrap();
        thread_builder
            .ipc_sender("1st_hop")
            .unwrap()
            .send([0_u8, 0, 0])
            .unwrap();
        thread_builder
            .build_with_main_thread(
                "main",
                HoppingNode::new(2, String::from("final"), [1, 1, 0]),
                None,
            )
            .unwrap();
        assert_eq!(final_rx.recv().unwrap().content(), [1, 1, 1]);
    }
}

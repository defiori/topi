// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use crate::messaging::{
    create_messenger, ipc_channel, IpcSender, Message, Messenger, ThreadSender,
};
use crate::nodes::{Node, NodeConfig, NodeInfo, NodeSender};
use crate::thread::{ThreadCollection, ThreadCollectionBuilder};
use crate::{Error, Status};
use ipc_channel::ipc;
use iter::{
    ProcessCollectionBuilderIter, ProcessCollectionIter, ProcessCollectionWithLocalThreadsIter,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::mem::ManuallyDrop;
use std::thread;
use std::{convert, io, path, process};

pub struct ProcessCollectionBuilder<M, C>
where
    M: Message + Serialize,
    C: NodeConfig<M>,
{
    procs: HashMap<String, ProcessBuilder<M, C>>,
    ipc_destinations: HashMap<String, IpcSender<M>>,
}

#[allow(clippy::new_without_default)]
impl<M, C> ProcessCollectionBuilder<M, C>
where
    M: Message + Serialize,
    C: NodeConfig<M>,
{
    pub fn new() -> ProcessCollectionBuilder<M, C> {
        ProcessCollectionBuilder {
            procs: HashMap::new(),
            ipc_destinations: HashMap::new(),
        }
    }

    pub fn add_process<S: AsRef<str>, P: convert::AsRef<path::Path>>(
        mut self,
        id: S,
        // TODO: Iterator?
        nodes: HashMap<String, C>,
        executable_path: Option<P>,
    ) -> Result<ProcessCollectionBuilder<M, C>, Error> {
        // Test executable
        let executable_path = if let Some(path) = executable_path.as_ref() {
            path.as_ref()
        } else {
            path::Path::new("topi_proc")
        };
        let test_str = "0909";
        let executable = process::Command::new(executable_path)
            .arg("--echo")
            .arg(test_str)
            .output()?;
        if test_str.as_bytes() != executable.stdout.as_slice() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "Can`t find topi_proc executable at `{}`",
                    executable_path.display()
                ),
            )
            .into());
        }
        // Check for duplicate process ids
        if self.procs.keys().any(|k| k == id.as_ref()) {
            return Err(Error::DuplicateProcessName(String::from(id.as_ref())));
        }
        // Check for duplicate node ids
        for key in nodes.keys() {
            if self
                .procs
                .values()
                .flat_map(|b| b.nodes.keys())
                .chain(self.ipc_destinations.keys())
                .any(|thread_id| thread_id == key)
            {
                return Err(Error::DuplicateNodeName(String::from(key)));
            }
        }
        // Make sure only one node wants to run as the main thread
        let ids_requesting_main: Vec<_> = nodes
            .iter()
            .filter_map(|(id, cfg)| cfg.must_be_main_thread().then(|| id.clone()))
            .collect();
        if ids_requesting_main.len() > 1 {
            return Err(Error::MultipleNodesAsMainThread(ids_requesting_main));
        }
        // Store process specs
        self.procs.insert(
            String::from(id.as_ref()),
            ProcessBuilder {
                executable: process::Command::new(executable_path),
                nodes,
                proc: None,
            },
        );
        Ok(self)
    }

    pub fn add_ipc_destination<S: AsRef<str>>(
        mut self,
        id: S,
        sender: IpcSender<M>,
    ) -> Result<ProcessCollectionBuilder<M, C>, Error> {
        // Check for duplicate node ids
        if self
            .procs
            .values()
            .flat_map(|b| b.nodes.keys())
            .any(|thread_id| thread_id == id.as_ref())
        {
            Err(Error::DuplicateNodeName(String::from(id.as_ref())))
        } else {
            self.ipc_destinations
                .insert(String::from(id.as_ref()), sender);
            Ok(self)
        }
    }

    /// Start initialization of all processes and return senders to all
    /// contained nodes.
    #[allow(clippy::type_complexity)]
    fn start_processes(
        mut self,
    ) -> Result<
        (
            ProcessCollectionBuilder<M, C>,
            HashMap<String, IpcSender<M>>,
        ),
        Error,
    > {
        let mut remote_destinations = HashMap::new();
        for (_id, proc) in self.procs.iter_mut() {
            let nodes = proc.start_process()?;
            remote_destinations.extend(nodes.into_iter());
        }
        remote_destinations.extend(self.ipc_destinations.drain());
        Ok((self, remote_destinations))
    }

    /// Finish process initialization by passing in senders to all
    /// out-of-process destinations.
    fn build_processes(
        self,
        remote_destinations: &HashMap<String, IpcSender<M>>,
    ) -> Result<HashMap<String, ProcessHandle<M, C>>, Error> {
        // Only pass out-of-process destinations to each process
        let mut handles = HashMap::new();
        for (proc_id, proc) in self.procs.into_iter() {
            let selected_destinations: HashMap<_, _> = remote_destinations
                .iter()
                .filter(|&(id, _)| !proc.nodes.keys().any(|k| k == id))
                .map(|(id, p)| (id.clone(), p.clone()))
                .collect();
            let handle = proc.build(selected_destinations)?;
            handles.insert(proc_id, handle);
        }
        Ok(handles)
    }

    pub fn build(self) -> Result<ProcessCollection<M, C>, Error> {
        // TODO: build checks, e.g. don't build without any (non-empty) processes added
        let (new_self, remote_destinations) = self.start_processes()?;
        let handles = new_self.build_processes(&remote_destinations)?;
        Ok(ProcessCollection { procs: handles })
    }

    pub fn build_with_messenger<S: AsRef<str>, MR: Messenger<M>>(
        self,
        id: S,
    ) -> Result<(ProcessCollection<M, C>, MR), Error> {
        let (new_self, mut remote_destinations) = self.start_processes()?;
        let (messenger, _, send_messenger_ipc) = create_messenger(
            id.as_ref(),
            new_self.contained_nodes(),
            std::iter::empty::<(String, _)>(),
            remote_destinations.iter(),
            None,
            None,
        )?;
        remote_destinations.insert(String::from(id.as_ref()), send_messenger_ipc);
        let handles = new_self.build_processes(&remote_destinations)?;
        Ok((ProcessCollection { procs: handles }, messenger))
    }

    /// Check for duplicates in node ids across `proc_collection` and `threads`.
    fn check_local_thread_duplicates(
        proc_collection: &ProcessCollectionBuilder<M, C>,
        threads: &ThreadCollectionBuilder<M>,
    ) -> Result<(), Error> {
        for thread_id in threads.contained_nodes() {
            if proc_collection
                .procs
                .values()
                .flat_map(|b| b.nodes.keys())
                .chain(proc_collection.ipc_destinations.keys())
                .any(|proc_id| proc_id == thread_id)
            {
                return Err(Error::DuplicateNodeName(String::from(thread_id)));
            }
        }
        Ok(())
    }

    /// Add `remote_destinations` to `threads`, then extend
    /// `remote_destinations` with ipc senders to the nodes in `threads`.
    fn manage_local_thread_destinations(
        mut threads: ThreadCollectionBuilder<M>,
        remote_destinations: &mut HashMap<String, IpcSender<M>>,
    ) -> Result<ThreadCollectionBuilder<M>, Error> {
        // Add out-of-process destinations to local threads
        for (id, send_remote) in remote_destinations.iter() {
            threads = threads.add_ipc_destination(id, send_remote.clone())?;
        }
        // Retrieve ipc senders to local threads, add to remote destinations
        for local_id in threads.contained_nodes() {
            remote_destinations.insert(
                String::from(local_id),
                threads.ipc_sender(local_id).unwrap().clone(),
            );
        }
        Ok(threads)
    }

    pub fn build_with_local_threads(
        self,
        threads: ThreadCollectionBuilder<M>,
    ) -> Result<ProcessCollectionWithLocalThreads<M, C>, Error> {
        Self::check_local_thread_duplicates(&self, &threads)?;
        let (new_self, mut remote_destinations) = self.start_processes()?;
        let threads = Self::manage_local_thread_destinations(threads, &mut remote_destinations)?;
        let handles = new_self.build_processes(&remote_destinations)?;
        Ok(ProcessCollectionWithLocalThreads {
            threads: threads.build()?,
            procs: handles,
        })
    }

    pub fn build_with_local_threads_and_messenger<S: AsRef<str>, MR: Messenger<M>>(
        self,
        threads: ThreadCollectionBuilder<M>,
        id: S,
    ) -> Result<(ProcessCollectionWithLocalThreads<M, C>, MR), Error> {
        Self::check_local_thread_duplicates(&self, &threads)?;
        // The order here is important
        // Collect remote destinations
        let (new_self, mut remote_destinations) = self.start_processes()?;
        // Deal with senders between local and remote threads
        let threads = Self::manage_local_thread_destinations(threads, &mut remote_destinations)?;
        // Get the messenger, this should include ipc senders to the out-of-process
        // nodes
        let (threads, mut messenger): (_, MR) = threads.build_with_messenger(&id)?;
        // The other processes don't yet know about the messenger, add its ipc sender to
        // the remote destinations
        remote_destinations.insert(
            String::from(id.as_ref()),
            messenger.channels().send_self_ipc.clone(),
        );
        // Finish process initialization
        let handles = new_self.build_processes(&remote_destinations)?;
        Ok((
            ProcessCollectionWithLocalThreads {
                threads,
                procs: handles,
            },
            messenger,
        ))
    }

    pub fn build_with_local_main_thread<S, N, MR>(
        self,
        threads: ThreadCollectionBuilder<M>,
        thread_id: S,
        node: N,
    ) -> Result<(), Error>
    where
        S: AsRef<str>,
        N: Node<M, MR>,
        MR: Messenger<M>,
    {
        Self::check_local_thread_duplicates(&self, &threads)?;
        let (new_self, mut remote_destinations) = self.start_processes()?;
        let threads = Self::manage_local_thread_destinations(threads, &mut remote_destinations)?;
        // Add main thread ipc_sender to remote destinations
        let (send_main_ipc, recv_peers_ipc) = ipc_channel()?;
        remote_destinations.insert(String::from(thread_id.as_ref()), send_main_ipc.clone());
        let handles = new_self.build_processes(&remote_destinations)?;
        let procs = ProcessCollection { procs: handles };
        threads.build_with_main_thread(thread_id, node, Some((send_main_ipc, recv_peers_ipc)))?;
        procs.wait_for_all()
    }
}

impl<'a, M, C> NodeInfo<'a> for ProcessCollectionBuilder<M, C>
where
    M: Message + Serialize,
    C: 'a + NodeConfig<M>,
{
    type List = ProcessCollectionBuilderIter<'a, M, C>;

    #[allow(clippy::type_complexity)]
    fn contained_nodes(&'a self) -> Self::List {
        // ? This doesn't coerce correctly in the return
        let map_fn: fn(
            &ProcessBuilder<M, C>,
        ) -> std::iter::Map<
            std::collections::hash_map::Keys<String, C>,
            fn(&String) -> &str,
        > = |builder| builder.nodes.keys().map(String::as_str);
        ProcessCollectionBuilderIter {
            inner: self.procs.values().map(map_fn).flatten(),
        }
    }
}

#[allow(clippy::type_complexity)]
fn wait_for_all<M, C>(handles: HashMap<String, ProcessHandle<M, C>>) -> Result<(), Error>
where
    M: Message + Serialize,
    C: NodeConfig<M>,
{
    let mut receiver_set = ipc::IpcReceiverSet::new().unwrap();
    let mut id_map = HashMap::new();
    let mut status_map = HashMap::new();
    for (id, mut handle) in handles.into_iter() {
        // Clone the `send_child` handle so it doesn't get dropped.
        status_map.insert(
            id.clone(),
            (handle.status, None, false, handle.send_child.clone()),
        );
        // Since the IpcReceiverSet takes ownership of recv_child we need to move
        // it out of the ProcessHandle. Since ProcessHandle implements Drop this
        // requires some ManuallyDrop faff.
        // DO NOT USE THE HANDLE AFTER THIS
        let recv_child = unsafe { ManuallyDrop::take(&mut handle.recv_child) };
        handle.recv_child_removed = true;
        id_map.insert(receiver_set.add(recv_child).unwrap(), id);
    }
    // Returns an error if all processes have finished but some with a failed state.
    let completion_check =
        |status_map: &HashMap<String, (Status, Option<Vec<String>>, bool, ipc::IpcSender<_>)>| {
            if status_map
                .values()
                .all(|(status, _, _, _)| matches!(status, Status::Success))
            {
                Ok(true)
            } else if status_map
                .values()
                .all(|(status, _, _, _)| matches!(status, Status::Success | Status::Failure))
            {
                Err(Error::ProcessFailure(
                    status_map
                        .iter()
                        .filter_map(|(id, (status, failed_ids, _, _))| {
                            if let Status::Failure = status {
                                Some((id.clone(), failed_ids.clone()))
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
    let mut result = None;
    loop {
        for event in receiver_set.select().unwrap() {
            match event {
                ipc::IpcSelectionResult::MessageReceived(msg_id, msg) => {
                    match msg.to::<ProcessMessage<M, C>>().unwrap() {
                        ProcessMessage::Success => {
                            let mut entry =
                                status_map.get_mut(id_map.get(&msg_id).unwrap()).unwrap();
                            assert_eq!(entry.2, false, "Received process Success message after the process signalled completion.");
                            entry.0 = Status::Success;
                            // Check whether all processes are done
                            match completion_check(&status_map) {
                                Ok(true) => {
                                    result = Some(Ok(()));
                                }
                                Ok(false) => continue,
                                Err(err) => {
                                    result = Some(Err(err));
                                }
                            }
                        }
                        ProcessMessage::Failure(failed_ids) => {
                            let mut entry =
                                status_map.get_mut(id_map.get(&msg_id).unwrap()).unwrap();
                            assert_eq!(entry.2, false, "Received process Failure message after the process signalled completion.");
                            entry.0 = Status::Failure;
                            entry.1 = Some(failed_ids);
                            // Check whether all processes are done
                            match completion_check(&status_map) {
                                Ok(true) => {
                                    result = Some(Ok(()));
                                }
                                Ok(false) => continue,
                                Err(err) => {
                                    result = Some(Err(err));
                                }
                            }
                        }
                        ProcessMessage::ConfirmTermination => {
                            status_map.get_mut(id_map.get(&msg_id).unwrap()).unwrap().2 = true;
                            // Check if all processes have confirmed their termination.
                            if status_map.values().all(|&(_, _, conf, _)| conf) {
                                return result.expect(
                                    "Received ConfirmTermination signal before status message.",
                                );
                            }
                        }
                        _ => panic!(
                            "Received unexpected ProcessMessage while waiting on completion."
                        ),
                    }
                }
                ipc::IpcSelectionResult::ChannelClosed(msg_id) => {
                    let entry = status_map.get_mut(id_map.get(&msg_id).unwrap()).unwrap();
                    if let (Status::Running, _, _, _) = entry {
                        entry.0 = Status::Failure;
                        entry.1 = None;
                        entry.2 = true;
                    }
                    // Check whether all processes are done
                    match completion_check(&status_map) {
                        Ok(true) => {
                            result = Some(Ok(()));
                        }
                        Ok(false) => continue,
                        Err(err) => {
                            result = Some(Err(err));
                        }
                    }
                }
            }
        }
    }
}

pub struct ProcessCollection<M, C>
where
    M: Message + Serialize,
    C: NodeConfig<M>,
{
    procs: HashMap<String, ProcessHandle<M, C>>,
}

impl<M, C> ProcessCollection<M, C>
where
    M: Message + Serialize,
    C: NodeConfig<M>,
{
    pub fn wait_for_all(self) -> Result<(), Error> {
        wait_for_all(self.procs)
    }

    pub fn force_quit(self) {
        todo!()
    }
}

impl<'a, M, C> NodeInfo<'a> for ProcessCollection<M, C>
where
    M: Message + Serialize,
    C: 'a + NodeConfig<M>,
{
    type List = ProcessCollectionIter<'a, M, C>;

    #[allow(clippy::type_complexity)]
    fn contained_nodes(&'a self) -> Self::List {
        // ? This doesn't coerce correctly in the return
        let map_fn: fn(
            &ProcessHandle<M, C>,
        ) -> std::iter::Map<
            std::collections::hash_map::Keys<String, IpcSender<M>>,
            fn(&String) -> &str,
        > = |handle| handle.send_nodes.keys().map(String::as_str);
        ProcessCollectionIter {
            inner: self.procs.values().map(map_fn).flatten(),
        }
    }
}

impl<M, C> NodeSender<M> for ProcessCollection<M, C>
where
    M: Message + Serialize,
    C: NodeConfig<M>,
{
    #[allow(unused_variables)]
    fn sender<S: AsRef<str>>(&self, to: S) -> Option<&ThreadSender<M>> {
        None
    }

    fn ipc_sender<S: AsRef<str>>(&self, to: S) -> Option<&IpcSender<M>> {
        self.procs
            .values()
            .flat_map(|handle| handle.send_nodes.iter())
            .find_map(|(id_iter, sender)| {
                if id_iter.as_str() == to.as_ref() {
                    Some(sender)
                } else {
                    None
                }
            })
    }
}

pub struct ProcessCollectionWithLocalThreads<M, C>
where
    M: Message + Serialize,
    C: NodeConfig<M>,
{
    threads: ThreadCollection<M>,
    procs: HashMap<String, ProcessHandle<M, C>>,
}

impl<M, C> ProcessCollectionWithLocalThreads<M, C>
where
    M: Message + Serialize,
    C: NodeConfig<M>,
{
    pub fn wait_for_all(self) -> Result<(), Error> {
        self.threads.wait_for_all()?;
        wait_for_all(self.procs)
    }

    pub fn force_quit(self) {
        todo!()
    }
}

impl<'a, M, C> NodeInfo<'a> for ProcessCollectionWithLocalThreads<M, C>
where
    M: Message + Serialize,
    C: 'a + NodeConfig<M>,
{
    type List = ProcessCollectionWithLocalThreadsIter<'a, M, C>;

    #[allow(clippy::type_complexity)]
    fn contained_nodes(&'a self) -> Self::List {
        // ? This doesn't coerce correctly in the return
        let map_fn: fn(
            &ProcessHandle<M, C>,
        ) -> std::iter::Map<
            std::collections::hash_map::Keys<String, IpcSender<M>>,
            fn(&String) -> &str,
        > = |handle| handle.send_nodes.keys().map(String::as_str);
        ProcessCollectionWithLocalThreadsIter {
            inner: self.procs.values().map(map_fn).flatten(),
        }
    }
}

impl<M, C> NodeSender<M> for ProcessCollectionWithLocalThreads<M, C>
where
    M: Message + Serialize,
    C: NodeConfig<M>,
{
    fn sender<S: AsRef<str>>(&self, to: S) -> Option<&ThreadSender<M>> {
        self.threads.sender(to)
    }

    fn ipc_sender<S: AsRef<str>>(&self, to: S) -> Option<&IpcSender<M>> {
        self.procs
            .values()
            .flat_map(|handle| handle.send_nodes.iter())
            .find_map(|(id_iter, sender)| {
                if id_iter.as_str() == to.as_ref() {
                    Some(sender)
                } else {
                    None
                }
            })
    }
}

struct ProcessBuilder<M, C>
where
    M: Message + Serialize,
    C: NodeConfig<M>,
{
    executable: process::Command,
    nodes: HashMap<String, C>,
    proc: Option<ProcessHandle<M, C>>,
}

impl<M, C> ProcessBuilder<M, C>
where
    M: Message + Serialize,
    C: NodeConfig<M>,
{
    fn start_process(&mut self) -> Result<HashMap<String, IpcSender<M>>, Error> {
        // Bootstrap ipc server and spawn process
        let (server, server_name) = ipc::IpcOneShotServer::new()?;
        let proc = self.executable.arg("--server").arg(server_name).spawn()?;
        let (recv_child, msg) = server.accept()?;
        let send_child = if let ProcessMessage::Init(sender) = msg {
            Ok(sender)
        } else {
            Err(Error::TopiProcUnexpectedInitMessage)
        }?;
        // Start communication
        send_child.send(ProcessMessage::Nodes(self.nodes.clone()))?;

        match recv_child.recv()? {
            ProcessMessage::NodeSenders(senders) => {
                self.proc = Some(ProcessHandle {
                    proc,
                    send_nodes: senders.clone(),
                    send_child,
                    recv_child_removed: false,
                    recv_child: ManuallyDrop::new(recv_child),
                    status: Status::Running,
                });
                Ok(senders)
            }
            ProcessMessage::UnusableNodeConfig(cfg) => Err(Error::UnusableNodeConfig(cfg)),
            ProcessMessage::InitError(err_string) => Err(Error::TopiProcInitFailure(err_string)),
            _ => Err(Error::TopiProcUnexpectedInitMessage),
        }
    }

    fn build(
        self,
        remote_destinations: HashMap<String, IpcSender<M>>,
    ) -> Result<ProcessHandle<M, C>, Error> {
        let proc = self.proc.expect("Start process before calling build.");
        proc.send_child
            .send(ProcessMessage::NodeSenders(remote_destinations))?;
        if let ProcessMessage::Running = proc.recv_child.recv()? {
            Ok(proc)
        } else {
            Err(Error::TopiProcUnexpectedInitMessage)
        }
    }
}

#[allow(dead_code)]
struct ProcessHandle<M, C>
where
    M: Message + Serialize,
    C: NodeConfig<M>,
{
    proc: process::Child,
    send_nodes: HashMap<String, IpcSender<M>>,
    send_child: ipc::IpcSender<ProcessMessage<M, C>>,
    recv_child_removed: bool,
    // See comment in `wait_for_all` on why we need the ManuallyDrop wrapper.
    recv_child: ManuallyDrop<ipc::IpcReceiver<ProcessMessage<M, C>>>,
    status: Status,
}

impl<M, C> Drop for ProcessHandle<M, C>
where
    M: Message + Serialize,
    C: NodeConfig<M>,
{
    fn drop(&mut self) {
        use ProcessMessage::*;
        if !self.recv_child_removed {
            if self.send_child.send(ProcessMessage::Terminate).is_ok() {
                if let Ok(msg) = self.recv_child.recv() {
                    match msg {
                        // Success/Failure: process already done, Interrupted: process
                        // received Terminate signal
                        Success | Failure(_) | Interrupted => (),
                        _ => {
                            panic!("Received unexpected ProcessMessage during process termination.")
                        }
                    }
                }
                if let Ok(msg) = self.recv_child.recv() {
                    match msg {
                        ConfirmTermination => (),
                        _ => {
                            panic!("Received unexpected ProcessMessage during process termination.")
                        }
                    }
                }
            } else {
                // The receiver on the other end has dropped.
                // Can happen if the process has not shut down
                // cleanly (e.g. killed by the OS).
            }
            unsafe {
                ManuallyDrop::drop(&mut self.recv_child);
            }
        }
    }
}

#[derive(Serialize, Deserialize)]
pub enum ProcessMessage<M, C> {
    Init(ipc::IpcSender<ProcessMessage<M, C>>),
    Nodes(HashMap<String, C>),
    NodeSenders(HashMap<String, IpcSender<M>>),
    UnusableNodeConfig(String),
    InitError(String),
    Running,
    Success,
    Failure(Vec<String>),
    Terminate,
    Interrupted,
    ConfirmTermination,
}

pub fn run_topi_proc<M, C>(server_name: String) -> Result<(), Error>
where
    M: Message + Serialize,
    C: NodeConfig<M>,
{
    let send_parent = ipc::IpcSender::connect(server_name.clone())?;
    let (send_self, recv_parent) = ipc::channel()?;
    send_parent.send(ProcessMessage::<M, C>::Init(send_self))?;
    if let Err(err) = run_topi_proc_internal(server_name, &send_parent, recv_parent) {
        match err {
            Error::UnusableNodeConfig(cfg) => {
                send_parent.send(ProcessMessage::UnusableNodeConfig(cfg))?
            }
            _ => send_parent.send(ProcessMessage::InitError(format!("{:?}", err)))?,
        }
    };
    send_parent.send(ProcessMessage::ConfirmTermination)?;
    Ok(())
}

fn run_topi_proc_internal<M, C>(
    server_name: String,
    send_parent: &ipc::IpcSender<ProcessMessage<M, C>>,
    recv_parent: ipc::IpcReceiver<ProcessMessage<M, C>>,
) -> Result<(), Error>
where
    M: Message + Serialize,
    C: NodeConfig<M>,
{
    if let ProcessMessage::Nodes(mut nodes) = recv_parent.recv()? {
        let mut threads = ThreadCollectionBuilder::new();
        let mut send_children = HashMap::new();
        // Check if any node must be run in the main thread.
        let main_thread = if let Some(id) = nodes
            .iter()
            .find_map(|(id, cfg)| cfg.must_be_main_thread().then(|| id.clone()))
        {
            let (send_main, recv_peers) = ipc_channel()?;
            send_children.insert(id.clone(), send_main.clone());
            Some((
                id.clone(),
                nodes.remove(&id).unwrap(),
                send_main,
                recv_peers,
            ))
        } else {
            None
        };
        // Deal with the other nodes.
        for (id, node_cfg) in nodes {
            threads = node_cfg
                .add_node(&id, threads)
                .map_err(|_| Error::UnusableNodeConfig(id.clone()))?;
            send_children.insert(String::from(&id), threads.ipc_sender(&id).unwrap().clone());
        }
        send_parent.send(ProcessMessage::NodeSenders(send_children))?;
        // Get the ipc senders to nodes in other processes and add them to the thread
        // builder.
        if let ProcessMessage::NodeSenders(remote_destinations) = recv_parent.recv()? {
            for (id, send_remote) in remote_destinations {
                threads = threads.add_ipc_destination(id, send_remote)?;
            }
        } else {
            return Err(Error::TopiProcUnexpectedInitMessage);
        };
        let controller = {
            let send_parent = send_parent.clone();
            move |threads: ThreadCollection<M>| -> Result<(), Error> {
                // Set up router to convert ipc Terminate message into a thread interrupt
                // signal.
                let (send_interrupt, recv_router) = crossbeam_channel::bounded(1);
                thread::Builder::new()
                    .name(format!("{}_router", server_name))
                    .spawn(move || {
                        // The `recv()` call fails if the sender drops without having sent a
                        // Terminate signal. Ignore.
                        if let Ok(msg) = recv_parent.recv() {
                            if let ProcessMessage::Terminate = msg {
                                // This fails if we receive a Terminate signal after `recv_router`
                                // has dropped (After `wait_for_all_or_failure_with_interrupt` has
                                // returned.) Ignore that case.
                                send_interrupt.send(()).ok();
                            } else {
                                panic!("topi_proc converting router received unexpected message.")
                            }
                        }
                    })?;
                // Wait on either all threads to complete successfully or one of them to fail.
                if let Err(error) = threads.wait_for_all_or_failure_with_interrupt(recv_router) {
                    match error {
                    Error::ThreadFailure(failed_ids) => {
                        send_parent.send(ProcessMessage::Failure(failed_ids)).map_err(|e| e.into())
                    }
                    Error::Interrupted => {
                        send_parent.send(ProcessMessage::Interrupted).map_err(|e| e.into())
                    }
                    _ => panic!(
                        "topi_proc received unexpected error while waiting on thread completion: {:?}",
                        error
                    ),
                }
                } else {
                    send_parent
                        .send(ProcessMessage::Success)
                        .map_err(|e| e.into())
                }
                // The router thread should be terminated automatically once the
                // process returns.
            }
        };

        if let Some((main_id, main_cfg, send_main, recv_peers)) = main_thread {
            send_parent.send(ProcessMessage::Running)?;
            main_cfg.add_node_as_main_with_controller(
                main_id,
                threads,
                Some((send_main, recv_peers)),
                controller,
            )
        } else {
            // Build the ThreadCollection.
            let threads = threads.build()?;
            send_parent.send(ProcessMessage::Running)?;
            controller(threads)
        }
    } else {
        Err(Error::TopiProcUnexpectedInitMessage)
    }
}

pub mod iter {
    use super::*;

    #[allow(clippy::type_complexity)]
    pub struct ProcessCollectionBuilderIter<'a, M, C>
    where
        M: Message + Serialize,
        C: NodeConfig<M>,
    {
        pub(super) inner: std::iter::Flatten<
            std::iter::Map<
                std::collections::hash_map::Values<'a, std::string::String, ProcessBuilder<M, C>>,
                fn(
                    &ProcessBuilder<M, C>,
                ) -> std::iter::Map<
                    std::collections::hash_map::Keys<String, C>,
                    fn(&String) -> &str,
                >,
            >,
        >,
    }

    impl<'a, M, C> std::iter::Iterator for ProcessCollectionBuilderIter<'a, M, C>
    where
        M: Message + Serialize,
        C: NodeConfig<M>,
    {
        type Item = &'a str;
        fn next(&mut self) -> Option<Self::Item> {
            self.inner.next()
        }
    }

    #[allow(clippy::type_complexity)]
    pub struct ProcessCollectionIter<'a, M, C>
    where
        M: Message + Serialize,
        C: NodeConfig<M>,
    {
        pub(super) inner: std::iter::Flatten<
            std::iter::Map<
                std::collections::hash_map::Values<'a, std::string::String, ProcessHandle<M, C>>,
                fn(
                    &ProcessHandle<M, C>,
                ) -> std::iter::Map<
                    std::collections::hash_map::Keys<String, IpcSender<M>>,
                    fn(&String) -> &str,
                >,
            >,
        >,
    }

    impl<'a, M, C> std::iter::Iterator for ProcessCollectionIter<'a, M, C>
    where
        M: Message + Serialize,
        C: NodeConfig<M>,
    {
        type Item = &'a str;
        fn next(&mut self) -> Option<Self::Item> {
            self.inner.next()
        }
    }

    #[allow(clippy::type_complexity)]
    pub struct ProcessCollectionWithLocalThreadsIter<'a, M, C>
    where
        M: Message + Serialize,
        C: NodeConfig<M>,
    {
        pub(super) inner: std::iter::Flatten<
            std::iter::Map<
                std::collections::hash_map::Values<'a, std::string::String, ProcessHandle<M, C>>,
                fn(
                    &ProcessHandle<M, C>,
                ) -> std::iter::Map<
                    std::collections::hash_map::Keys<String, IpcSender<M>>,
                    fn(&String) -> &str,
                >,
            >,
        >,
    }

    impl<'a, M, C> std::iter::Iterator for ProcessCollectionWithLocalThreadsIter<'a, M, C>
    where
        M: Message + Serialize,
        C: NodeConfig<M>,
    {
        type Item = &'a str;
        fn next(&mut self) -> Option<Self::Item> {
            self.inner.next()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::HoppingConfig;
    use std::thread::spawn;

    fn test_node_cfgs() -> HashMap<String, HoppingConfig> {
        let mut node_cfgs = HashMap::new();
        node_cfgs.insert(
            String::from("1st_hop"),
            HoppingConfig::new(0, String::from("2nd_hop"), [0; 3]),
        );
        node_cfgs.insert(
            String::from("2nd_hop"),
            HoppingConfig::new(1, String::from("3rd_hop"), [1, 0, 0]),
        );
        node_cfgs.insert(
            String::from("3rd_hop"),
            HoppingConfig::new(2, String::from("final"), [1, 1, 0]),
        );
        node_cfgs
    }

    #[test]
    fn topi_proc_mut_chain() {
        let (server, server_name) = ipc::IpcOneShotServer::new().unwrap();
        let handle = spawn(|| {
            run_topi_proc::<_, HoppingConfig>(server_name).unwrap();
        });
        let (recv_child, msg) = server.accept().unwrap();
        let send_child = if let ProcessMessage::Init(sender) = msg {
            sender
        } else {
            panic!()
        };
        send_child
            .send(ProcessMessage::Nodes(test_node_cfgs()))
            .unwrap();
        let mut senders = if let ProcessMessage::NodeSenders(senders) = recv_child.recv().unwrap() {
            senders
        } else {
            panic!()
        };
        let (send_peer, recv_peer) = ipc_channel().unwrap();
        send_child
            .send(ProcessMessage::NodeSenders(
                [(String::from("final"), send_peer)]
                    .iter()
                    .cloned()
                    .collect(),
            ))
            .unwrap();
        if let ProcessMessage::Running = recv_child.recv().unwrap() {
        } else {
            panic!()
        };
        senders.get_mut("1st_hop").unwrap().send([0_u8; 3]).unwrap();
        assert_eq!(recv_peer.recv().unwrap().content(), [1_u8, 1, 1]);
        handle.join().unwrap();
    }
}

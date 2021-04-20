// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use crate::messaging::{IpcReceiver, IpcSender, Message, Messenger, ThreadSender};
use crate::thread::{ThreadCollection, ThreadCollectionBuilder};
use crate::Error;
use serde::{de::DeserializeOwned, Serialize};
use std::convert::AsRef;
use std::marker::Send;
use std::panic::UnwindSafe;

pub trait Node<M, MR>: Send + UnwindSafe + 'static
where
    M: Message + Serialize,
    MR: Messenger<M>,
{
    fn entrypoint(&mut self, messenger: &mut MR) -> Result<(), String>;
}

pub trait NodeConfig<M>: Clone + Serialize + Send + DeserializeOwned + 'static
where
    M: Message + Serialize,
{
    fn must_be_main_thread(&self) -> bool;

    fn add_node<S: AsRef<str>>(
        self,
        id: S,
        thread_builder: ThreadCollectionBuilder<M>,
    ) -> Result<ThreadCollectionBuilder<M>, Error>;

    fn add_node_as_main_with_controller<S, F>(
        self,
        id: S,
        thread_builder: ThreadCollectionBuilder<M>,
        ipc_pair: Option<(IpcSender<M>, IpcReceiver<M>)>,
        controller: F,
    ) -> Result<(), Error>
    where
        S: AsRef<str>,
        F: FnOnce(ThreadCollection<M>) -> Result<(), Error> + Send + 'static;
}

pub trait NodeInfo<'a> {
    type List: std::iter::Iterator<Item = &'a str>;
    fn contained_nodes(&'a self) -> Self::List;
}

pub trait NodeSender<M> {
    fn sender<S: AsRef<str>>(&self, to: S) -> Option<&ThreadSender<M>>;
    fn ipc_sender<S: AsRef<str>>(&self, to: S) -> Option<&IpcSender<M>>;
}

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

pub mod config_parse;
pub mod test_utils;

use config_parse::{GlobalConfig, ParsedConfig};
use serde::{Deserialize, Serialize};
use std::convert::{From, TryFrom, TryInto};
use std::iter::Iterator;
use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
};
pub use topi_base;
use topi_base::{
    data::c_api::{self, Message},
    messaging::{IpcReceiver, IpcSender, Messenger},
    nodes::NodeConfig,
    process::{ProcessCollection, ProcessCollectionBuilder, ProcessCollectionWithLocalThreads},
    thread::{ThreadCollection, ThreadCollectionBuilder},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub enum Config {
    Python {
        must_be_main_thread: bool,
        source: String,
    },
    C {
        must_be_main_thread: bool,
        lib_path: std::path::PathBuf,
    },
}

impl NodeConfig<Message> for Config {
    fn must_be_main_thread(&self) -> bool {
        match *self {
            Config::Python {
                must_be_main_thread,
                ..
            } => must_be_main_thread,
            Config::C {
                must_be_main_thread,
                ..
            } => must_be_main_thread,
        }
    }

    fn add_node<S: AsRef<str>>(
        self,
        id: S,
        thread_builder: ThreadCollectionBuilder<Message>,
    ) -> Result<ThreadCollectionBuilder<Message>, topi_base::Error> {
        match self {
            Config::Python { ref source, .. } => {
                config_into_node::python_add_node(source, id, thread_builder)
            }
            Config::C { .. } => todo!(),
        }
    }

    fn add_node_as_main_with_controller<S, F>(
        self,
        id: S,
        thread_builder: ThreadCollectionBuilder<Message>,
        ipc_pair: Option<(IpcSender<Message>, IpcReceiver<Message>)>,
        controller: F,
    ) -> Result<(), topi_base::Error>
    where
        S: AsRef<str>,
        F: FnOnce(ThreadCollection<Message>) -> Result<(), topi_base::Error> + Send + 'static,
    {
        match self {
            Config::Python { ref source, .. } => {
                config_into_node::python_add_node_as_main_with_controller(
                    source,
                    id,
                    thread_builder,
                    ipc_pair,
                    controller,
                )
            }
            Config::C { .. } => todo!(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcessId(i64);

impl<'a, T: Ids> From<&'a T> for ProcessId {
    fn from(parsed: &'a T) -> Self {
        parsed.process_id()
    }
}

impl From<i64> for ProcessId {
    fn from(num: i64) -> Self {
        ProcessId(num)
    }
}

pub trait Ids {
    fn node_id(&self) -> &str;
    fn process_id(&self) -> ProcessId;
}

impl<'a, P: Into<ProcessId> + Copy, N: AsRef<str>> Ids for (P, N) {
    fn process_id(&self) -> ProcessId {
        self.0.into()
    }

    fn node_id(&self) -> &str {
        self.1.as_ref()
    }
}

pub struct TopiNodeConfig {
    config: Config,
    node_id: String,
    process_id: ProcessId,
}

impl TopiNodeConfig {
    pub fn from_config<I: Ids>(ids: I, config: Config) -> Self {
        let node_id = String::from(ids.node_id());
        let process_id = ids.process_id();
        TopiNodeConfig {
            node_id,
            process_id,
            config,
        }
    }

    pub fn from_parsed_config<T>(config: T) -> Result<Self, topi_base::Error>
    where
        T: TryInto<Config, Error = topi_base::Error> + Ids,
    {
        let node_id = String::from(config.node_id());
        let process_id = config.process_id();
        Ok(TopiNodeConfig {
            node_id,
            process_id,
            config: config.try_into()?,
        })
    }
}

impl TryFrom<TopiNodeConfig> for Config {
    type Error = topi_base::Error;
    fn try_from(node_config: TopiNodeConfig) -> Result<Self, Self::Error> {
        Ok(node_config.config)
    }
}

impl Ids for TopiNodeConfig {
    fn node_id(&self) -> &str {
        self.node_id.as_str()
    }

    fn process_id(&self) -> ProcessId {
        self.process_id
    }
}

pub struct TopiNetworkWithMessenger<MR> {
    network: Network,
    messenger: MR,
}

impl<MR> TopiNetworkWithMessenger<MR>
where
    MR: Messenger<Message>,
{
    pub fn from_parsed_config<S: AsRef<str>>(
        config: ParsedConfig,
        messenger_id: S,
    ) -> Result<Self, Error> {
        let (network, messenger) = from_parsed_config(config, Some(messenger_id.as_ref()))?;
        Ok(Self {
            network,
            messenger: messenger.unwrap(),
        })
    }

    pub fn from_node_config<I, T, S>(
        global: GlobalConfig,
        node_config: I,
        messenger_id: S,
    ) -> Result<Self, Error>
    where
        I: Iterator<Item = Result<T, topi_base::Error>>,
        T: TryInto<Config, Error = topi_base::Error> + Ids,
        S: AsRef<str>,
    {
        let (network, messenger) =
            from_node_config(global, node_config, Some(messenger_id.as_ref()))?;
        Ok(Self {
            network,
            messenger: messenger.unwrap(),
        })
    }

    pub fn wait_for_all(self) -> Result<(), Error> {
        use Network::*;
        match self.network {
            ThreadCollection(threads) => threads.wait_for_all().map_err(Into::into),
            ProcessCollection(procs) => procs.wait_for_all().map_err(Into::into),
            ProcessCollectionWithLocalThreads(procs) => procs.wait_for_all().map_err(Into::into),
        }
    }

    pub fn messenger(&mut self) -> &mut MR {
        &mut self.messenger
    }
}

pub struct TopiNetwork {
    network: Network,
}

impl TopiNetwork {
    pub fn from_parsed_config(config: ParsedConfig) -> Result<Self, Error> {
        let (network, _) = from_parsed_config::<c_api::Messenger>(config, None)?;
        Ok(Self { network })
    }

    pub fn from_node_config<I, T>(global: GlobalConfig, node_config: I) -> Result<Self, Error>
    where
        I: Iterator<Item = Result<T, topi_base::Error>>,
        T: TryInto<Config, Error = topi_base::Error> + Ids,
    {
        let (network, _) = from_node_config::<_, _, c_api::Messenger>(global, node_config, None)?;
        Ok(Self { network })
    }

    pub fn wait_for_all(self) -> Result<(), Error> {
        use Network::*;
        match self.network {
            ThreadCollection(threads) => threads.wait_for_all().map_err(Into::into),
            ProcessCollection(procs) => procs.wait_for_all().map_err(Into::into),
            ProcessCollectionWithLocalThreads(procs) => procs.wait_for_all().map_err(Into::into),
        }
    }
}

fn from_parsed_config<MR>(
    mut config: ParsedConfig,
    messenger_id: Option<&str>,
) -> Result<(Network, Option<MR>), Error>
where
    MR: Messenger<Message>,
{
    let iter = config
        .python
        .iter_mut()
        .flat_map(|v| v.drain(..).map(TopiNodeConfig::from_parsed_config))
        .chain(
            config
                .c
                .iter_mut()
                .flat_map(|v| v.drain(..).map(TopiNodeConfig::from_parsed_config)),
        );
    from_node_config(config.global, iter, messenger_id)
}

fn from_node_config<I, T, MR>(
    global: GlobalConfig,
    node_config: I,
    messenger_id: Option<&str>,
) -> Result<(Network, Option<MR>), Error>
where
    I: Iterator<Item = Result<T, topi_base::Error>>,
    T: TryInto<Config, Error = topi_base::Error> + Ids,
    MR: Messenger<Message>,
{
    let mut id_map: HashMap<String, (ProcessId, Config)> = HashMap::new();
    let mut process_map: HashMap<i64, HashSet<String>> = HashMap::new();
    let mut language_map: HashMap<String, Language> = HashMap::new();

    let is_thread_safe = {
        let python_thread_safe = global.python_thread_safe;
        move |lang: &Language| match lang {
            Language::Python => python_thread_safe,
            Language::C => todo!(),
        }
    };

    // Fill id and language maps
    for cfg in node_config {
        let cfg = cfg?;
        let process_id: ProcessId = cfg.process_id();
        let node_id = String::from(cfg.node_id());
        let cfg = cfg.try_into()?;
        // A node in the main process can't run in the main thread.
        if (process_id == ProcessId::from(0)) && cfg.must_be_main_thread() {
            return Err(Error::MainThreadRequestInMainProcess(node_id));
        }
        // A node in its own process will always run in the main thread.
        if (process_id == ProcessId::from(-1)) && !cfg.must_be_main_thread() {
            return Err(Error::SeparateThreadRequestInOwnProcess(node_id));
        }
        if language_map
            .insert(node_id.clone(), (&cfg).into())
            .is_some()
        {
            return Err(Error::TopiBase(topi_base::Error::DuplicateNodeName(
                node_id,
            )));
        }
        id_map.insert(node_id, (process_id, cfg));
    }

    if id_map.is_empty() {
        return Err(Error::NoNodesSpecified);
    }

    // Assign nodes to processes.
    for (node_id, (ProcessId(process_id), _)) in id_map.iter() {
        let process_id = *process_id;
        if process_id < -1 {
            return Err(Error::ProcessIdOutOfRange(process_id));
        }
        if let Some(value) = process_map.get_mut(&process_id) {
            // Key is present.
            if !value.insert(node_id.clone()) {
                return Err(Error::TopiBase(topi_base::Error::DuplicateNodeName(
                    node_id.clone(),
                )));
            }
        } else {
            // Key is absent.
            let mut value = HashSet::new();
            value.insert(node_id.clone());
            process_map.insert(process_id, value);
        }
    }

    // All nodes in the main process must be thread safe because we don't know what
    // other threads will run in it outside of topi.
    if let Some(set) = process_map.get(&0) {
        if let Some(id) = set.iter().find(|&id| !is_thread_safe(&language_map[id])) {
            return Err(Error::ThreadUnsafeNodeInMainProcess(id.clone()));
        }
    }

    // Nodes assigned to random processes with id > 0 must not share the process
    // with another node in the same language if the language is not thread
    // safe.
    for (process_id, set) in process_map.iter().filter(|&(&k, _)| k > 0) {
        let mut thread_unsafe_langs: HashMap<Language, String> = HashMap::new();
        // Iterate over nodes in thread unsafe langs, check for duplicates.
        for (unsafe_node_id, lang) in set.iter().filter_map(|id| {
            let lang = language_map[id];
            if !is_thread_safe(&lang) {
                Some((id, lang))
            } else {
                None
            }
        }) {
            if let Some(first_node_id) = thread_unsafe_langs.get(&lang) {
                return Err(Error::ThreadUnsafetyInProcess(
                    *process_id,
                    lang,
                    first_node_id.clone(),
                    unsafe_node_id.clone(),
                ));
            } else {
                thread_unsafe_langs.insert(lang, unsafe_node_id.clone());
            }
        }
    }

    // Build Thread/ProcessCollections
    if process_map.contains_key(&0) {
        // We need some local threads.
        if process_map.keys().all(|&k| k == 0) {
            // We only need local threads.
            let mut threads = ThreadCollectionBuilder::new();
            for (node_id, (_, cfg)) in id_map {
                threads = cfg.add_node(node_id, threads)?;
            }
            if let Some(id) = messenger_id {
                let (threads, messenger) = threads.build_with_messenger(id)?;
                Ok((Network::ThreadCollection(threads), Some(messenger)))
            } else {
                Ok((Network::ThreadCollection(threads.build()?), None))
            }
        } else {
            // Combination of local threads and other processes.
            // Build a ThreadCollection from nodes with process id == 0.
            let threads = {
                let node_ids = process_map.remove(&0).unwrap();
                let mut threads = ThreadCollectionBuilder::new();
                for node_id in node_ids {
                    let (_, cfg) = id_map.remove(&node_id).unwrap();
                    threads = cfg.add_node(node_id, threads)?;
                }
                threads
            };
            // The remaining nodes with process id == -1 or > 0 go into a ProcessCollection.
            let procs = assign_nodes_to_procs(id_map, process_map)?;
            if let Some(id) = messenger_id {
                let (procs, messenger) =
                    procs.build_with_local_threads_and_messenger(threads, id)?;
                Ok((
                    Network::ProcessCollectionWithLocalThreads(procs),
                    Some(messenger),
                ))
            } else {
                Ok((
                    Network::ProcessCollectionWithLocalThreads(
                        procs.build_with_local_threads(threads)?,
                    ),
                    None,
                ))
            }
        }
    } else {
        // All threads in other processes.
        let procs = assign_nodes_to_procs(id_map, process_map)?;
        if let Some(id) = messenger_id {
            let (procs, messenger) = procs.build_with_messenger(id)?;
            Ok((Network::ProcessCollection(procs), Some(messenger)))
        } else {
            Ok((Network::ProcessCollection(procs.build()?), None))
        }
    }
}

/// Builds ProcessCollections from node config information. This does **not**
/// check whether nodes should run in the same process.
fn assign_nodes_to_procs(
    mut id_map: HashMap<String, (ProcessId, Config)>,
    mut process_map: HashMap<i64, HashSet<String>>,
) -> Result<ProcessCollectionBuilder<Message, Config>, Error> {
    let mut procs = ProcessCollectionBuilder::new();
    // Deal with nodes that get their own exclusive process (process id == -1).
    if let Some(node_ids) = process_map.remove(&-1) {
        let mut max_proc_id = if let Some(max) = process_map.keys().max() {
            *max
        } else {
            1
        };
        for node_id in node_ids {
            let (_, cfg) = id_map.remove(&node_id).unwrap();
            let mut nodes = HashMap::new();
            nodes.insert(node_id, cfg);
            max_proc_id = if let Some(proc_id) = max_proc_id.checked_add(1) {
                proc_id
            } else if let Some(proc_id) = (1_i64..i64::MAX)
                .into_iter()
                .find(|i| !process_map.contains_key(&i))
            {
                proc_id
            } else {
                return Err(Error::OutOfProcessIds);
            };
            procs = procs.add_process(
                format!("{}", max_proc_id),
                nodes,
                Some(std::path::Path::new("topi_proc")),
            )?;
        }
    }
    // Assign remaining nodes to processes.
    for (process_id, node_ids) in process_map {
        let nodes: HashMap<_, _> = node_ids
            .into_iter()
            .map(|node_id| {
                let (_, cfg) = id_map.remove(&node_id).unwrap();
                (node_id, cfg)
            })
            .collect();
        procs = procs.add_process(
            format!("{}", process_id),
            nodes,
            Some(std::path::Path::new("topi_proc")),
        )?;
    }
    // Make sure we've dealt with all nodes.
    debug_assert!(id_map.is_empty());
    Ok(procs)
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum Language {
    Python,
    C,
}

impl<'a> From<&'a Config> for Language {
    fn from(config: &'a Config) -> Self {
        match config {
            Config::Python { .. } => Language::Python,
            Config::C { .. } => Language::C,
        }
    }
}

#[derive(Debug)]
pub enum Error {
    CompiledWithoutLangSupport(Language),
    NoNodesSpecified,
    ProcessIdOutOfRange(i64),
    ThreadUnsafeNodeInMainProcess(String),
    ThreadUnsafetyInProcess(i64, Language, String, String),
    OutOfProcessIds,
    MainThreadRequestInMainProcess(String),
    SeparateThreadRequestInOwnProcess(String),
    TopiBase(topi_base::Error),
}

impl From<topi_base::Error> for Error {
    fn from(err: topi_base::Error) -> Self {
        Error::TopiBase(err)
    }
}

enum Network {
    ThreadCollection(ThreadCollection<Message>),
    ProcessCollection(ProcessCollection<Message, Config>),
    ProcessCollectionWithLocalThreads(ProcessCollectionWithLocalThreads<Message, Config>),
}

mod config_into_node {
    use super::*;
    #[cfg(feature = "python-node")]
    use topi_node_python::PythonNode;

    #[cfg(feature = "python-node")]
    pub fn python_add_node<S: AsRef<str>>(
        source: &str,
        id: S,
        thread_builder: ThreadCollectionBuilder<Message>,
    ) -> Result<ThreadCollectionBuilder<Message>, topi_base::Error> {
        thread_builder.add_thread(id, PythonNode::new(source))
    }

    #[allow(unused_variables)]
    #[cfg(not(feature = "python-node"))]
    pub fn python_add_node<S: AsRef<str>>(
        source: &str,
        id: S,
        thread_builder: ThreadCollectionBuilder<Message>,
    ) -> Result<ThreadCollectionBuilder<Message>, topi_base::Error> {
        // TODO: return error
        panic!("Activate the 'python_node-main' feature to run Python nodes in the main process.")
    }

    #[cfg(feature = "python-node")]
    pub fn python_add_node_as_main_with_controller<S, F>(
        source: &str,
        id: S,
        thread_builder: ThreadCollectionBuilder<Message>,
        ipc_pair: Option<(IpcSender<Message>, IpcReceiver<Message>)>,
        controller: F,
    ) -> Result<(), topi_base::Error>
    where
        S: AsRef<str>,
        F: FnOnce(ThreadCollection<Message>) -> Result<(), topi_base::Error> + Send + 'static,
    {
        thread_builder.build_with_main_thread_and_controller(
            id,
            PythonNode::new(source),
            ipc_pair,
            controller,
        )
    }

    #[allow(unused_variables)]
    #[cfg(not(feature = "python-node"))]
    pub fn python_add_node_as_main_with_controller<S, F>(
        source: &str,
        id: S,
        thread_builder: ThreadCollectionBuilder<Message>,
        ipc_pair: Option<(IpcSender<Message>, IpcReceiver<Message>)>,
        controller: F,
    ) -> Result<(), topi_base::Error>
    where
        S: AsRef<str>,
        F: FnOnce(ThreadCollection<Message>) -> Result<(), topi_base::Error> + Send + 'static,
    {
        // TODO: return error
        panic!("Activate the 'python_node-main' feature to run Python nodes in the main process.")
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parse_config() {
        let config = include_str!("../tests/topi.toml");

        let config: ParsedConfig = toml::from_str(&config).unwrap();
        assert_eq!(config.global.debug_mode, false);
        assert_eq!(config.global.python_thread_safe, false);
        assert!(config.c.is_none());
        let py_cfg = &config.python.as_ref().unwrap()[0];
        assert_eq!(py_cfg.common.path, "./python_tests/tests.py");
        assert_eq!(py_cfg.main_thread_in_process, true);
    }
}

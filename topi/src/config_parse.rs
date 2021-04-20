// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use crate::{Config, Ids, ProcessId};
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::default::Default;
use std::fs::File;
use std::io::Read;

#[derive(Clone, Serialize, Deserialize)]
pub struct ParsedConfig {
    #[serde(flatten)]
    pub global: GlobalConfig,
    pub python: Option<Vec<ParsedPythonConfig>>,
    pub c: Option<Vec<ParsedCConfig>>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct GlobalConfig {
    #[serde(default = "default_false")]
    pub debug_mode: bool,
    #[serde(default = "default_false")]
    pub python_thread_safe: bool,
}

impl Default for GlobalConfig {
    fn default() -> Self {
        GlobalConfig {
            debug_mode: default_false(),
            python_thread_safe: default_false(),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ParsedCommonConfig {
    pub node_id: String,
    pub path: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ParsedCConfig {
    #[serde(default = "default_zero")]
    pub process_id: i64,
    #[serde(default = "default_false")]
    pub main_thread_in_process: bool,
    #[serde(flatten)]
    pub common: ParsedCommonConfig,
}

impl TryFrom<ParsedCConfig> for Config {
    type Error = topi_base::Error;
    fn try_from(_cfg: ParsedCConfig) -> Result<Self, topi_base::Error> {
        todo!()
    }
}

impl Ids for ParsedCConfig {
    fn node_id(&self) -> &str {
        self.common.node_id.as_ref()
    }
    fn process_id(&self) -> ProcessId {
        ProcessId(self.process_id)
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ParsedPythonConfig {
    #[serde(default = "default_minus_one")]
    pub process_id: i64,
    #[serde(default = "default_true")]
    pub main_thread_in_process: bool,
    #[serde(flatten)]
    pub common: ParsedCommonConfig,
}

impl TryFrom<ParsedPythonConfig> for Config {
    type Error = topi_base::Error;
    fn try_from(py_cfg: ParsedPythonConfig) -> Result<Self, topi_base::Error> {
        let ParsedPythonConfig {
            common: ParsedCommonConfig { path, .. },
            main_thread_in_process,
            ..
        } = py_cfg;
        let source = {
            let mut file = File::open(path)?;
            let mut source = String::new();
            file.read_to_string(&mut source)?;
            source
        };
        Ok(Config::Python {
            source,
            must_be_main_thread: main_thread_in_process,
        })
    }
}

impl Ids for ParsedPythonConfig {
    fn node_id(&self) -> &str {
        self.common.node_id.as_ref()
    }
    fn process_id(&self) -> ProcessId {
        ProcessId(self.process_id)
    }
}

fn default_true() -> bool {
    true
}

fn default_false() -> bool {
    false
}

fn default_minus_one() -> i64 {
    -1
}

fn default_zero() -> i64 {
    0
}

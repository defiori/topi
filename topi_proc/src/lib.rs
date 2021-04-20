// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use serde::Serialize;
use structopt::StructOpt;
use topi::{
    test_utils::{BenchMessage, TestConfig},
    Config,
};
pub use topi_base::Error;
use topi_base::{self, data::c_api, messaging, nodes::NodeConfig, process::run_topi_proc};

#[cfg(feature = "called-from-python")]
pub use topi_node_python;

#[derive(Debug, StructOpt)]
#[structopt(name = "topi_proc", about = "Runs topi threads in a separate process.")]
struct Opt {
    // Takes care of the executable path which is passed as a positional argument
    // when used as a console_script entry point in Python (see pytopi_proc).
    path: Option<String>,

    /// Print input.
    #[structopt(long)]
    echo: Option<String>,

    /// Server name to bootstrap ipc_channel.
    #[structopt(long, conflicts_with = "echo", required_unless = "echo")]
    server: Option<String>,
}

fn run<M, C>()
where
    M: messaging::Message + Serialize,
    C: NodeConfig<M>,
{
    use std::io::{self, Write};
    let opt = Opt::from_args();
    if let Some(echo) = opt.echo {
        print!("{}", echo);
        // Flush stdout to make sure the echo is printed
        io::stdout().flush().unwrap();
    } else if let Some(server_name) = opt.server {
        if let Err(err) = run_topi_proc::<M, C>(server_name) {
            println!("TOPIPROC err {:?}", err);
        }
    } else {
        unreachable!()
    }
}

pub fn main_() {
    run::<c_api::Message, Config>()
}

pub fn main_test_() {
    run::<BenchMessage, TestConfig>()
}

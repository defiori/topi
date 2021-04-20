// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

#[cfg(all(
    feature = "cargo-install-topi-proc-local",
    not(feature = "cargo-install-topi-proc")
))]
use std::{env, path, process};
#[cfg(all(
    feature = "cargo-install-topi-proc",
    not(feature = "cargo-install-topi-proc-local")
))]
use std::{env, process};

fn main() {
    cargo_install_topi_proc();
}

#[cfg(all(
    feature = "cargo-install-topi-proc",
    not(feature = "cargo-install-topi-proc-local")
))]
fn cargo_install_topi_proc() {
    let cargo_binary = env::var("CARGO").unwrap();
    let output = process::Command::new(cargo_binary)
        .arg("install")
        .arg("--force")
        .arg("--bin")
        .arg("topi_proc")
        .output()
        .unwrap();
    if !output.status.success() {
        println!(
            "cargo:warning={}",
            String::from_utf8(output.stderr).unwrap()
        );
    }
}

#[cfg(all(
    feature = "cargo-install-topi-proc-local",
    not(feature = "cargo-install-topi-proc")
))]
fn cargo_install_topi_proc() {
    let cargo_binary = env::var("CARGO").unwrap();
    let topi_home = env::var("CARGO_MANIFEST_DIR").unwrap();
    let mut topi_proc_home = path::PathBuf::from(topi_home);
    topi_proc_home.push("topi_proc");
    let output = process::Command::new(cargo_binary)
        .arg("install")
        .arg("--force")
        .arg("--bin")
        .arg("topi_proc")
        .arg("--path")
        .arg(topi_proc_home)
        .output()
        .unwrap();
    if !output.status.success() {
        println!(
            "cargo:warning={}",
            String::from_utf8(output.stderr).unwrap()
        );
    }
}

#[cfg(not(any(
    feature = "cargo-install-topi-proc",
    feature = "cargo-install-topi-proc-local"
)))]
fn cargo_install_topi_proc() {}

#[cfg(all(
    feature = "cargo-install-topi-proc",
    feature = "cargo-install-topi-proc-local"
))]
fn cargo_install_topi_proc() {
    println!("cargo:warning=The 'cargo-install-topi-proc' and 'cargo-install-topi-proc-local' features are mutually exclusive, 'topi_proc' not installed.");
}

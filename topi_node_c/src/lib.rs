// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use topi_base::{
    data::c_api::{Callbacks, Message, Messenger},
    nodes::Node,
};

#[allow(dead_code)]
pub struct C {
    path: String,
    symbol: String,
}

impl C {
    pub fn new(path: String, symbol: String) -> Self {
        C { path, symbol }
    }
}

impl Node<Message, Messenger> for C {
    fn entrypoint(&mut self, messenger: &mut Messenger) -> Result<(), String> {
        let lib = libloading::Library::new(&self.path).unwrap();
        let entry: libloading::Symbol<extern "C" fn(*const Messenger, *const Callbacks)> =
            unsafe { lib.get(b"entrypoint\0").unwrap() };
        let mut callbacks = Callbacks::new();
        entry(
            messenger as *mut Messenger,
            &mut callbacks as *mut Callbacks,
        );
        Ok(())
    }
}

impl Drop for C {
    fn drop(&mut self) {
        println!("C dropped.");
    }
}

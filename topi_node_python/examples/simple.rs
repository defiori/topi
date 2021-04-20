// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use indoc::indoc;
use topi_base::{
    data::{
        c_api::{Message, Messenger},
        Content, DataMessage,
    },
    messaging::Envelope,
    ndarray,
    nodes::Node,
    sharify,
    thread::ThreadCollectionBuilder,
};
use topi_node_python::PythonNode;

fn main() {
    let threads = ThreadCollectionBuilder::new();
    let base_node = Base {};
    let python_source = indoc! {"
        import numpy as np
        from topi import *
        from gc import collect
        import IPython

        def topi_entrypoint(messenger):
            IPython.embed_kernel()
    "};
    threads
        .add_thread("Eagle", PythonNode::new(python_source))
        .unwrap()
        .build_with_main_thread("Base", base_node, None)
        .unwrap();
}

struct Base {}

impl Node<Message, Messenger> for Base {
    fn entrypoint(&mut self, messenger: &mut Messenger) -> Result<(), String> {
        loop {
            match messenger.recv().unwrap() {
                Envelope::Terminate => break,
                Envelope::Message(DataMessage { content, .. }) => match content {
                    Content::ArrayMutFloat64(mut arr) => {
                        *arr.first_mut().unwrap() += 1.0;
                        messenger.send("Eagle", arr, None).unwrap();
                    }
                    Content::SharedArrayMutFloat64(mut arr) => {
                        {
                            let mut view = arr.as_view_mut();
                            *view.first_mut().unwrap() += 1.0;
                        }
                        messenger.send("Eagle", arr, None).unwrap();
                    }
                    Content::String(s) => match s.as_str() {
                        "quit" => break,
                        "make" => {
                            let mut array: sharify::SharedArrayMut<f64, ndarray::IxDyn> =
                                sharify::SharedArrayMut::new(&(0.0, ndarray::IxDyn(&[1000])))
                                    .unwrap();
                            *array.as_view_mut().first_mut().unwrap() = 25.0;
                            messenger.send("Eagle", array, None).unwrap();
                        }
                        _ => {
                            messenger
                                .send("Eagle", "Base received unexpected String.", None)
                                .unwrap();
                        }
                    },
                    _ => panic!("Base received unexpected object."),
                },
            }
        }
        Ok(())
    }
}

impl Drop for Base {
    fn drop(&mut self) {
        println!("Base dropped.");
    }
}

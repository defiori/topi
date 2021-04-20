// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use topi_base::{
    data::{
        c_api::{Message, Messenger},
        Content, DataMessage,
    },
    messaging::Envelope,
    ndarray,
    nodes::Node,
    sharify::SharedArrayMut,
    thread::ThreadCollectionBuilder,
};
use topi_node_c::C;

fn main() {
    let threads = ThreadCollectionBuilder::new();
    let base_node = Base {};
    threads
        .add_thread(
            "Eagle",
            C::new(String::from("/tmp/libsimple.so"), String::from("_")),
        )
        .unwrap()
        .build_with_main_thread("Base", base_node, None)
        .unwrap();
}

struct Base {}

impl Node<Message, Messenger> for Base {
    fn entrypoint(&mut self, messenger: &mut Messenger) -> Result<(), String> {
        let mut array: SharedArrayMut<f64, ndarray::IxDyn> =
            SharedArrayMut::new(&(0.0, ndarray::IxDyn(&[1000]))).unwrap();
        array.as_view_mut()[0] = 25.0;
        messenger.send("Eagle", array, None).unwrap();
        if let Envelope::Message(DataMessage {
            content: Content::SharedArrayMutFloat64(mut array),
            ..
        }) = messenger.recv().unwrap()
        {
            assert!((array.as_view_mut()[0] - 99.0).abs() < f64::EPSILON);
            messenger
                .send(
                    "Eagle",
                    ndarray::Array::<f64, _>::zeros(ndarray::IxDyn(&[100])),
                    None,
                )
                .unwrap();
            Ok(())
        } else {
            panic!("Base received unexpected message.")
        }
    }
}

impl Drop for Base {
    fn drop(&mut self) {
        println!("Base dropped.");
    }
}

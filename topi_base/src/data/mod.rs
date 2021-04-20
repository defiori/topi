// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

pub mod c_api;

use crate::{
    messaging::{Channels, Envelope, Message, Messenger, SelectResult, Sender, ThreadReceiver},
    Error,
};
use ndarray::{self, Dimension, IxDyn};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sharify::{SharedArray, SharedArrayMut, SharedSlice, SharedSliceMut};
use std::collections::HashMap;
use std::convert::From;
use topi_internal_macros::{impl_Sealed_for_arrays, impl_TopiType_for_arrays};

#[repr(C)]
pub struct DataMessenger<U, ME>
where
    U: Message + Serialize,
    ME: Message + Serialize,
{
    owning_thread_id: String,
    send_peers: HashMap<String, Sender<DataMessage<U, ME>>>,
    recv_peers: ThreadReceiver<DataMessage<U, ME>>,
    channels: Channels<DataMessage<U, ME>>,
}

impl<U, ME> Messenger<DataMessage<U, ME>> for DataMessenger<U, ME>
where
    U: Message + Serialize,
    ME: Message + Serialize,
{
    fn new(channels: Channels<DataMessage<U, ME>>) -> Self {
        DataMessenger {
            owning_thread_id: channels.owning_thread_id(),
            recv_peers: channels.receiver_from_peers(),
            send_peers: channels.send_to_peers(),
            channels,
        }
    }

    fn channels(&mut self) -> &mut Channels<DataMessage<U, ME>> {
        &mut self.channels
    }
}

impl<U, ME> DataMessenger<U, ME>
where
    U: Message + Serialize,
    ME: Message + Serialize,
{
    pub fn node_id(&self) -> &str {
        self.owning_thread_id.as_ref()
    }

    pub fn send<S, T>(&self, recv_id: S, object: T, metadata: Option<ME>) -> Result<(), Error>
    where
        S: std::convert::AsRef<str>,
        T: TopiType<U>,
    {
        if let Some(sender) = self.send_peers.get(recv_id.as_ref()) {
            match sender {
                Sender::ProcLocal(tx) => {
                    tx.send(DataMessage {
                        content: object.into_content(),
                        metadata,
                    })?;
                }
                Sender::Ipc(tx) => {
                    tx.send(DataMessage {
                        content: object.into_content(),
                        metadata,
                    })?;
                }
            }
            Ok(())
        } else {
            Err(Error::UnknownSendDestination(String::from(
                recv_id.as_ref(),
            )))
        }
    }

    pub fn send_user_data<S: std::convert::AsRef<str>>(
        &self,
        recv_id: S,
        msg: U,
        metadata: Option<ME>,
    ) -> Result<(), Error> {
        if let Some(sender) = self.send_peers.get(recv_id.as_ref()) {
            let msg = DataMessage {
                content: Content::UserData(msg),
                metadata,
            };
            match sender {
                Sender::ProcLocal(tx) => {
                    tx.send(msg)?;
                }
                Sender::Ipc(tx) => {
                    tx.send(msg)?;
                }
            }
            Ok(())
        } else {
            Err(Error::UnknownSendDestination(String::from(
                recv_id.as_ref(),
            )))
        }
    }

    pub fn recv(&self) -> Result<Envelope<DataMessage<U, ME>>, Error> {
        self.recv_peers.recv()
    }

    pub fn try_recv(&self) -> Result<Option<Envelope<DataMessage<U, ME>>>, Error> {
        self.recv_peers.try_recv()
    }

    pub fn select_recv<T>(
        &self,
        external: &[crossbeam_channel::Receiver<T>],
    ) -> Result<SelectResult<DataMessage<U, ME>, T>, Error> {
        match self.recv_peers.select(external)? {
            SelectResult::Self_(msg) => Ok(SelectResult::Self_(msg)),
            SelectResult::External { recv_index, data } => {
                Ok(SelectResult::External { recv_index, data })
            }
        }
    }

    // TODO
    pub fn is_ipc_destination<S: std::convert::AsRef<str>>(&self, _id: S) -> bool {
        todo!();
    }
}

#[derive(Serialize, Deserialize)]
pub struct DataMessage<U, ME> {
    pub metadata: Option<ME>,
    pub content: Content<U>,
}

impl<U, ME> DataMessage<U, ME> {
    pub fn new_from_user(data: U, metadata: Option<ME>) -> Self {
        Self {
            content: Content::UserData(data),
            metadata,
        }
    }
}

#[non_exhaustive]
#[derive(Serialize, Deserialize)]
pub enum Content<U> {
    // Boolean arrays
    ArrayMutBool(ndarray::Array<bool, IxDyn>),
    SharedArrayBool(SharedArray<bool, IxDyn>),
    SharedArrayMutBool(SharedArrayMut<bool, IxDyn>),
    // Arrays u8
    ArrayMutUInt8(ndarray::Array<u8, IxDyn>),
    SharedArrayUInt8(SharedArray<u8, IxDyn>),
    SharedArrayMutUInt8(SharedArrayMut<u8, IxDyn>),
    // Arrays u16
    ArrayMutUInt16(ndarray::Array<u16, IxDyn>),
    SharedArrayUInt16(SharedArray<u16, IxDyn>),
    SharedArrayMutUInt16(SharedArrayMut<u16, IxDyn>),
    // Arrays u32
    ArrayMutUInt32(ndarray::Array<u32, IxDyn>),
    SharedArrayUInt32(SharedArray<u32, IxDyn>),
    SharedArrayMutUInt32(SharedArrayMut<u32, IxDyn>),
    // Arrays u64
    ArrayMutUInt64(ndarray::Array<u64, IxDyn>),
    SharedArrayUInt64(SharedArray<u64, IxDyn>),
    SharedArrayMutUInt64(SharedArrayMut<u64, IxDyn>),
    // Arrays i8
    ArrayMutInt8(ndarray::Array<i8, IxDyn>),
    SharedArrayInt8(SharedArray<i8, IxDyn>),
    SharedArrayMutInt8(SharedArrayMut<i8, IxDyn>),
    // Arrays i16
    ArrayMutInt16(ndarray::Array<i16, IxDyn>),
    SharedArrayInt16(SharedArray<i16, IxDyn>),
    SharedArrayMutInt16(SharedArrayMut<i16, IxDyn>),
    // Arrays i32
    ArrayMutInt32(ndarray::Array<i32, IxDyn>),
    SharedArrayInt32(SharedArray<i32, IxDyn>),
    SharedArrayMutInt32(SharedArrayMut<i32, IxDyn>),
    // Arrays i64
    ArrayMutInt64(ndarray::Array<i64, IxDyn>),
    SharedArrayInt64(SharedArray<i64, IxDyn>),
    SharedArrayMutInt64(SharedArrayMut<i64, IxDyn>),
    // Arrays f32
    ArrayMutFloat32(ndarray::Array<f32, IxDyn>),
    SharedArrayFloat32(SharedArray<f32, IxDyn>),
    SharedArrayMutFloat32(SharedArrayMut<f32, IxDyn>),
    // Arrays f64
    ArrayMutFloat64(ndarray::Array<f64, IxDyn>),
    SharedArrayFloat64(SharedArray<f64, IxDyn>),
    SharedArrayMutFloat64(SharedArrayMut<f64, IxDyn>),
    // Immutable string
    String(String),
    // Bytes
    Bytes(Vec<u8>),
    SharedBytes(SharedSlice<u8>),
    SharedBytesMut(SharedSliceMut<u8>),
    // User defined
    UserData(U),
}

#[impl_TopiType_for_arrays(bool, u8, u16, u32, u64, i8, i16, i32, i64, f32, f64)]
pub trait TopiType<U>: private::Sealed {
    #[doc(hidden)]
    fn into_content(self) -> Content<U>;
}

impl<'a, U> TopiType<U> for &'a str {
    fn into_content(self) -> Content<U> {
        Content::String(String::from(self))
    }
}

impl<'a, U> TopiType<U> for &'a [u8] {
    fn into_content(self) -> Content<U> {
        Content::Bytes(self.into())
    }
}

impl<U> TopiType<U> for SharedSlice<u8> {
    fn into_content(self) -> Content<U> {
        Content::SharedBytes(self)
    }
}

impl<U> TopiType<U> for SharedSliceMut<u8> {
    fn into_content(self) -> Content<U> {
        Content::SharedBytesMut(self)
    }
}

mod private {
    use super::*;

    #[impl_Sealed_for_arrays(bool, u8, u16, u32, u64, i8, i16, i32, i64, f32, f64)]
    pub trait Sealed {}

    impl<'a> Sealed for &'a str {}

    impl<'a> Sealed for &'a [u8] {}

    impl Sealed for SharedSlice<u8> {}

    impl Sealed for SharedSliceMut<u8> {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messaging::{create_messenger, ipc_channel, thread_channel, Envelope};

    fn create_thread_messenger_pair() -> (DataMessenger<(), ()>, DataMessenger<(), ()>) {
        let (send_remote, recv_base) = thread_channel().unwrap();
        let (msgr_base, send_base, _) = create_messenger(
            "base",
            std::iter::empty(),
            [(String::from("remote"), send_remote.clone())]
                .iter()
                .map(|(a, b)| (a, b)),
            std::iter::empty::<(String, _)>(),
            None,
            None,
        )
        .unwrap();
        let (msgr_remote, _, _) = create_messenger(
            "remote",
            std::iter::empty(),
            [(String::from("base"), send_base)]
                .iter()
                .map(|(a, b)| (a, b)),
            std::iter::empty::<(String, _)>(),
            Some((send_remote, recv_base)),
            None,
        )
        .unwrap();
        (msgr_base, msgr_remote)
    }

    fn create_ipc_messenger_pair() -> (DataMessenger<(), ()>, DataMessenger<(), ()>) {
        let (ipc_send_remote, ipc_recv_base) = ipc_channel().unwrap();
        let (msgr_base, _, ipc_send_base) = create_messenger(
            "base",
            std::iter::empty(),
            std::iter::empty::<(String, _)>(),
            [(String::from("remote"), ipc_send_remote.clone())]
                .iter()
                .map(|(a, b)| (a, b)),
            None,
            None,
        )
        .unwrap();
        let (msgr_remote, _, _) = create_messenger(
            "remote",
            std::iter::empty(),
            std::iter::empty::<(String, _)>(),
            [(String::from("base"), ipc_send_base)]
                .iter()
                .map(|(a, b)| (a, b)),
            None,
            Some((ipc_send_remote, ipc_recv_base)),
        )
        .unwrap();
        (msgr_base, msgr_remote)
    }

    #[test]
    fn shared_mut_array_send_thread() {
        let (msgr_base, msgr_remote) = create_thread_messenger_pair();
        shared_mut_array_send(&msgr_base, &msgr_remote);
    }

    #[test]
    fn shared_mut_array_send_ipc() {
        let (msgr_base, msgr_remote) = create_ipc_messenger_pair();
        shared_mut_array_send(&msgr_base, &msgr_remote);
    }

    fn shared_mut_array_send(
        msgr_base: &DataMessenger<(), ()>,
        msgr_remote: &DataMessenger<(), ()>,
    ) {
        let shapes: &[&[usize]] = &[
            &[100],
            &[100, 100],
            &[100, 200],
            &[200, 100],
            &[100, 200, 300],
            &[200, 300, 100],
            &[300, 100, 200],
            &[1, 2, 3, 4, 5, 6, 7, 8, 9],
        ];
        for &shape in shapes.iter() {
            let mut array: SharedArrayMut<f64, ndarray::IxDyn> =
                SharedArrayMut::new(&(0.0, ndarray::IxDyn(shape))).unwrap();
            for (i, element) in array.as_view_mut().iter_mut().enumerate() {
                *element = i as f64;
            }
            msgr_base.send("remote", array, None).unwrap();
            let array_recv = if let Envelope::Message(DataMessage { content, .. }) =
                msgr_remote.recv().unwrap()
            {
                match content {
                    Content::SharedArrayMutFloat64(arr) => arr,
                    _ => panic!("Received unexpected object."),
                }
            } else {
                panic!("Received unexpected message.")
            };
            assert_eq!(array_recv.as_view().shape(), shape);
            for (sent, &recv) in array_recv.as_view().iter().enumerate() {
                assert!((sent as f64 - recv).abs() < f64::EPSILON);
            }

            msgr_remote.send("base", array_recv, None).unwrap();
            let array_recv =
                if let Envelope::Message(DataMessage { content, .. }) = msgr_base.recv().unwrap() {
                    match content {
                        Content::SharedArrayMutFloat64(arr) => arr,
                        _ => panic!("Received unexpected object."),
                    }
                } else {
                    panic!("Received unexpected message.")
                };
            assert_eq!(array_recv.as_view().shape(), shape);
        }
    }
}

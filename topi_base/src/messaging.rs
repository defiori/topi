// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use crate::{thread::ThreadMessage, Error};
use ipc_channel::ipc;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::marker::Send;
use std::panic::UnwindSafe;
use std::thread;

#[derive(Debug)]
#[non_exhaustive]
pub enum Sender<M> {
    Ipc(IpcSender<M>),
    ProcLocal(ThreadSender<M>),
}

impl<M> Clone for Sender<M> {
    fn clone(&self) -> Self {
        match self {
            Self::Ipc(sender) => Sender::Ipc(sender.clone()),
            Self::ProcLocal(sender) => Sender::ProcLocal(sender.clone()),
        }
    }
}

pub fn thread_channel<M>() -> Result<(ThreadSender<M>, ThreadReceiver<M>), Error> {
    let (inner_sender, inner_receiver) = crossbeam_channel::unbounded();
    let sender = ThreadSender {
        inner: inner_sender,
    };
    let receiver = ThreadReceiver {
        inner: inner_receiver,
    };
    Ok((sender, receiver))
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Envelope<M> {
    Message(M),
    Terminate,
}

impl<M> Envelope<M> {
    /// Retrieves the message contained in the `Envelope::Message` variant,
    /// panicks for other variants.
    pub fn content(self) -> M {
        match self {
            Envelope::Message(msg) => msg,
            Envelope::Terminate => panic!("Envelope without content."),
        }
    }
}

#[derive(Debug)]
pub struct ThreadSender<M> {
    inner: crossbeam_channel::Sender<Envelope<M>>,
}

impl<M> Clone for ThreadSender<M> {
    fn clone(&self) -> Self {
        ThreadSender {
            inner: self.inner.clone(),
        }
    }
}

impl<M> ThreadSender<M> {
    pub fn send(&self, msg: M) -> Result<(), Error> {
        Ok(self.inner.send(Envelope::Message(msg))?)
    }

    pub(crate) fn send_internal(&self, envelope: Envelope<M>) -> Result<(), Error> {
        Ok(self.inner.send(envelope)?)
    }
}

#[derive(Debug)]
pub struct ThreadReceiver<M> {
    inner: crossbeam_channel::Receiver<Envelope<M>>,
}

impl<M> Clone for ThreadReceiver<M> {
    fn clone(&self) -> Self {
        ThreadReceiver {
            inner: self.inner.clone(),
        }
    }
}

impl<M> ThreadReceiver<M> {
    pub fn recv(&self) -> Result<Envelope<M>, Error> {
        Ok(self.inner.recv()?)
    }

    pub fn try_recv(&self) -> Result<Option<Envelope<M>>, Error> {
        match self.inner.try_recv() {
            Ok(msg) => Ok(Some(msg)),
            Err(err) => {
                if let crossbeam_channel::TryRecvError::Empty = err {
                    Ok(None)
                } else {
                    Err(err.into())
                }
            }
        }
    }

    pub fn select<T>(
        &self,
        external: &[crossbeam_channel::Receiver<T>],
    ) -> Result<SelectResult<M, T>, Error> {
        let mut select = crossbeam_channel::Select::new();
        select.recv(&self.inner);
        for ext_recv in external {
            select.recv(ext_recv);
        }
        let oper = select.select();
        let recv_index = oper.index();
        if recv_index == 0 {
            Ok(SelectResult::Self_(oper.recv(&self.inner)?))
        } else {
            let data = oper.recv(&external[recv_index - 1])?;
            Ok(SelectResult::External { recv_index, data })
        }
    }
}

pub enum SelectResult<M, T> {
    Self_(Envelope<M>),
    External { recv_index: usize, data: T },
}

pub fn ipc_channel<M>() -> Result<(IpcSender<M>, IpcReceiver<M>), Error> {
    let (bytes_sender, bytes_receiver) = ipc::bytes_channel()?;
    let sender = IpcSender {
        inner: bytes_sender,
        msg: std::marker::PhantomData,
    };
    let receiver = IpcReceiver {
        inner: bytes_receiver,
        msg: std::marker::PhantomData,
    };
    Ok((sender, receiver))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IpcSender<M> {
    inner: ipc::IpcBytesSender,
    msg: std::marker::PhantomData<M>,
}

impl<M> Clone for IpcSender<M> {
    fn clone(&self) -> Self {
        IpcSender {
            inner: self.inner.clone(),
            msg: self.msg,
        }
    }
}

impl<M> IpcSender<M>
where
    M: Serialize + Message,
{
    pub fn send(&self, msg: M) -> Result<(), Error> {
        let envelope = IpcEnvelope::UserData(Envelope::Message(msg));
        // TODO: allocate Vec with capacity, size hint in `Message`?
        let mut bytes = Vec::new();
        bincode::serialize_into(&mut bytes, &envelope)?;
        Ok(self.inner.send(bytes.as_slice())?)
    }
}

impl<M> IpcSender<M>
where
    M: Serialize + DeserializeOwned,
{
    pub(crate) fn send_internal(&self, envelope: IpcEnvelope<M>) -> Result<(), Error> {
        // TODO: allocate Vec with capacity, size hint?
        let mut bytes = Vec::new();
        bincode::serialize_into(&mut bytes, &envelope)?;
        Ok(self.inner.send(bytes.as_slice())?)
    }
}

pub struct IpcReceiver<M> {
    inner: ipc::IpcBytesReceiver,
    msg: std::marker::PhantomData<M>,
}

impl<M> IpcReceiver<M>
where
    M: Serialize + Message,
{
    pub fn recv(&self) -> Result<Envelope<M>, Error> {
        let bytes = self.inner.recv()?;
        match bincode::deserialize(bytes.as_slice())? {
            IpcEnvelope::QuitRouter => panic!("Unexpected message."),
            IpcEnvelope::UserData(data) => Ok(data),
        }
    }

    pub(crate) fn recv_internal(&self) -> Result<IpcEnvelope<M>, Error> {
        let bytes = self.inner.recv()?;
        Ok(bincode::deserialize(bytes.as_slice())?)
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(bound = "M: DeserializeOwned")]
pub(crate) enum IpcEnvelope<M>
where
    M: Serialize + DeserializeOwned,
{
    QuitRouter,
    UserData(Envelope<M>),
}

pub trait Messenger<M>: UnwindSafe
where
    M: Message + Serialize,
{
    fn new(channels: Channels<M>) -> Self;
    // Forces the Messenger impl type to hold on to the Channels.
    fn channels(&mut self) -> &mut Channels<M>;
}

#[allow(dead_code)]
pub struct Channels<M>
where
    M: Serialize + DeserializeOwned,
{
    pub(crate) owning_thread_id: String,
    pub(crate) send_parent: Option<crossbeam_channel::Sender<ThreadMessage>>,
    pub(crate) recv_peers: ThreadReceiver<M>,
    pub(crate) send_peers: HashMap<String, Sender<M>>,
    pub(crate) send_self_ipc: IpcSender<M>,
}

// TODO: what access should Messenger implementors have? send_parent?
impl<M> Channels<M>
where
    M: Serialize + DeserializeOwned,
{
    pub fn owning_thread_id(&self) -> String {
        self.owning_thread_id.clone()
    }

    pub fn receiver_from_peers(&self) -> ThreadReceiver<M> {
        self.recv_peers.clone()
    }

    pub fn send_to_peers(&self) -> HashMap<String, Sender<M>> {
        self.send_peers.clone()
    }
}

impl<M> Drop for Channels<M>
where
    M: Serialize + DeserializeOwned,
{
    fn drop(&mut self) {
        // if let Some(sender) = self.send_self_ipc.take() {
        //     sender.send_internal(IpcEnvelope::QuitRouter).unwrap();
        // }
        self.send_self_ipc
            .send_internal(IpcEnvelope::QuitRouter)
            .unwrap();
    }
}

pub trait Message: Send + UnwindSafe + DeserializeOwned + 'static {}

impl<T> Message for T where T: Send + UnwindSafe + DeserializeOwned + 'static {}

pub(crate) fn create_converting_router<M: Message + Serialize>(
    id: &str,
    send_child: ThreadSender<M>,
    recv_peers_ipc: IpcReceiver<M>,
) -> Result<(), Error> {
    thread::Builder::new()
        .name(format!("{}_router", id))
        .spawn(move || {
            loop {
                match recv_peers_ipc.recv_internal() {
                    Ok(envelope) => match envelope {
                        IpcEnvelope::QuitRouter => break,
                        IpcEnvelope::UserData(msg) => {
                            send_child.send_internal(msg).unwrap();
                        }
                    },
                    Err(err) => match err {
                        Error::IpcRecvError(err) => match err {
                            ipc::IpcError::Disconnected => {
                                panic!("Expected QuitRouter message.")
                            }
                            // TODO: handle deserialization error
                            ipc::IpcError::Bincode(_) => todo!(),
                            _ => todo!(),
                        },
                        _ => println!("{:?}", err),
                    },
                }
            }
        })?;
    Ok(())
}

pub(crate) fn create_messenger<'a, S, ID, LD, LDS, IP, IPS, MR, M>(
    id: S,
    mut existing_ids: ID,
    local_destinations: LD,
    ipc_destinations: IP,
    thread_pair: Option<(ThreadSender<M>, ThreadReceiver<M>)>,
    ipc_pair: Option<(IpcSender<M>, IpcReceiver<M>)>,
) -> Result<(MR, ThreadSender<M>, IpcSender<M>), Error>
where
    S: AsRef<str>,
    ID: Iterator<Item = &'a str>,
    LD: IntoIterator<Item = (LDS, &'a ThreadSender<M>)>,
    LDS: AsRef<str>,
    IP: IntoIterator<Item = (IPS, &'a IpcSender<M>)>,
    IPS: AsRef<str>,
    MR: Messenger<M>,
    M: Message + Serialize,
{
    let id = id.as_ref();
    if existing_ids.any(|existing| existing == id) {
        Err(Error::DuplicateNodeName(String::from(id)))
    } else {
        // This is the Messenger tx/rx pair
        let (send_main, recv_peers) = if let Some(thread_pair) = thread_pair {
            thread_pair
        } else {
            thread_channel()?
        };
        // Create router to convert ipc messages to crossbeam ones
        let send_self_ipc = if let Some((send_self_ipc, recv_peers_ipc)) = ipc_pair {
            create_converting_router(id, send_main.clone(), recv_peers_ipc)?;
            send_self_ipc
        } else {
            let (send_self_ipc, recv_peers_ipc) = ipc_channel()?;
            create_converting_router(id, send_main.clone(), recv_peers_ipc)?;
            send_self_ipc
        };
        let mut main_channels = Channels {
            owning_thread_id: String::from(id),
            send_parent: None,
            recv_peers,
            send_peers: HashMap::new(),
            send_self_ipc: send_self_ipc.clone(),
        };
        // Add intra-process senders to the messenger
        for (id, sender) in local_destinations {
            main_channels
                .send_peers
                .insert(String::from(id.as_ref()), Sender::ProcLocal(sender.clone()))
                .and(Some(
                    Result::<(MR, ThreadSender<M>, IpcSender<M>), Error>::Err(
                        Error::DuplicateNodeName(String::from(id.as_ref())),
                    ),
                ))
                .transpose()?;
        }
        // Add inter-process senders to the messenger
        for (id, sender) in ipc_destinations {
            main_channels
                .send_peers
                .insert(String::from(id.as_ref()), Sender::Ipc(sender.clone()))
                .and(Some(
                    Result::<(MR, ThreadSender<M>, IpcSender<M>), Error>::Err(
                        Error::DuplicateNodeName(String::from(id.as_ref())),
                    ),
                ))
                .transpose()?;
        }
        Ok((MR::new(main_channels), send_main, send_self_ipc))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_bincode_roundtrip() {
        let data: [i32; 5] = [1, 2, 3, 5, 4];
        let envelope = IpcEnvelope::UserData(Envelope::Message(data));
        let bytes = bincode::serialize(&envelope).unwrap();
        match bincode::deserialize::<IpcEnvelope<[i32; 5]>>(bytes.as_slice()).unwrap() {
            IpcEnvelope::UserData(Envelope::Message(recv_data)) => assert_eq!(data, recv_data),
            _ => panic!("Wrong deserialization."),
        }
    }

    #[test]
    fn custom_channel() {
        let (sender, receiver) = ipc_channel().unwrap();
        let data = vec![1, 2, 3, 5, 4];
        sender.send(data.clone()).unwrap();
        assert_eq!(
            receiver.recv().unwrap().content().as_slice(),
            data.as_slice()
        );
    }
}

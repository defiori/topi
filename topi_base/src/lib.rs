// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

pub mod messaging;
pub mod nodes;
pub mod process;
#[cfg(feature = "test-mode")]
pub mod test_utils;
pub mod thread;
pub use serde;
#[cfg(feature = "data")]
pub mod data;
#[cfg(feature = "data")]
pub use ndarray;
#[cfg(feature = "data")]
pub use sharify;

use std::collections::HashMap;
use std::fmt::Debug;
use std::io;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
enum Status {
    Running,
    Success,
    Failure,
}

#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    DuplicateNodeName(String),
    IoError(io::Error),
    UnknownSendDestination(String),
    DuplicateProcessName(String),
    TopiProcInitFailure(String),
    TopiProcUnexpectedInitMessage,
    CrossbeamSendError,
    IpcError(ipc_channel::ErrorKind),
    IpcRecvError(ipc_channel::ipc::IpcError),
    CrossbeamRecvError(crossbeam_channel::RecvError),
    CrossbeamTryRecvError(crossbeam_channel::TryRecvError),
    ThreadFailure(Vec<String>),
    ProcessFailure(HashMap<String, Option<Vec<String>>>),
    UnusableNodeConfig(String),
    Sharify(sharify::Error),
    Interrupted,
    ControllerThreadPanic,
    MultipleNodesAsMainThread(Vec<String>),
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Error {
        Error::IoError(error)
    }
}

impl From<ipc_channel::Error> for Error {
    fn from(error: ipc_channel::Error) -> Error {
        Error::IpcError(*error)
    }
}

impl From<ipc_channel::ipc::IpcError> for Error {
    fn from(error: ipc_channel::ipc::IpcError) -> Error {
        Error::IpcRecvError(error)
    }
}

impl<M> From<crossbeam_channel::SendError<M>> for Error {
    fn from(_error: crossbeam_channel::SendError<M>) -> Error {
        Error::CrossbeamSendError
    }
}

impl From<crossbeam_channel::RecvError> for Error {
    fn from(error: crossbeam_channel::RecvError) -> Error {
        Error::CrossbeamRecvError(error)
    }
}

impl From<crossbeam_channel::TryRecvError> for Error {
    fn from(error: crossbeam_channel::TryRecvError) -> Error {
        Error::CrossbeamTryRecvError(error)
    }
}

#[cfg(feature = "data")]
impl From<sharify::Error> for Error {
    fn from(error: sharify::Error) -> Error {
        Error::Sharify(error)
    }
}

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

#![allow(dead_code)]

use crate::{
    data::{
        Content, DataMessage, DataMessenger, SharedArray, SharedArrayMut, SharedSlice,
        SharedSliceMut,
    },
    messaging::Envelope,
    Error,
};
use ndarray::{self, IxDyn};
use serde::{Deserialize, Serialize};
use std::convert::{From, TryFrom};
use std::ffi::CStr;
use std::os::raw::{c_char, c_uint, c_ushort, c_void};
use std::str::Utf8Error;

pub type Message = DataMessage<(), Metadata>;
pub type Messenger = DataMessenger<(), Metadata>;

#[derive(Serialize, Deserialize)]
pub enum Metadata {
    String(String),
    Bytes(Vec<u8>),
    StringAndBytes(String, Vec<u8>),
}

impl TryFrom<(Option<String>, Option<Vec<u8>>)> for Metadata {
    type Error = ();

    fn try_from(meta_tuple: (Option<String>, Option<Vec<u8>>)) -> Result<Self, Self::Error> {
        let (string, bytes) = meta_tuple;
        match string {
            Some(string) => match bytes {
                Some(bytes) => Ok(Metadata::StringAndBytes(string, bytes)),
                None => Ok(Metadata::String(string)),
            },
            None => match bytes {
                Some(bytes) => Ok(Metadata::Bytes(bytes)),
                None => Err(()),
            },
        }
    }
}

enum ReturnCode {
    Ok,
    NullPointer,
    InvalidUtf8,
    TopiError,
    InvalidAccess,
}

impl From<ReturnCode> for c_uint {
    fn from(code: ReturnCode) -> Self {
        match code {
            ReturnCode::Ok => 0,
            ReturnCode::NullPointer => 1,
            ReturnCode::InvalidUtf8 => 2,
            ReturnCode::TopiError => 3,
            ReturnCode::InvalidAccess => 4,
        }
    }
}

impl From<Utf8Error> for ReturnCode {
    fn from(_: Utf8Error) -> Self {
        ReturnCode::InvalidUtf8
    }
}

impl<T> From<Result<T, Error>> for ReturnCode {
    fn from(result: Result<T, Error>) -> Self {
        match result {
            Ok(_) => ReturnCode::Ok,
            Err(_) => ReturnCode::TopiError,
        }
    }
}

#[allow(clippy::type_complexity)]
#[repr(C)]
pub struct Callbacks {
    message_recv: Option<
        extern "C" fn(
            messenger: Option<&Messenger>,
            terminate: Option<&mut bool>,
            msg: Option<&mut MessageC>,
        ) -> c_uint,
    >,
    message_send_array: Option<
        extern "C" fn(
            messenger: Option<&Messenger>,
            to: *const c_char,
            array: Option<Box<ArrayC>>,
            metadata: Option<Box<Metadata>>,
        ) -> c_uint,
    >,
    message_send_string: Option<
        extern "C" fn(
            messenger: Option<&Messenger>,
            to: *const c_char,
            string: Option<Box<String>>,
            metadata: Option<Box<Metadata>>,
        ) -> c_uint,
    >,
    message_send_bytes: Option<
        extern "C" fn(
            messenger: Option<&Messenger>,
            to: *const c_char,
            bytes: Option<Box<BytesC>>,
            metadata: Option<Box<Metadata>>,
        ) -> c_uint,
    >,
    array_create: Option<
        extern "C" fn(
            array: Option<&mut *mut ArrayC>,
            array_type: c_ushort,
            elem_type: c_ushort,
            n_dims: c_uint,
            shape: *const c_uint,
        ) -> c_uint,
    >,
    array_free: Option<extern "C" fn(array: Option<Box<ArrayC>>) -> c_uint>,
    array_type: Option<
        extern "C" fn(
            array: Option<&ArrayC>,
            array_type: Option<&mut c_ushort>,
            elem_type: Option<&mut c_ushort>,
        ) -> c_uint,
    >,
    array_n_elements:
        Option<extern "C" fn(array: Option<&ArrayC>, n_elements: Option<&mut c_uint>) -> c_uint>,
    array_view_mut:
        Option<extern "C" fn(array: Option<&mut ArrayC>, data: Option<&mut *mut c_void>) -> c_uint>,
}

#[allow(clippy::new_without_default)]
impl Callbacks {
    pub fn new() -> Self {
        Callbacks {
            message_recv: Some(message_recv),
            message_send_array: Some(message_send_array),
            message_send_string: Some(message_send_string),
            message_send_bytes: Some(message_send_bytes),
            array_create: Some(array_create),
            array_free: Some(array_free),
            array_type: Some(array_type),
            array_n_elements: Some(array_n_elements),
            array_view_mut: Some(array_view_mut),
        }
    }
}

#[repr(C)]
struct MessageC {
    metadata: *mut Metadata,
    data_type: c_ushort,
    data: Data,
}

#[repr(C)]
union Data {
    array: *mut ArrayC,
    string: *mut String,
    bytes: *mut BytesC,
}

impl From<DataMessage<(), Metadata>> for MessageC {
    fn from(msg: DataMessage<(), Metadata>) -> Self {
        let DataMessage { content, metadata } = msg;
        let metadata = if let Some(metadata) = metadata {
            let boxed = Box::new(metadata);
            Box::into_raw(boxed)
        } else {
            std::ptr::null_mut()
        };
        let (data_type, data) = content.into_data();
        MessageC {
            metadata,
            data_type,
            data,
        }
    }
}

impl Content<()> {
    fn into_data(self) -> (c_ushort, Data) {
        use Content::*;
        // TODO: replace this mess with constants that work across
        // the FFI boundary
        match self {
            // Arrays
            ArrayMutBool(_)
            | SharedArrayBool(_)
            | SharedArrayMutBool(_)
            | ArrayMutUInt8(_)
            | SharedArrayUInt8(_)
            | SharedArrayMutUInt8(_)
            | ArrayMutUInt16(_)
            | SharedArrayUInt16(_)
            | SharedArrayMutUInt16(_)
            | ArrayMutUInt32(_)
            | SharedArrayUInt32(_)
            | SharedArrayMutUInt32(_)
            | ArrayMutUInt64(_)
            | SharedArrayUInt64(_)
            | SharedArrayMutUInt64(_)
            | ArrayMutInt8(_)
            | SharedArrayInt8(_)
            | SharedArrayMutInt8(_)
            | ArrayMutInt16(_)
            | SharedArrayInt16(_)
            | SharedArrayMutInt16(_)
            | ArrayMutInt32(_)
            | SharedArrayInt32(_)
            | SharedArrayMutInt32(_)
            | ArrayMutInt64(_)
            | SharedArrayInt64(_)
            | SharedArrayMutInt64(_)
            | ArrayMutFloat32(_)
            | SharedArrayFloat32(_)
            | SharedArrayMutFloat32(_)
            | ArrayMutFloat64(_)
            | SharedArrayFloat64(_)
            | SharedArrayMutFloat64(_) => {
                let data: Box<ArrayC> = Box::new(self.into());
                (
                    1,
                    Data {
                        array: Box::into_raw(data),
                    },
                )
            }
            // Immutable string
            String(s) => {
                let data: Box<std::string::String> = Box::new(s);
                (
                    2,
                    Data {
                        string: Box::into_raw(data),
                    },
                )
            }
            // Bytes
            Bytes(_) | SharedBytes(_) | SharedBytesMut(_) => {
                let data: Box<BytesC> = Box::new(self.into());
                (
                    3,
                    Data {
                        bytes: Box::into_raw(data),
                    },
                )
            }
            // User defined
            UserData(_) => panic!("Content::UserData is not part of the C API."),
        }
    }
}

enum ArrayC {
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
}

impl ArrayC {
    fn to_tags(&self) -> (c_ushort, c_ushort) {
        use ArrayC::*;
        // TODO: replace this mess with constants that work across
        // the FFI boundary
        match self {
            // Boolean arrays
            ArrayMutBool(_) => (1, 1),
            SharedArrayBool(_) => (2, 1),
            SharedArrayMutBool(_) => (3, 1),
            // Arrays u8
            ArrayMutUInt8(_) => (1, 2),
            SharedArrayUInt8(_) => (2, 2),
            SharedArrayMutUInt8(_) => (3, 2),
            // Arrays u16
            ArrayMutUInt16(_) => (1, 3),
            SharedArrayUInt16(_) => (2, 3),
            SharedArrayMutUInt16(_) => (3, 3),
            // Arrays u32
            ArrayMutUInt32(_) => (1, 4),
            SharedArrayUInt32(_) => (2, 4),
            SharedArrayMutUInt32(_) => (3, 4),
            // Arrays u64
            ArrayMutUInt64(_) => (1, 5),
            SharedArrayUInt64(_) => (2, 5),
            SharedArrayMutUInt64(_) => (3, 5),
            // Arrays i8
            ArrayMutInt8(_) => (1, 6),
            SharedArrayInt8(_) => (2, 6),
            SharedArrayMutInt8(_) => (3, 6),
            // Arrays i16
            ArrayMutInt16(_) => (1, 7),
            SharedArrayInt16(_) => (2, 7),
            SharedArrayMutInt16(_) => (3, 7),
            // Arrays i32
            ArrayMutInt32(_) => (1, 8),
            SharedArrayInt32(_) => (2, 8),
            SharedArrayMutInt32(_) => (3, 8),
            // Arrays i64
            ArrayMutInt64(_) => (1, 9),
            SharedArrayInt64(_) => (2, 9),
            SharedArrayMutInt64(_) => (3, 9),
            // Arrays f32
            ArrayMutFloat32(_) => (1, 10),
            SharedArrayFloat32(_) => (2, 10),
            SharedArrayMutFloat32(_) => (3, 10),
            // Arrays f64
            ArrayMutFloat64(_) => (1, 11),
            SharedArrayFloat64(_) => (2, 11),
            SharedArrayMutFloat64(_) => (3, 11),
        }
    }

    fn from_tags(
        array_type: c_ushort,
        elem_type: c_ushort,
        shape: &[usize],
    ) -> Result<ArrayC, Error> {
        use ArrayC::*;
        match array_type {
            1 => match elem_type {
                1 => Ok(ArrayMutBool(ndarray::Array::from_elem(
                    ndarray::IxDyn(shape),
                    false,
                ))),
                2 => Ok(ArrayMutUInt8(ndarray::Array::from_elem(
                    ndarray::IxDyn(shape),
                    0,
                ))),
                3 => Ok(ArrayMutUInt16(ndarray::Array::from_elem(
                    ndarray::IxDyn(shape),
                    0,
                ))),
                4 => Ok(ArrayMutUInt32(ndarray::Array::from_elem(
                    ndarray::IxDyn(shape),
                    0,
                ))),
                5 => Ok(ArrayMutUInt64(ndarray::Array::from_elem(
                    ndarray::IxDyn(shape),
                    0,
                ))),
                6 => Ok(ArrayMutInt8(ndarray::Array::from_elem(
                    ndarray::IxDyn(shape),
                    0,
                ))),
                7 => Ok(ArrayMutInt16(ndarray::Array::from_elem(
                    ndarray::IxDyn(shape),
                    0,
                ))),
                8 => Ok(ArrayMutInt32(ndarray::Array::from_elem(
                    ndarray::IxDyn(shape),
                    0,
                ))),
                9 => Ok(ArrayMutInt64(ndarray::Array::from_elem(
                    ndarray::IxDyn(shape),
                    0,
                ))),
                10 => Ok(ArrayMutFloat32(ndarray::Array::from_elem(
                    ndarray::IxDyn(shape),
                    0.0,
                ))),
                11 => Ok(ArrayMutFloat64(ndarray::Array::from_elem(
                    ndarray::IxDyn(shape),
                    0.0,
                ))),
                _ => panic!("Unknown tag for element type '{}'.", elem_type),
            },
            2 => match elem_type {
                1 => Ok(SharedArrayBool(SharedArray::new(&(
                    false,
                    ndarray::IxDyn(shape),
                ))?)),
                2 => Ok(SharedArrayUInt8(SharedArray::new(&(
                    0,
                    ndarray::IxDyn(shape),
                ))?)),
                3 => Ok(SharedArrayUInt16(SharedArray::new(&(
                    0,
                    ndarray::IxDyn(shape),
                ))?)),
                4 => Ok(SharedArrayUInt32(SharedArray::new(&(
                    0,
                    ndarray::IxDyn(shape),
                ))?)),
                5 => Ok(SharedArrayUInt64(SharedArray::new(&(
                    0,
                    ndarray::IxDyn(shape),
                ))?)),
                6 => Ok(SharedArrayInt8(SharedArray::new(&(
                    0,
                    ndarray::IxDyn(shape),
                ))?)),
                7 => Ok(SharedArrayInt16(SharedArray::new(&(
                    0,
                    ndarray::IxDyn(shape),
                ))?)),
                8 => Ok(SharedArrayInt32(SharedArray::new(&(
                    0,
                    ndarray::IxDyn(shape),
                ))?)),
                9 => Ok(SharedArrayInt64(SharedArray::new(&(
                    0,
                    ndarray::IxDyn(shape),
                ))?)),
                10 => Ok(SharedArrayFloat32(SharedArray::new(&(
                    0.0,
                    ndarray::IxDyn(shape),
                ))?)),
                11 => Ok(SharedArrayFloat64(SharedArray::new(&(
                    0.0,
                    ndarray::IxDyn(shape),
                ))?)),
                _ => panic!("Unknown tag for element type '{}'.", elem_type),
            },
            3 => match elem_type {
                1 => Ok(SharedArrayMutBool(SharedArrayMut::new(&(
                    false,
                    ndarray::IxDyn(shape),
                ))?)),
                2 => Ok(SharedArrayMutUInt8(SharedArrayMut::new(&(
                    0,
                    ndarray::IxDyn(shape),
                ))?)),
                3 => Ok(SharedArrayMutUInt16(SharedArrayMut::new(&(
                    0,
                    ndarray::IxDyn(shape),
                ))?)),
                4 => Ok(SharedArrayMutUInt32(SharedArrayMut::new(&(
                    0,
                    ndarray::IxDyn(shape),
                ))?)),
                5 => Ok(SharedArrayMutUInt64(SharedArrayMut::new(&(
                    0,
                    ndarray::IxDyn(shape),
                ))?)),
                6 => Ok(SharedArrayMutInt8(SharedArrayMut::new(&(
                    0,
                    ndarray::IxDyn(shape),
                ))?)),
                7 => Ok(SharedArrayMutInt16(SharedArrayMut::new(&(
                    0,
                    ndarray::IxDyn(shape),
                ))?)),
                8 => Ok(SharedArrayMutInt32(SharedArrayMut::new(&(
                    0,
                    ndarray::IxDyn(shape),
                ))?)),
                9 => Ok(SharedArrayMutInt64(SharedArrayMut::new(&(
                    0,
                    ndarray::IxDyn(shape),
                ))?)),
                10 => Ok(SharedArrayMutFloat32(SharedArrayMut::new(&(
                    0.0,
                    ndarray::IxDyn(shape),
                ))?)),
                11 => Ok(SharedArrayMutFloat64(SharedArrayMut::new(&(
                    0.0,
                    ndarray::IxDyn(shape),
                ))?)),
                _ => panic!("Unknown tag for element type '{}'.", elem_type),
            },
            _ => panic!("Unknown tag for array type '{}'.", array_type),
        }
    }

    fn send(self, messenger: &Messenger, recv_id: &str, metadata: Option<Metadata>) -> ReturnCode {
        use ArrayC::*;
        match self {
            // Boolean arrays
            ArrayMutBool(arr) => messenger.send(recv_id, arr, metadata).into(),
            SharedArrayBool(arr) => messenger.send(recv_id, arr, metadata).into(),
            SharedArrayMutBool(arr) => messenger.send(recv_id, arr, metadata).into(),
            // Arrays u8
            ArrayMutUInt8(arr) => messenger.send(recv_id, arr, metadata).into(),
            SharedArrayUInt8(arr) => messenger.send(recv_id, arr, metadata).into(),
            SharedArrayMutUInt8(arr) => messenger.send(recv_id, arr, metadata).into(),
            // Arrays u16
            ArrayMutUInt16(arr) => messenger.send(recv_id, arr, metadata).into(),
            SharedArrayUInt16(arr) => messenger.send(recv_id, arr, metadata).into(),
            SharedArrayMutUInt16(arr) => messenger.send(recv_id, arr, metadata).into(),
            // Arrays u32
            ArrayMutUInt32(arr) => messenger.send(recv_id, arr, metadata).into(),
            SharedArrayUInt32(arr) => messenger.send(recv_id, arr, metadata).into(),
            SharedArrayMutUInt32(arr) => messenger.send(recv_id, arr, metadata).into(),
            // Arrays u64
            ArrayMutUInt64(arr) => messenger.send(recv_id, arr, metadata).into(),
            SharedArrayUInt64(arr) => messenger.send(recv_id, arr, metadata).into(),
            SharedArrayMutUInt64(arr) => messenger.send(recv_id, arr, metadata).into(),
            // Arrays i8
            ArrayMutInt8(arr) => messenger.send(recv_id, arr, metadata).into(),
            SharedArrayInt8(arr) => messenger.send(recv_id, arr, metadata).into(),
            SharedArrayMutInt8(arr) => messenger.send(recv_id, arr, metadata).into(),
            // Arrays i16
            ArrayMutInt16(arr) => messenger.send(recv_id, arr, metadata).into(),
            SharedArrayInt16(arr) => messenger.send(recv_id, arr, metadata).into(),
            SharedArrayMutInt16(arr) => messenger.send(recv_id, arr, metadata).into(),
            // Arrays i32
            ArrayMutInt32(arr) => messenger.send(recv_id, arr, metadata).into(),
            SharedArrayInt32(arr) => messenger.send(recv_id, arr, metadata).into(),
            SharedArrayMutInt32(arr) => messenger.send(recv_id, arr, metadata).into(),
            // Arrays i64
            ArrayMutInt64(arr) => messenger.send(recv_id, arr, metadata).into(),
            SharedArrayInt64(arr) => messenger.send(recv_id, arr, metadata).into(),
            SharedArrayMutInt64(arr) => messenger.send(recv_id, arr, metadata).into(),
            // Arrays f32
            ArrayMutFloat32(arr) => messenger.send(recv_id, arr, metadata).into(),
            SharedArrayFloat32(arr) => messenger.send(recv_id, arr, metadata).into(),
            SharedArrayMutFloat32(arr) => messenger.send(recv_id, arr, metadata).into(),
            // Arrays f64
            ArrayMutFloat64(arr) => messenger.send(recv_id, arr, metadata).into(),
            SharedArrayFloat64(arr) => messenger.send(recv_id, arr, metadata).into(),
            SharedArrayMutFloat64(arr) => messenger.send(recv_id, arr, metadata).into(),
        }
    }

    fn is_mut(&self) -> bool {
        use ArrayC::*;
        matches!(
            self,
            ArrayMutBool(_)
                | SharedArrayMutBool(_)
                | ArrayMutUInt8(_)
                | SharedArrayMutUInt8(_)
                | ArrayMutUInt16(_)
                | SharedArrayMutUInt16(_)
                | ArrayMutUInt32(_)
                | SharedArrayMutUInt32(_)
                | ArrayMutUInt64(_)
                | SharedArrayMutUInt64(_)
                | ArrayMutInt8(_)
                | SharedArrayMutInt8(_)
                | ArrayMutInt16(_)
                | SharedArrayMutInt16(_)
                | ArrayMutInt32(_)
                | SharedArrayMutInt32(_)
                | ArrayMutInt64(_)
                | SharedArrayMutInt64(_)
                | ArrayMutFloat32(_)
                | SharedArrayMutFloat32(_)
                | ArrayMutFloat64(_)
                | SharedArrayMutFloat64(_)
        )
    }

    fn n_elements(&self) -> c_uint {
        use ArrayC::*;
        let len = match self {
            // Boolean arrays
            ArrayMutBool(arr) => arr.len(),
            SharedArrayBool(arr) => arr.as_view().len(),
            SharedArrayMutBool(arr) => arr.as_view().len(),
            // Arrays u8
            ArrayMutUInt8(arr) => arr.len(),
            SharedArrayUInt8(arr) => arr.as_view().len(),
            SharedArrayMutUInt8(arr) => arr.as_view().len(),
            // Arrays u16
            ArrayMutUInt16(arr) => arr.len(),
            SharedArrayUInt16(arr) => arr.as_view().len(),
            SharedArrayMutUInt16(arr) => arr.as_view().len(),
            // Arrays u32
            ArrayMutUInt32(arr) => arr.len(),
            SharedArrayUInt32(arr) => arr.as_view().len(),
            SharedArrayMutUInt32(arr) => arr.as_view().len(),
            // Arrays u64
            ArrayMutUInt64(arr) => arr.len(),
            SharedArrayUInt64(arr) => arr.as_view().len(),
            SharedArrayMutUInt64(arr) => arr.as_view().len(),
            // Arrays i8
            ArrayMutInt8(arr) => arr.len(),
            SharedArrayInt8(arr) => arr.as_view().len(),
            SharedArrayMutInt8(arr) => arr.as_view().len(),
            // Arrays i16
            ArrayMutInt16(arr) => arr.len(),
            SharedArrayInt16(arr) => arr.as_view().len(),
            SharedArrayMutInt16(arr) => arr.as_view().len(),
            // Arrays i32
            ArrayMutInt32(arr) => arr.len(),
            SharedArrayInt32(arr) => arr.as_view().len(),
            SharedArrayMutInt32(arr) => arr.as_view().len(),
            // Arrays i64
            ArrayMutInt64(arr) => arr.len(),
            SharedArrayInt64(arr) => arr.as_view().len(),
            SharedArrayMutInt64(arr) => arr.as_view().len(),
            // Arrays f32
            ArrayMutFloat32(arr) => arr.len(),
            SharedArrayFloat32(arr) => arr.as_view().len(),
            SharedArrayMutFloat32(arr) => arr.as_view().len(),
            // Arrays f64
            ArrayMutFloat64(arr) => arr.len(),
            SharedArrayFloat64(arr) => arr.as_view().len(),
            SharedArrayMutFloat64(arr) => arr.as_view().len(),
        };
        len as c_uint
    }

    fn view(&self) -> *const c_void {
        use ArrayC::*;
        match self {
            // Boolean arrays
            ArrayMutBool(arr) => arr.as_ptr() as *const c_void,
            SharedArrayBool(arr) => arr.as_view().as_ptr() as *const c_void,
            SharedArrayMutBool(arr) => arr.as_view().as_ptr() as *const c_void,
            // Arrays u8
            ArrayMutUInt8(arr) => arr.as_ptr() as *const c_void,
            SharedArrayUInt8(arr) => arr.as_view().as_ptr() as *const c_void,
            SharedArrayMutUInt8(arr) => arr.as_view().as_ptr() as *const c_void,
            // Arrays u16
            ArrayMutUInt16(arr) => arr.as_ptr() as *const c_void,
            SharedArrayUInt16(arr) => arr.as_view().as_ptr() as *const c_void,
            SharedArrayMutUInt16(arr) => arr.as_view().as_ptr() as *const c_void,
            // Arrays u32
            ArrayMutUInt32(arr) => arr.as_ptr() as *const c_void,
            SharedArrayUInt32(arr) => arr.as_view().as_ptr() as *const c_void,
            SharedArrayMutUInt32(arr) => arr.as_view().as_ptr() as *const c_void,
            // Arrays u64
            ArrayMutUInt64(arr) => arr.as_ptr() as *const c_void,
            SharedArrayUInt64(arr) => arr.as_view().as_ptr() as *const c_void,
            SharedArrayMutUInt64(arr) => arr.as_view().as_ptr() as *const c_void,
            // Arrays i8
            ArrayMutInt8(arr) => arr.as_ptr() as *const c_void,
            SharedArrayInt8(arr) => arr.as_view().as_ptr() as *const c_void,
            SharedArrayMutInt8(arr) => arr.as_view().as_ptr() as *const c_void,
            // Arrays i16
            ArrayMutInt16(arr) => arr.as_ptr() as *const c_void,
            SharedArrayInt16(arr) => arr.as_view().as_ptr() as *const c_void,
            SharedArrayMutInt16(arr) => arr.as_view().as_ptr() as *const c_void,
            // Arrays i32
            ArrayMutInt32(arr) => arr.as_ptr() as *const c_void,
            SharedArrayInt32(arr) => arr.as_view().as_ptr() as *const c_void,
            SharedArrayMutInt32(arr) => arr.as_view().as_ptr() as *const c_void,
            // Arrays i64
            ArrayMutInt64(arr) => arr.as_ptr() as *const c_void,
            SharedArrayInt64(arr) => arr.as_view().as_ptr() as *const c_void,
            SharedArrayMutInt64(arr) => arr.as_view().as_ptr() as *const c_void,
            // Arrays f32
            ArrayMutFloat32(arr) => arr.as_ptr() as *const c_void,
            SharedArrayFloat32(arr) => arr.as_view().as_ptr() as *const c_void,
            SharedArrayMutFloat32(arr) => arr.as_view().as_ptr() as *const c_void,
            // Arrays f64
            ArrayMutFloat64(arr) => arr.as_ptr() as *const c_void,
            SharedArrayFloat64(arr) => arr.as_view().as_ptr() as *const c_void,
            SharedArrayMutFloat64(arr) => arr.as_view().as_ptr() as *const c_void,
        }
    }

    fn view_mut(&mut self) -> Result<*mut c_void, ()> {
        use ArrayC::*;
        match self {
            // Boolean arrays
            ArrayMutBool(arr) => Ok(arr.as_mut_ptr() as *mut c_void),
            SharedArrayBool(_) => Err(()),
            SharedArrayMutBool(arr) => Ok(arr.as_view_mut().as_mut_ptr() as *mut c_void),
            // Arrays u8
            ArrayMutUInt8(arr) => Ok(arr.as_mut_ptr() as *mut c_void),
            SharedArrayUInt8(_) => Err(()),
            SharedArrayMutUInt8(arr) => Ok(arr.as_view_mut().as_mut_ptr() as *mut c_void),
            // Arrays u16
            ArrayMutUInt16(arr) => Ok(arr.as_mut_ptr() as *mut c_void),
            SharedArrayUInt16(_) => Err(()),
            SharedArrayMutUInt16(arr) => Ok(arr.as_view_mut().as_mut_ptr() as *mut c_void),
            // Arrays u32
            ArrayMutUInt32(arr) => Ok(arr.as_mut_ptr() as *mut c_void),
            SharedArrayUInt32(_) => Err(()),
            SharedArrayMutUInt32(arr) => Ok(arr.as_view_mut().as_mut_ptr() as *mut c_void),
            // Arrays u64
            ArrayMutUInt64(arr) => Ok(arr.as_mut_ptr() as *mut c_void),
            SharedArrayUInt64(_) => Err(()),
            SharedArrayMutUInt64(arr) => Ok(arr.as_view_mut().as_mut_ptr() as *mut c_void),
            // Arrays i8
            ArrayMutInt8(arr) => Ok(arr.as_mut_ptr() as *mut c_void),
            SharedArrayInt8(_) => Err(()),
            SharedArrayMutInt8(arr) => Ok(arr.as_view_mut().as_mut_ptr() as *mut c_void),
            // Arrays i16
            ArrayMutInt16(arr) => Ok(arr.as_mut_ptr() as *mut c_void),
            SharedArrayInt16(_) => Err(()),
            SharedArrayMutInt16(arr) => Ok(arr.as_view_mut().as_mut_ptr() as *mut c_void),
            // Arrays i32
            ArrayMutInt32(arr) => Ok(arr.as_mut_ptr() as *mut c_void),
            SharedArrayInt32(_) => Err(()),
            SharedArrayMutInt32(arr) => Ok(arr.as_view_mut().as_mut_ptr() as *mut c_void),
            // Arrays i64
            ArrayMutInt64(arr) => Ok(arr.as_mut_ptr() as *mut c_void),
            SharedArrayInt64(_) => Err(()),
            SharedArrayMutInt64(arr) => Ok(arr.as_view_mut().as_mut_ptr() as *mut c_void),
            // Arrays f32
            ArrayMutFloat32(arr) => Ok(arr.as_mut_ptr() as *mut c_void),
            SharedArrayFloat32(_) => Err(()),
            SharedArrayMutFloat32(arr) => Ok(arr.as_view_mut().as_mut_ptr() as *mut c_void),
            // Arrays f64
            ArrayMutFloat64(arr) => Ok(arr.as_mut_ptr() as *mut c_void),
            SharedArrayFloat64(_) => Err(()),
            SharedArrayMutFloat64(arr) => Ok(arr.as_view_mut().as_mut_ptr() as *mut c_void),
        }
    }
}

impl From<Content<()>> for ArrayC {
    fn from(content: Content<()>) -> Self {
        use Content::*;
        match content {
            // Boolean arrays
            ArrayMutBool(arr) => ArrayC::ArrayMutBool(arr),
            SharedArrayBool(arr) => ArrayC::SharedArrayBool(arr),
            SharedArrayMutBool(arr) => ArrayC::SharedArrayMutBool(arr),
            // Arrays u8
            ArrayMutUInt8(arr) => ArrayC::ArrayMutUInt8(arr),
            SharedArrayUInt8(arr) => ArrayC::SharedArrayUInt8(arr),
            SharedArrayMutUInt8(arr) => ArrayC::SharedArrayMutUInt8(arr),
            // Arrays u16
            ArrayMutUInt16(arr) => ArrayC::ArrayMutUInt16(arr),
            SharedArrayUInt16(arr) => ArrayC::SharedArrayUInt16(arr),
            SharedArrayMutUInt16(arr) => ArrayC::SharedArrayMutUInt16(arr),
            // Arrays u32
            ArrayMutUInt32(arr) => ArrayC::ArrayMutUInt32(arr),
            SharedArrayUInt32(arr) => ArrayC::SharedArrayUInt32(arr),
            SharedArrayMutUInt32(arr) => ArrayC::SharedArrayMutUInt32(arr),
            // Arrays u64
            ArrayMutUInt64(arr) => ArrayC::ArrayMutUInt64(arr),
            SharedArrayUInt64(arr) => ArrayC::SharedArrayUInt64(arr),
            SharedArrayMutUInt64(arr) => ArrayC::SharedArrayMutUInt64(arr),
            // Arrays i8
            ArrayMutInt8(arr) => ArrayC::ArrayMutInt8(arr),
            SharedArrayInt8(arr) => ArrayC::SharedArrayInt8(arr),
            SharedArrayMutInt8(arr) => ArrayC::SharedArrayMutInt8(arr),
            // Arrays i16
            ArrayMutInt16(arr) => ArrayC::ArrayMutInt16(arr),
            SharedArrayInt16(arr) => ArrayC::SharedArrayInt16(arr),
            SharedArrayMutInt16(arr) => ArrayC::SharedArrayMutInt16(arr),
            // Arrays i32
            ArrayMutInt32(arr) => ArrayC::ArrayMutInt32(arr),
            SharedArrayInt32(arr) => ArrayC::SharedArrayInt32(arr),
            SharedArrayMutInt32(arr) => ArrayC::SharedArrayMutInt32(arr),
            // Arrays i64
            ArrayMutInt64(arr) => ArrayC::ArrayMutInt64(arr),
            SharedArrayInt64(arr) => ArrayC::SharedArrayInt64(arr),
            SharedArrayMutInt64(arr) => ArrayC::SharedArrayMutInt64(arr),
            // Arrays f32
            ArrayMutFloat32(arr) => ArrayC::ArrayMutFloat32(arr),
            SharedArrayFloat32(arr) => ArrayC::SharedArrayFloat32(arr),
            SharedArrayMutFloat32(arr) => ArrayC::SharedArrayMutFloat32(arr),
            // Arrays f64
            ArrayMutFloat64(arr) => ArrayC::ArrayMutFloat64(arr),
            SharedArrayFloat64(arr) => ArrayC::SharedArrayFloat64(arr),
            SharedArrayMutFloat64(arr) => ArrayC::SharedArrayMutFloat64(arr),
            // Immutable string
            String(_) => panic!("Can't create Array from Content::String variant."),
            // Bytes
            Bytes(_) | SharedBytes(_) | SharedBytesMut(_) => {
                panic!("Can't create Array from Content::Bytes variants.")
            }
            // User defined
            UserData(_) => panic!("Content::UserData is not part of the C API."),
        }
    }
}

enum BytesC {
    Bytes(Vec<u8>),
    SharedBytes(SharedSlice<u8>),
    SharedBytesMut(SharedSliceMut<u8>),
}

impl From<Content<()>> for BytesC {
    fn from(content: Content<()>) -> Self {
        match content {
            Content::Bytes(bytes) => BytesC::Bytes(bytes),
            Content::SharedBytes(bytes) => BytesC::SharedBytes(bytes),
            Content::SharedBytesMut(bytes) => BytesC::SharedBytesMut(bytes),
            _ => panic!("Only Content::Bytes variants can be converted to Bytes."),
        }
    }
}

extern "C" fn message_recv(
    messenger: Option<&Messenger>,
    terminate: Option<&mut bool>,
    msg: Option<&mut MessageC>,
) -> c_uint {
    if let Some(messenger) = messenger {
        if let Ok(topi_msg) = messenger.recv() {
            match topi_msg {
                Envelope::Terminate => {
                    if let Some(term_ptr) = terminate {
                        *term_ptr = true;
                        ReturnCode::Ok
                    } else {
                        ReturnCode::NullPointer
                    }
                }
                Envelope::Message(topi_msg) => {
                    if let Some(msg_ptr) = msg {
                        *msg_ptr = topi_msg.into();
                        ReturnCode::Ok
                    } else {
                        ReturnCode::NullPointer
                    }
                }
            }
        } else {
            ReturnCode::TopiError
        }
    } else {
        ReturnCode::NullPointer
    }
    .into()
}

extern "C" fn message_send_array(
    messenger: Option<&Messenger>,
    to: *const c_char,
    array: Option<Box<ArrayC>>,
    metadata: Option<Box<Metadata>>,
) -> c_uint {
    if let Some(messenger) = messenger {
        if to.is_null() {
            ReturnCode::NullPointer
        } else if let Ok(recv_id) = unsafe { CStr::from_ptr(to).to_str() } {
            if let Some(array) = array {
                let metadata = if let Some(metadata) = metadata {
                    Some(*metadata)
                } else {
                    None
                };
                (*array).send(messenger, recv_id, metadata)
            } else {
                ReturnCode::NullPointer
            }
        } else {
            ReturnCode::InvalidUtf8
        }
    } else {
        ReturnCode::NullPointer
    }
    .into()
}

#[allow(unused_variables)]
extern "C" fn message_send_string(
    messenger: Option<&Messenger>,
    to: *const c_char,
    string: Option<Box<String>>,
    metadata: Option<Box<Metadata>>,
) -> c_uint {
    todo!()
}

#[allow(unused_variables)]
extern "C" fn message_send_bytes(
    messenger: Option<&Messenger>,
    to: *const c_char,
    bytes: Option<Box<BytesC>>,
    metadata: Option<Box<Metadata>>,
) -> c_uint {
    todo!()
}

// TODO: metadata functions

extern "C" fn array_create(
    array: Option<&mut *mut ArrayC>,
    array_type: c_ushort,
    elem_type: c_ushort,
    n_dims: c_uint,
    shape: *const c_uint,
) -> c_uint {
    if let Some(ptr) = array {
        if shape.is_null() {
            ReturnCode::NullPointer
        } else {
            let shape: Vec<_> = unsafe {
                let shape = std::slice::from_raw_parts(shape, n_dims as usize);
                shape.iter().map(|&x| x as usize).collect()
            };
            if let Ok(array) = ArrayC::from_tags(array_type, elem_type, shape.as_slice()) {
                let boxed = Box::new(array);
                *ptr = Box::into_raw(boxed);
                ReturnCode::Ok
            } else {
                ReturnCode::TopiError
            }
        }
    } else {
        ReturnCode::NullPointer
    }
    .into()
}

extern "C" fn array_free(array: Option<Box<ArrayC>>) -> c_uint {
    if let Some(_array) = array {
        ReturnCode::Ok
    } else {
        ReturnCode::NullPointer
    }
    .into()
}

extern "C" fn array_is_mut(array: Option<&ArrayC>, is_mut: Option<&mut bool>) -> c_uint {
    if let Some(array) = array {
        if let Some(ptr) = is_mut {
            *ptr = array.is_mut();
            ReturnCode::Ok
        } else {
            ReturnCode::NullPointer
        }
    } else {
        ReturnCode::NullPointer
    }
    .into()
}

extern "C" fn array_type(
    array: Option<&ArrayC>,
    array_type: Option<&mut c_ushort>,
    elem_type: Option<&mut c_ushort>,
) -> c_uint {
    if let Some(array) = array {
        if let Some(array_ty_ptr) = array_type {
            if let Some(elem_ty_ptr) = elem_type {
                let (array_type, elem_type) = array.to_tags();
                *array_ty_ptr = array_type;
                *elem_ty_ptr = elem_type;
                ReturnCode::Ok
            } else {
                ReturnCode::NullPointer
            }
        } else {
            ReturnCode::NullPointer
        }
    } else {
        ReturnCode::NullPointer
    }
    .into()
}

extern "C" fn array_n_elements(array: Option<&ArrayC>, n_elements: Option<&mut c_uint>) -> c_uint {
    if let Some(array) = array {
        if let Some(ptr) = n_elements {
            *ptr = array.n_elements();
            ReturnCode::Ok
        } else {
            ReturnCode::NullPointer
        }
    } else {
        ReturnCode::NullPointer
    }
    .into()
}

// TODO: use macros for array_n_dims ... array_strides
// TODO: array_view

extern "C" fn array_view_mut(array: Option<&mut ArrayC>, data: Option<&mut *mut c_void>) -> c_uint {
    if let Some(array) = array {
        if let Some(data_ptr) = data {
            if let Ok(array_ptr) = array.view_mut() {
                *data_ptr = array_ptr;
                ReturnCode::Ok
            } else {
                ReturnCode::InvalidAccess
            }
        } else {
            ReturnCode::NullPointer
        }
    } else {
        ReturnCode::NullPointer
    }
    .into()
}

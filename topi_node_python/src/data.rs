// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use numpy::{self, IntoPyArray, ToNpyDims};
use pyo3::{
    exceptions::{PyException, PySystemExit},
    prelude::*,
};
use std::convert::TryInto;
use topi_base::{
    data::{
        c_api::{Message, Messenger, Metadata},
        Content, DataMessage,
    },
    messaging::Envelope,
    ndarray, sharify, Error,
};
use topi_internal_macros::{py_concretify, py_send_topitype};

#[pyclass(unsendable)]
pub struct _Messenger {
    inner: *const Messenger,
}

impl _Messenger {
    pub fn from_messenger(messenger: *const Messenger) -> Self {
        Self { inner: messenger }
    }
}

#[pymethods]
impl _Messenger {
    fn send(
        &self,
        recv_id: &str,
        object: TopiType,
        meta_string: Option<String>,
        meta_bytes: Option<Vec<u8>>,
    ) -> PyResult<()> {
        let messenger = unsafe { self.inner.as_ref().unwrap() };
        let metadata: Option<Metadata> = (meta_string, meta_bytes).try_into().ok();
        py_send_topitype!()
    }

    fn recv(&self, py: Python) -> PyResult<(PyObject, PyObject)> {
        let messenger = unsafe { self.inner.as_ref().unwrap() };
        process_message(py, messenger.recv().map_err(topi_base_err_to_py_err)?)
    }

    fn try_recv(&self, py: Python) -> PyResult<(Option<PyObject>, Option<PyObject>)> {
        let messenger = unsafe { self.inner.as_ref().unwrap() };
        if let Some(msg) = messenger.try_recv().map_err(topi_base_err_to_py_err)? {
            let (content, metadata) = process_message(py, msg)?;
            Ok((Some(content), Some(metadata)))
        } else {
            Ok((None, None))
        }
    }
}

fn topi_base_err_to_py_err(err: Error) -> PyErr {
    PyException::new_err(format!("{:?}", err))
}

fn process_message(py: Python, msg: Envelope<Message>) -> PyResult<(PyObject, PyObject)> {
    match msg {
        Envelope::Terminate => Err(PySystemExit::new_err(
            "Node has received termination signal.",
        )),
        Envelope::Message(DataMessage { content, metadata }) => {
            let metadata: Option<PyObject> = if let Some(metadata) = metadata {
                match metadata {
                    Metadata::String(string) => Some(string.into_py(py)),
                    Metadata::Bytes(bytes) => Some(bytes.into_py(py)),
                    Metadata::StringAndBytes(string, bytes) => Some((string, bytes).into_py(py)),
                }
            } else {
                None
            };
            use Content::*;
            match content {
                ArrayMutBool(arr) => Ok((arr.into_pyarray(py).into_py(py), metadata.into_py(py))),
                SharedArrayBool(_arr) => todo!(),
                SharedArrayMutBool(arr) => Ok((
                    _SharedArrayMut_bool::new_from_shared(arr)?.into_py(py),
                    metadata.into_py(py),
                )),
                // Arrays u8
                ArrayMutUInt8(arr) => Ok((arr.into_pyarray(py).into_py(py), metadata.into_py(py))),
                SharedArrayUInt8(_arr) => todo!(),
                SharedArrayMutUInt8(arr) => Ok((
                    _SharedArrayMut_u8::new_from_shared(arr)?.into_py(py),
                    metadata.into_py(py),
                )),
                // Arrays u16
                ArrayMutUInt16(arr) => Ok((arr.into_pyarray(py).into_py(py), metadata.into_py(py))),
                SharedArrayUInt16(_arr) => todo!(),
                SharedArrayMutUInt16(arr) => Ok((
                    _SharedArrayMut_u16::new_from_shared(arr)?.into_py(py),
                    metadata.into_py(py),
                )),
                // Arrays u32
                ArrayMutUInt32(arr) => Ok((arr.into_pyarray(py).into_py(py), metadata.into_py(py))),
                SharedArrayUInt32(_arr) => todo!(),
                SharedArrayMutUInt32(arr) => Ok((
                    _SharedArrayMut_u32::new_from_shared(arr)?.into_py(py),
                    metadata.into_py(py),
                )),
                // Arrays u64
                ArrayMutUInt64(arr) => Ok((arr.into_pyarray(py).into_py(py), metadata.into_py(py))),
                SharedArrayUInt64(_arr) => todo!(),
                SharedArrayMutUInt64(arr) => Ok((
                    _SharedArrayMut_u64::new_from_shared(arr)?.into_py(py),
                    metadata.into_py(py),
                )),
                // Arrays i8
                ArrayMutInt8(arr) => Ok((arr.into_pyarray(py).into_py(py), metadata.into_py(py))),
                SharedArrayInt8(_arr) => todo!(),
                SharedArrayMutInt8(arr) => Ok((
                    _SharedArrayMut_i8::new_from_shared(arr)?.into_py(py),
                    metadata.into_py(py),
                )),
                // Arrays i16
                ArrayMutInt16(arr) => Ok((arr.into_pyarray(py).into_py(py), metadata.into_py(py))),
                SharedArrayInt16(_arr) => todo!(),
                SharedArrayMutInt16(arr) => Ok((
                    _SharedArrayMut_i16::new_from_shared(arr)?.into_py(py),
                    metadata.into_py(py),
                )),
                // Arrays i32
                ArrayMutInt32(arr) => Ok((arr.into_pyarray(py).into_py(py), metadata.into_py(py))),
                SharedArrayInt32(_arr) => todo!(),
                SharedArrayMutInt32(arr) => Ok((
                    _SharedArrayMut_i32::new_from_shared(arr)?.into_py(py),
                    metadata.into_py(py),
                )),
                // Arrays i64
                ArrayMutInt64(arr) => Ok((arr.into_pyarray(py).into_py(py), metadata.into_py(py))),
                SharedArrayInt64(_arr) => todo!(),
                SharedArrayMutInt64(arr) => Ok((
                    _SharedArrayMut_i64::new_from_shared(arr)?.into_py(py),
                    metadata.into_py(py),
                )),
                // Arrays f32
                ArrayMutFloat32(arr) => {
                    Ok((arr.into_pyarray(py).into_py(py), metadata.into_py(py)))
                }
                SharedArrayFloat32(_arr) => todo!(),
                SharedArrayMutFloat32(arr) => Ok((
                    _SharedArrayMut_f32::new_from_shared(arr)?.into_py(py),
                    metadata.into_py(py),
                )),
                // Arrays f64
                ArrayMutFloat64(arr) => {
                    Ok((arr.into_pyarray(py).into_py(py), metadata.into_py(py)))
                }
                SharedArrayFloat64(_arr) => todo!(),
                SharedArrayMutFloat64(arr) => Ok((
                    _SharedArrayMut_f64::new_from_shared(arr)?.into_py(py),
                    metadata.into_py(py),
                )),
                // Immutable string
                String(s) => Ok((s.into_py(py), metadata.into_py(py))),
                // Bytes
                Bytes(bytes) => Ok((bytes.into_py(py), metadata.into_py(py))),
                SharedBytes(_bytes) => todo!(),
                SharedBytesMut(_bytes) => todo!(),
                // User defined
                UserData(_) => panic!("Content::UserData is not part of the data node API."),
                _ => panic!("Unexpected Content variant."),
            }
        }
    }
}

#[derive(FromPyObject)]
enum TopiType<'a> {
    NumpyArrayBool(numpy::PyReadonlyArrayDyn<'a, bool>),
    NumpyArrayUInt8(numpy::PyReadonlyArrayDyn<'a, u8>),
    NumpyArrayUInt16(numpy::PyReadonlyArrayDyn<'a, u16>),
    NumpyArrayUInt32(numpy::PyReadonlyArrayDyn<'a, u32>),
    NumpyArrayUInt64(numpy::PyReadonlyArrayDyn<'a, u64>),
    NumpyArrayInt8(numpy::PyReadonlyArrayDyn<'a, i8>),
    NumpyArrayInt16(numpy::PyReadonlyArrayDyn<'a, i16>),
    NumpyArrayInt32(numpy::PyReadonlyArrayDyn<'a, i32>),
    NumpyArrayInt64(numpy::PyReadonlyArrayDyn<'a, i64>),
    NumpyArrayFloat32(numpy::PyReadonlyArrayDyn<'a, f32>),
    NumpyArrayFloat64(numpy::PyReadonlyArrayDyn<'a, f64>),
    SharedArrayMutBool(PyRefMut<'a, _SharedArrayMut_bool>),
    SharedArrayMutUInt8(PyRefMut<'a, _SharedArrayMut_u8>),
    SharedArrayMutUInt16(PyRefMut<'a, _SharedArrayMut_u16>),
    SharedArrayMutUInt32(PyRefMut<'a, _SharedArrayMut_u32>),
    SharedArrayMutUInt64(PyRefMut<'a, _SharedArrayMut_u64>),
    SharedArrayMutInt8(PyRefMut<'a, _SharedArrayMut_i8>),
    SharedArrayMutInt16(PyRefMut<'a, _SharedArrayMut_i16>),
    SharedArrayMutInt32(PyRefMut<'a, _SharedArrayMut_i32>),
    SharedArrayMutInt64(PyRefMut<'a, _SharedArrayMut_i64>),
    SharedArrayMutFloat32(PyRefMut<'a, _SharedArrayMut_f32>),
    SharedArrayMutFloat64(PyRefMut<'a, _SharedArrayMut_f64>),
    String(&'a str),
    // TODO: Bytes
}

#[py_concretify(bool, u8, u16, u32, u64, i8, i16, i32, i64, f32, f64)]
struct _SharedArrayMut<T>
where
    T: Copy + std::default::Default,
{
    inner: Option<sharify::SharedArrayMut<T, ndarray::IxDyn>>,
}

impl<T> _SharedArrayMut<T>
where
    T: Copy + std::default::Default + numpy::Element + 'static,
{
    fn new(shape: Vec<usize>) -> PyResult<Self> {
        use topi_base::sharify::SharedArrayMut;
        let inner: SharedArrayMut<T, ndarray::IxDyn> =
            SharedArrayMut::new(&(T::default(), ndarray::IxDyn(shape.as_slice()))).unwrap();
        Self::new_from_shared(inner)
    }

    #[allow(clippy::unnecessary_wraps)]
    fn new_from_shared(shared: sharify::SharedArrayMut<T, ndarray::IxDyn>) -> PyResult<Self> {
        Ok(Self {
            inner: Some(shared),
        })
    }

    fn new_numpy_array<'py>(&mut self, py: Python<'py>) -> PyResult<&'py numpy::PyArrayDyn<T>> {
        if let Some(shared) = self.inner.as_mut() {
            let mut view = shared.as_view_mut();
            let dims = view.raw_dim();
            let mut strides = calculate_numpy_strides(
                view.strides().iter().map(|&x| x as numpy::npyffi::npy_intp),
                std::mem::size_of::<T>(),
            );
            unsafe {
                let ptr = numpy::PY_ARRAY_API.PyArray_New(
                    numpy::PY_ARRAY_API.get_type_object(numpy::npyffi::NpyTypes::PyArray_Type),
                    dims.ndim_cint(),
                    dims.as_dims_ptr(),
                    T::npy_type() as i32,
                    strides.as_mut_ptr(),
                    view.as_mut_ptr() as _,
                    std::mem::size_of::<T>() as i32,
                    numpy::npyffi::NPY_ARRAY_WRITEABLE,
                    std::ptr::null_mut(),
                );
                Ok(numpy::PyArrayDyn::from_owned_ptr(py, ptr))
            }
        } else {
            Err(pyo3::exceptions::PyException::new_err("TODO"))
        }
    }

    fn is_valid<'py>(&self, py: Python<'py>) -> &'py pyo3::types::PyBool {
        if self.inner.is_some() {
            pyo3::types::PyBool::new(py, true)
        } else {
            pyo3::types::PyBool::new(py, false)
        }
    }

    /// Updates the shape & stride of the underlying `SharedArrayMut`. Meant to
    /// be called from the Python side just before `_Messenger::send`.
    ///
    /// # Safety
    /// TODO
    fn update_strideshape(&mut self, view: &numpy::PyArrayDyn<T>) -> PyResult<()> {
        if let Some(arr) = self.inner.as_mut() {
            unsafe {
                let view = view.as_array();
                let metadata = arr.metadata_mut();
                metadata.0 = Vec::from(view.shape());
                metadata.1 = Vec::from(view.strides());
            }
            Ok(())
        } else {
            Err(pyo3::exceptions::PyException::new_err("TODO"))
        }
    }

    fn invalidate(&mut self) -> Result<sharify::SharedArrayMut<T, ndarray::IxDyn>, ()> {
        if let Some(arr) = self.inner.take() {
            Ok(arr)
        } else {
            Err(())
        }
    }
}

// Based on rust-numpy at https://github.com/PyO3/rust-numpy/blob/master/src/convert.rs
fn calculate_numpy_strides(
    strides: impl ExactSizeIterator<Item = numpy::npyffi::npy_intp>,
    type_size: usize,
) -> Vec<numpy::npyffi::npy_intp> {
    strides
        .map(|n| n as numpy::npyffi::npy_intp * type_size as numpy::npyffi::npy_intp)
        .collect()
}

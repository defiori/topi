// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use crate::data::*;
use pyo3::prelude::*;
use topi_base::{
    data::c_api::{Message, Messenger},
    nodes::Node,
};

pub struct PythonNode {
    source: String,
}

impl PythonNode {
    pub fn new<S>(source: S) -> Self
    where
        S: std::convert::AsRef<str>,
    {
        PythonNode {
            source: String::from(source.as_ref()),
        }
    }
}

impl Node<Message, Messenger> for PythonNode {
    #[cfg(not(feature = "called-from-python"))]
    fn entrypoint(&mut self, messenger: &mut Messenger) -> Result<(), String> {
        // When embedding the Python interpreter, append `pytopi_proc` as a built-in
        // module.
        let name = std::ffi::CString::new("pytopi_proc").unwrap();
        if unsafe { pyo3::ffi::PyImport_AppendInittab(name.as_ptr(), Some(initfunc)) } != 0 {
            return Err(String::from(
                "Failed to add 'pytopi_proc' module to Python interpreter.",
            ));
        }
        let gil = Python::acquire_gil();
        let py = gil.python();
        setup(py, self.source.as_str(), messenger)
    }

    #[cfg(feature = "called-from-python")]
    fn entrypoint(&mut self, messenger: &mut Messenger) -> Result<(), String> {
        // TODO: review properly
        // This is wild, we're tricking pyo3 into believing we hold the GIL so the rest
        // of the pyo3 machinery works. Since we're only accessing this entrypoint from
        // an otherwise clean Python interpreter, this is kind of true. HOWEVER,
        // 1. We are now in a different thread.
        // 2. No instance of the pyo3 GIL wrappers (in pyo3/src/gil.rs) actually exists,
        // which means the Drop impl on `pyo3::GILPool` won't be run, which
        // means the refcount for Python objects held in the pyo3 thread-local
        // storage won't be decremented. As a result we leak any Python
        // object created on the Rust side. However in the following code this should
        // only affect `py_messenger` and some empty return on calling
        // `topi_entrypoint`, both of which must be kept alive until the thread
        // completes anyway. Overall this is brittle and likely has a better
        // solution talking directly to the Python C API. The duct tape should
        // hold as long as
        // - We run only one Python node per topi_proc process.
        // - None of the objects created from within user code leak (e.g.
        //   `SharedMutArray`). This
        // should be the case since their refcount is managed by Python.
        // - pyo3 is built in release mode. This switches off a `debug_assert` in
        //   `pyo3::gil::register_owned`
        //  checking if the GIL is actually held. `register_owned` is called through
        // `PyModule::call1` -> `PyAny::call` -> `Python::from_owned_ptr_or_err`
        // -> `FromPyPointer::from_owned_ptr_or_err`.
        let py = unsafe { Python::assume_gil_acquired() };
        setup(py, self.source.as_str(), messenger)
    }
}

fn setup(py: Python, source: &str, messenger: &mut Messenger) -> Result<(), String> {
    // Load user-facing topi module.
    let topi_source = include_str!("topi.py");
    let topi_module = PyModule::from_code(py, topi_source, "topi.py", "topi")
        .map_err(|e| py_to_str_err(py, e))?;

    // Create `Messenger`.
    let messenger_wrap = _Messenger::from_messenger(messenger as *const _);
    let py_messenger = topi_module
        .call1("Messenger", (messenger_wrap.into_py(py),))
        .map_err(|e| py_to_str_err(py, e))?;

    // Load user module and call `entrypoint` function.
    let user_module = PyModule::from_code(py, source, "user_supplied", "topi_user")
        .map_err(|e| py_to_str_err(py, e))?;
    user_module
        .call1("topi_entrypoint", (py_messenger,))
        .map_err(|e| py_to_str_err(py, e))?;
    Ok(())
}

fn py_to_str_err(py: Python, err: PyErr) -> String {
    err.print(py);
    // let stacktrace = if let Some(stacktrace) = err.ptraceback(py) {
    //     format!("\n{}", stacktrace)
    //     // if let Ok(s) = stacktrace.str() {
    //     //     format!("\n{}", s.to_string_lossy())
    //     // } else {
    //     //     String::from("")
    //     // }
    // } else {
    //     String::from("")
    // };
    // println!("PYTHON ERROR{}\n{}", stacktrace, err);
    // format!("PYTHON ERROR{}\n{}", stacktrace, err)
    // println!("{}", err.ptraceback(py).unwrap().to_string());
    // println!("{:?}", err);
    format!("{:?}", err)
}

#[cfg(not(feature = "called-from-python"))]
extern "C" fn initfunc() -> *mut pyo3::ffi::PyObject {
    unsafe { PyInit_pytopi_proc() }
}

/// Topi Python module.
#[cfg(not(feature = "called-from-python"))]
#[pymodule]
fn pytopi_proc(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<_Messenger>()?;
    m.add_class::<_SharedArrayMut_bool>()?;
    m.add_class::<_SharedArrayMut_u8>()?;
    m.add_class::<_SharedArrayMut_u16>()?;
    m.add_class::<_SharedArrayMut_u32>()?;
    m.add_class::<_SharedArrayMut_u64>()?;
    m.add_class::<_SharedArrayMut_i8>()?;
    m.add_class::<_SharedArrayMut_i16>()?;
    m.add_class::<_SharedArrayMut_i32>()?;
    m.add_class::<_SharedArrayMut_i64>()?;
    m.add_class::<_SharedArrayMut_f32>()?;
    m.add_class::<_SharedArrayMut_f64>()?;
    Ok(())
}

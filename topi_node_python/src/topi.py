# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import numbers
import functools
import numpy as np
from numpy.lib.mixins import NDArrayOperatorsMixin
from pytopi_proc import (_Messenger, _SharedArrayMut_bool, 
_SharedArrayMut_u8, _SharedArrayMut_u16, _SharedArrayMut_u32, 
_SharedArrayMut_u64, _SharedArrayMut_i8, _SharedArrayMut_i16, 
_SharedArrayMut_i32, _SharedArrayMut_i64, _SharedArrayMut_f32, 
_SharedArrayMut_f64)


class Messenger():
    def __init__(self, rs_messenger):
        if isinstance(rs_messenger, _Messenger):
            self._inner = rs_messenger
        else:
            raise TypeError("Expected argument of type {}".format(_Messenger))

    def send(self, to, msg, metadata=None):
        if not isinstance(to, str):
            raise TypeError("Expected first argument of type 'str', has type {}".format(type(to)))
        if metadata is None:
            meta_string = None
            meta_bytes = None
        elif isinstance(metadata, tuple):
            meta_string, meta_bytes = metadata
        elif isinstance(metadata, str):
            meta_string = metadata
            meta_bytes = None
        elif isinstance(metadata, bytes):
            meta_string = None
            meta_bytes = metadata
        else:
            raise TypeError("Wrong type for 'metadata' argument.")
        if isinstance(msg, (SharedMutArray,)):
            msg._inner.update_strideshape(msg._view)
            self._inner.send(to, msg._inner, meta_string, meta_bytes)
        elif isinstance(msg, (np.ndarray, str)):
            self._inner.send(to, msg, meta_string, meta_bytes)
        else:
            raise TypeError("Unsupported message type {}".format(type(msg)))

    def recv(self):
        (msg, metadata) = self._inner.recv()
        if isinstance(msg, (str, np.ndarray)):
            return (msg, metadata)
        elif isinstance(msg, (_SharedArrayMut_bool, _SharedArrayMut_u8, _SharedArrayMut_u16, 
            _SharedArrayMut_u32, _SharedArrayMut_u64, _SharedArrayMut_i8, _SharedArrayMut_i16, 
            _SharedArrayMut_i32, _SharedArrayMut_i64, _SharedArrayMut_f32, _SharedArrayMut_f64,)):
            view = msg.new_numpy_array()
            return (SharedMutArray(msg, view), metadata)
        else:
            TypeError("Received unexpected message type {}".format(type(msg)))

    def try_recv(self):
        (msg, metadata) = self._inner.try_recv()
        if msg is None:
            return (None, None)
        elif isinstance(msg, (str, np.ndarray)):
            return (msg, metadata)
        elif isinstance(msg, (_SharedArrayMut_bool, _SharedArrayMut_u8, _SharedArrayMut_u16, 
            _SharedArrayMut_u32, _SharedArrayMut_u64, _SharedArrayMut_i8, _SharedArrayMut_i16, 
            _SharedArrayMut_i32, _SharedArrayMut_i64, _SharedArrayMut_f32, _SharedArrayMut_f64,)):
            view = msg.new_numpy_array()
            return (SharedMutArray(msg, view), metadata)
        else:
            TypeError("Received unexpected message type {}".format(type(msg)))
            

def if_valid(func):
    @functools.wraps(func)
    def decorator(*args, **kwargs):
        if not args[0]._inner.is_valid():
            raise Exception
        return func(*args, **kwargs)
    return decorator

def array(object, dtype=None):
    if dtype is None:
        ipt = np.asarray(object)
        (inner, view) = _array_dtype(ipt.shape, ipt.dtype)
    else:
        ipt = np.asarray(object, dtype)
        (inner, view) = _array_dtype(ipt.shape, dtype)
    view[...] = ipt
    return SharedMutArray(inner, view)

def zeros(shape, dtype=np.float64):
    if isinstance(shape, int):
        (inner, view) = _array_dtype((shape,), dtype)
    else:
        (inner, view) = _array_dtype(shape, dtype)
    return SharedMutArray(inner, view)

def _array_dtype(shape, dtype):
    if isinstance(dtype, np.dtype):
        dtype = dtype.type
    if dtype is np.bool or dtype is np.bool_:
        inner = _SharedArrayMut_bool(shape)
    elif dtype is np.uint8:
        inner = _SharedArrayMut_u8(shape)
    elif dtype is np.uint16:
        inner = _SharedArrayMut_u16(shape)
    elif dtype is np.uint32:
        inner = _SharedArrayMut_u32(shape)
    elif dtype is np.uint64:
        inner = _SharedArrayMut_u64(shape)
    elif dtype is np.int8:
        inner = _SharedArrayMut_i8(shape)
    elif dtype is np.int16:
        inner = _SharedArrayMut_i16(shape)
    elif dtype is np.int32:
        inner = _SharedArrayMut_i32(shape)
    elif dtype is np.int64:
        inner = _SharedArrayMut_i64(shape)
    elif dtype is np.float32:
        inner = _SharedArrayMut_f32(shape)
    elif dtype is np.float64 or dtype is float:
        inner = _SharedArrayMut_f64(shape)
    else:
        raise TypeError("SharedMutArray does not support '{}' elements.".format(dtype))
    view = inner.new_numpy_array()
    return (inner, view)

class SharedMutArray(NDArrayOperatorsMixin):
    def __init__(self, inner, view):
        """
        Return a new array of given shape and type, filled with zeros.
        Parameters
        ----------
        shape : int or sequence of ints
            Shape of the new array, e.g., ``(2, 3)`` or ``2``.
        dtype : data-type, optional
            The desired data-type for the array, e.g., `numpy.int8`.  Default is
            `numpy.float64`.
        Returns
        -------
        out : SharedMutArray
            Array of zeros with the given shape, dtype, and order.

        """
        if not isinstance(inner, (_SharedArrayMut_bool, _SharedArrayMut_u8, _SharedArrayMut_u16, 
            _SharedArrayMut_u32, _SharedArrayMut_u64, _SharedArrayMut_i8, _SharedArrayMut_i16, 
            _SharedArrayMut_i32, _SharedArrayMut_i64, _SharedArrayMut_f32, _SharedArrayMut_f64,)):
            raise TypeError("Unexpected type {} for 'inner' argument.".format(type(inner)))
        elif not isinstance(view, (np.ndarray,)):
            raise TypeError("Unexpected type {} for 'view' argument.".format(type(view)))
        self._inner = inner
        self._view = view
        self._UFUNC_HANDLED_INPUT_TYPES = (np.ndarray, numbers.Number, SharedMutArray)
        self._UFUNC_HANDLED_OUTPUT_TYPES = (np.ndarray, SharedMutArray)

    @property
    @if_valid
    def dtype(self):
        return self._view.dtype

    @property
    @if_valid
    def shape(self):
        return self._view.shape

    @if_valid
    def __getitem__(self, key):
        item = self._view[key._view if isinstance(key, type(self)) else key]
        if isinstance(item, self.dtype.type):
            return item
        else:
            return SharedMutArray(self._inner, item)

    @if_valid
    def __setitem__(self, key, value):
        self._view.__setitem__(key._view if isinstance(key, type(self)) else key, 
            value._view if isinstance(value, type(self)) else value)

    def __repr__(self):
        if self._inner.is_valid():
            return '{}, dtype=numpy.{})'.format(self._view.__repr__()[:-1], self.dtype)
        else:
            return 'Invalidated topi.SharedMutArray'

    def __str__(self):
        if self._inner.is_valid():
            return self._view.__str__()
        else:
            return 'Invalidated topi.SharedMutArray'

    # Providing the '__array__' interface makes potentially expensive operations look cheap
    # by copying implicitly. Better to only allow numpy functions implemented explicitly
    # via '__array_function__' and copy with a separated method?
    # @if_valid
    # def __array__(self):
    #     return self._view.copy()

    @if_valid
    def get(self, order='C'):
        return self._view.copy(order)

    @if_valid
    def __array_ufunc__(self, ufunc, method, *inputs, **kwargs):
        # See https://numpy.org/doc/stable/reference/generated/numpy.lib.mixins.NDArrayOperatorsMixin.html#numpy.lib.mixins.NDArrayOperatorsMixin
        # for this implementation.
        if method == '__call__':
            out = kwargs.get('out', ())
            for x in inputs:
                # Only support operations with instances of _UFUNC_HANDLED_INPUT_TYPES.
                # Use SharedMutArray instead of type(self) for isinstance to
                # allow subclasses that don't override __array_ufunc__ to
                # handle SharedMutArray objects.
                if not isinstance(x, self._UFUNC_HANDLED_INPUT_TYPES):
                    return NotImplemented
            for x in out:
                if not isinstance(x, self._UFUNC_HANDLED_OUTPUT_TYPES):
                    return NotImplemented

            # Defer to the implementation of the ufunc on unwrapped values.
            inputs = tuple(x._view if isinstance(x, SharedMutArray) else x
                            for x in inputs)
            # Set kwargs['out'] to the np.array views which are filled by the 
            # ufunc call.
            if out:
                kwargs['out'] = tuple(
                    x._view if isinstance(x, SharedMutArray) else x
                    for x in out)
            else:
                # If kwargs['out'] isn't given allocate new SharedMutArrays.
                # Some ufuncs require specific output types, see
                # https://numpy.org/doc/stable/reference/ufuncs.html#floating-functions
                ufunc_types = [(key, value) for (key, value) in [x.split('->') for x in ufunc.types]]
                ufunc_dic = {key: value for (key, value) in ufunc_types}
                in_chars = ''.join([np.result_type(ipt).char for ipt in inputs])
                # If we find the correct ufunc signature without type casting, directly use
                # the signature output types.
                out_chars = ufunc_dic.get(in_chars, None)
                # If we can't find the right ufunc signature, it gets more messy
                if out_chars is None:
                    not_found = True
                    # Check for each ufunc signature whether we can safely typecast
                    # all the input types.
                    for (in_ty, out_ty) in ufunc_types:
                        if all(iter(np.can_cast(*x) for x in zip(inputs, in_ty))):
                            out_chars = out_ty
                            not_found = False
                            break
                    if not_found:
                        # The ufunc call should break with the right error description, if not make sure
                        # we don't continue.
                        ufunc(*inputs, **kwargs)
                        raise Exception("A ufunc should have failed but didn't. This is worth reporting.")
                new = tuple(zeros(self._view.shape, np.dtype(o).type) for o in out_chars)
                kwargs['out'] = tuple(n._view for n in new)

            result = ufunc(*inputs, **kwargs)
            if method == 'at':
                # no return value
                return None
            else:
                if out:
                    ret = out
                else:
                    ret = new
            if type(result) is tuple:
                # multiple return values
                return ret
            else:
                # one return value
                return ret[0]
        else:
            return NotImplemented

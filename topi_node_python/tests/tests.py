# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import numpy as np
import topi

def topi_entrypoint(messenger):
    send_shared_array(messenger)
    shared_array_tests(messenger)
    messenger.send('Base', 'quit')

def send_shared_array(messenger):
    arr = np.zeros((3,4), dtype=np.float64)
    messenger.send('Base', arr)
    msg, _ = messenger.recv()
    assert(msg[0,0] == 1.0)
    arr = topi.zeros((3,4), dtype=np.float64)
    messenger.send('Base', arr)
    msg, _ = messenger.recv()
    assert(msg[0,0] == 1.0)

def shared_array_tests(messenger):
    npy_a = np.arange(1, 13, dtype=np.float64).reshape((3,4))
    # Creation
    topi_a = topi.zeros((3,4), dtype=np.float64)
    topi_b = topi.array(npy_a, dtype=np.float64)
    # Conversion to numpy array
    assert(np.all(npy_a == topi_b.get()))
    # Indexing
    assert(topi_b[0,1] == 2.0)
    assert(np.all(topi_b[1,:].get() == npy_a[1,:]))
    assert(np.all(topi_b[npy_a > 4.0].get() == npy_a[npy_a > 4.0]))
    # Assignment
    topi_a[0,0] = 1.0
    topi_a[0,1] = 2.0
    topi_a[0,2] = 3.0
    topi_a[0,3] = 4.0
    topi_a[1,:] = npy_a[1,:]
    topi_a[npy_a > 4.0] = npy_a[npy_a > 4.0]
    assert(np.all(topi_a.get() == npy_a))
    # Ufuncs
    ufuncs = [
        # MATH
        (np.add, None),
        (np.subtract, None),
        (np.multiply, None),
        # (np.matmul, None), requires (n,k),(k,n) inputs
        (np.divide, None),
        (np.logaddexp, None),
        (np.logaddexp2, None),
        (np.true_divide, None),
        (np.floor_divide, None),
        (np.negative, None),
        (np.positive, None),
        (np.power, None),
        (np.float_power, None),
        (np.remainder, None),
        (np.mod, None),
        (np.fmod, None),
        (np.divmod, None),
        (np.absolute, None),
        (np.fabs, None),
        (np.rint, None),
        (np.sign, None),
        (np.heaviside, None),
        (np.conj, None),
        (np.conjugate, None),
        (np.exp, None),
        (np.exp2, None),
        (np.log, None),
        (np.log2, None),
        (np.log10, None),
        (np.expm1, None),
        (np.log1p, None),
        (np.sqrt, None),
        (np.square, None),
        (np.cbrt, None),
        (np.reciprocal, None),
        # (np.gcd, None), requires integer arrays
        # (np.lcm, None), requires integer arrays
        # TRIGONOMETRIC
        (np.sin, None),
        (np.cos, None),
        (np.tan, None),
        # (np.arcsin, None),
        # (np.arccos, None),
        (np.arctan, None),
        (np.arctan2, None),
        (np.hypot, None),
        (np.sinh, None),
        (np.cosh, None),
        (np.tanh, None),
        (np.arcsinh, None),
        (np.arccosh, None),
        # (np.arctanh, None),
        (np.degrees, None),
        (np.radians, None),
        (np.deg2rad, None),
        (np.rad2deg, None),
        # BIT-TWIDDLING, requires integer arrays
        # (np.bitwise_and, None),
        # (np.bitwise_or, None),
        # (np.bitwise_xor, None),
        # (np.invert, None),
        # (np.left_shift, None),
        # (np.right_shift, None),
        # COMPARISON
        (np.greater, bool),
        (np.greater_equal, bool),
        (np.less, bool),
        (np.less_equal, bool),
        (np.not_equal, bool),
        (np.equal, bool),
        (np.logical_and, bool),
        (np.logical_or, bool),
        (np.logical_xor, bool),
        (np.logical_not, bool),
        (np.maximum, None),
        (np.minimum, None),
        (np.fmax, None),
        (np.fmin, None),
        # FLOATING
        (np.isfinite, bool),
        (np.isinf, bool),
        (np.isnan, bool),
        (np.isnat, bool),
        (np.fabs, None),
        (np.signbit, bool),
        (np.copysign, None),
        (np.nextafter, None),
        (np.spacing, None),
        # (np.ldexp, None),# requires integer array as second argument
        (np.fmod, None),
        (np.floor, None),
        (np.ceil, None),
        (np.trunc, None),
    ]
    npy_a = np.arange(1, 13, dtype=np.float64).reshape((3,4))
    topi_a = topi.array(npy_a)
    npy_b = np.arange(13, 25, dtype=np.float64).reshape((3,4))
    topi_b = topi.array(npy_b)
    for (ufunc, output_dtype) in ufuncs:
        if output_dtype is None:
            # print(ufunc)
            apply_ufunc(ufunc, topi_a, topi_b, npy_a, npy_b, output_dtype)

def apply_ufunc(ufunc, topi_array_a, topi_array_b, npy_array_a, npy_array_b, output_dtype=None):
    # ALLOCATE NEW TOPI ARRAY
    # 2 topi arrays as inputs
    topi_new = ufunc(*[topi_array_a, topi_array_b][:ufunc.nin])
    check_ufunc_result(ufunc.nout, topi_new, ufunc(*[npy_array_a, npy_array_b][:ufunc.nin]))
    # 1 topi array, 1 scalar
    topi_new = ufunc(*[topi_array_a, 4.0][:ufunc.nin])
    check_ufunc_result(ufunc.nout, topi_new, ufunc(*[npy_array_a, 4.0][:ufunc.nin]))
    topi_new = ufunc(*[4.0, topi_array_a][:ufunc.nin])
    check_ufunc_result(ufunc.nout, topi_new, ufunc(*[4.0, npy_array_a][:ufunc.nin]))
    # 1 topi array, 1 numpy array
    topi_new = ufunc(*[topi_array_a, npy_array_b][:ufunc.nin])
    check_ufunc_result(ufunc.nout, topi_new, ufunc(*[npy_array_a, npy_array_b][:ufunc.nin]))
    topi_new = ufunc(*[npy_array_b, topi_array_a][:ufunc.nin])
    check_ufunc_result(ufunc.nout, topi_new, ufunc(*[npy_array_b, npy_array_a][:ufunc.nin]))

    # OUT TOPI ARRAY
    topi_new = tuple([topi.array(npy_array_a, dtype=output_dtype) for _ in range(ufunc.nout)])
    # 2 topi arrays as inputs
    ufunc(*[topi_array_a, topi_array_b][:ufunc.nin], out=topi_new)
    check_ufunc_result(ufunc.nout, topi_new, ufunc(*[npy_array_a, npy_array_b][:ufunc.nin]))
    # 1 topi array, 1 scalar
    ufunc(*[topi_array_a, 4.0][:ufunc.nin], out=topi_new)
    check_ufunc_result(ufunc.nout, topi_new, ufunc(*[npy_array_a, 4.0][:ufunc.nin]))
    ufunc(*[4.0, topi_array_a][:ufunc.nin], out=topi_new)
    check_ufunc_result(ufunc.nout, topi_new, ufunc(*[4.0, npy_array_a][:ufunc.nin]))
    # 1 topi array, 1 numpy array
    ufunc(*[topi_array_a, npy_array_b][:ufunc.nin], out=topi_new)
    check_ufunc_result(ufunc.nout, topi_new, ufunc(*[npy_array_a, npy_array_b][:ufunc.nin]))
    ufunc(*[npy_array_b, topi_array_a][:ufunc.nin], out=topi_new)
    check_ufunc_result(ufunc.nout, topi_new, ufunc(*[npy_array_b, npy_array_a][:ufunc.nin]))

def check_ufunc_result(nout, topi_result, npy_result):
    extract_topi = lambda result: result.get() if isinstance(result, topi.SharedMutArray) else result
    if nout == 1:
        if type(topi_result) is tuple:
            # assert(np.all(extract_topi(topi_result[0]) == npy_result))
            np.testing.assert_equal(extract_topi(topi_result[0]), npy_result)
        else:
            # assert(np.all(extract_topi(topi_result) == npy_result))
            np.testing.assert_equal(extract_topi(topi_result), npy_result)
    elif nout == 2:
        # assert(np.all(extract_topi(topi_result[0]) == npy_result[0]))
        np.testing.assert_equal(extract_topi(topi_result[0]), npy_result[0])
        # assert(np.all(extract_topi(topi_result[1]) == npy_result[1]))
        np.testing.assert_equal(extract_topi(topi_result[1]), npy_result[1])
    else:
        raise ValueError("Expected 'nout' < 3.")

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

#define TOPI_DATATYPE_ARRAY 1
#define TOPI_DATATYPE_STRING 2
#define TOPI_DATATYPE_BYTES 3

#define TOPI_ARRAYTYPE_MUT 1
#define TOPI_ARRAYTYPE_SHARED 2
#define TOPI_ARRAYTYPE_SHARED_MUT 3

#define TOPI_ARRAYELEMENTTYPE_BOOL 1
#define TOPI_ARRAYELEMENTTYPE_UINT8 2
#define TOPI_ARRAYELEMENTTYPE_UINT16 3
#define TOPI_ARRAYELEMENTTYPE_UINT32 4
#define TOPI_ARRAYELEMENTTYPE_UINT64 5
#define TOPI_ARRAYELEMENTTYPE_INT8 6
#define TOPI_ARRAYELEMENTTYPE_INT16 7
#define TOPI_ARRAYELEMENTTYPE_INT32 8
#define TOPI_ARRAYELEMENTTYPE_INT64 9
#define TOPI_ARRAYELEMENTTYPE_F32 10
#define TOPI_ARRAYELEMENTTYPE_F64 11

#define TOPI_BYTESTYPE_MUT 1
#define TOPI_BYTESTYPE_SHARED 2
#define TOPI_BYTESTYPE_SHARED_MUT 3

typedef struct topi_DataMessenger_t topi_DataMessenger_t;
typedef struct topi_MetaData_t topi_MetaData_t;
typedef unsigned short topi_DataType_t;

typedef struct topi_Array_t topi_Array_t;
typedef unsigned short topi_ArrayType_t;
typedef unsigned short topi_ArrayElementType_t;

typedef struct topi_String_t topi_String_t;
typedef struct topi_Bytes_t topi_Bytes_t;

typedef struct topi_Message_t {
    topi_MetaData_t *metadata;
    topi_DataType_t data_type;
    union {
        topi_Array_t *array;
        topi_String_t *string;
        topi_Bytes_t *bytes;
    } data;
} topi_Message_t;

typedef struct topi_Callbacks_t {
    unsigned int (*message_recv)(const topi_DataMessenger_t *messenger, bool *terminate, topi_Message_t *msg);
    unsigned int (*message_send_array)(const topi_DataMessenger_t *messenger, const char *recv_id, topi_Array_t *array, topi_MetaData_t *metadata);
    // unsigned int (*message_send_c_array)(const topi_DataMessenger_t *messenger, const char*recv_id, topi_ArrayElementType_t elem_type, const void *data, 
    //                                         unsigned int n_dims, const unsigned int *shape, const int *strides, topi_MetaData_t *metadata);
    unsigned int (*message_send_string)(const topi_DataMessenger_t *messenger, const char *recv_id, topi_String_t *, topi_MetaData_t *metadata);
    unsigned int (*message_send_bytes)(const topi_DataMessenger_t *messenger, const char *recv_id, topi_Bytes_t *, topi_MetaData_t *metadata);
    // unsigned int (*message_send_c_bytes)(const topi_DataMessenger_t *messenger, const char *recv_id, unsigned int len, const uint8_t *bytes, topi_MetaData_t *metadata);

    // unsigned int (*metadata_create)(topi_MetaData_t **, uint8_t *, unsigned int);
    // unsigned int (*metadata_len)(topi_MetaData_t *, unsigned int *);
    // unsigned int (*metadata_access)(topi_MetaData_t *, uint8_t **);
    // unsigned int (*metadata_free)(topi_MetaData_t *);

    unsigned int (*array_create)(topi_Array_t **array, topi_ArrayType_t array_type, topi_ArrayElementType_t elem_type, unsigned int n_dims, 
                                    const unsigned int *shape);
    unsigned int (*array_free)(topi_Array_t *array);
    // unsigned int (*array_is_mut)(const topi_Array_t *array, bool *is_mut);
    unsigned int (*array_type)(const topi_Array_t *array, topi_ArrayType_t *array_type, topi_ArrayElementType_t *elem_type);
    unsigned int (*array_n_elements)(const topi_Array_t *array, unsigned int *n_elements);
    // unsigned int (*array_n_dims)(const topi_Array_t *array, unsigned int *n_dims);
    // unsigned int (*array_shape)(const topi_Array_t *array, unsigned int *shape);
    // unsigned int (*array_strides)(const topi_Array_t *array, int *strides);
    // unsigned int (*array_view)(const topi_Array_t *array, const void **data);
    unsigned int (*array_view_mut)(topi_Array_t *array, void **data);

    // TODO: String & Bytes functions
} topi_Callbacks_t;

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdio.h>
#include <topi_data.h>
#include <unistd.h>
#include <assert.h>

void entrypoint(const topi_DataMessenger_t *messenger, const topi_Callbacks_t *callbacks) {
    setbuf(stdout, NULL);
    
    bool terminate = false;
    topi_Message_t msg;
    topi_Array_t *array;
    topi_ArrayType_t array_type = 0;
    topi_ArrayElementType_t elem_type = 0;
    void *elem;
    unsigned int len = 0;
    bool continue_iter = true;

    while (continue_iter)
    {
        if (callbacks->message_recv(messenger, &terminate, &msg))
        {
            puts("Receive error.");
            break;
        }
        if (terminate)
        {
            puts("Received terminate signal.");
            break;
        }
        
        switch (msg.data_type)
        {
        case TOPI_DATATYPE_ARRAY:
            array = msg.data.array;
            if (callbacks->array_type(array, &array_type, &elem_type)) {
                puts("Array type error.");
                continue_iter = false;
                break;
            }
            if (elem_type != TOPI_ARRAYELEMENTTYPE_F64) {
                puts("Unexpected array element type.");
                continue_iter = false;
                break;
            }
            switch (array_type)
            {
            case TOPI_ARRAYTYPE_SHARED_MUT:
                callbacks->array_view_mut(array, &elem);
                assert(*(double*)elem == 25.0);
                *(double*)elem = 99.0;
                callbacks->array_n_elements(array, &len);
                assert(len == 1000);
                callbacks->message_send_array(messenger, "Base", array, NULL);
                break;
            case TOPI_ARRAYTYPE_MUT:
                callbacks->array_view_mut(array, &elem);
                assert(*(double*)elem == 0.0);
                callbacks->array_free(array);
                continue_iter = false;
                break;
            default:
                puts("Unexpected array type.");
                continue_iter = false;
                break;
            }
            break;
        default:
            puts("Unexpected message data.");
            continue_iter = false;
            break;
        }
    }
}

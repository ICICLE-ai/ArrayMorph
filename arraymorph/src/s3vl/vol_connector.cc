/* This connector's header */
#include "arraymorph/core/constants.h"
#include "arraymorph/s3vl/dataset_callbacks.h"
#include "arraymorph/s3vl/file_callbacks.h"
#include "arraymorph/s3vl/group_callbacks.h"
#include "arraymorph/s3vl/initialize.h"
#include "arraymorph/s3vl/vol_connector.h"

#include <H5PLextern.h>
#include <hdf5.h>
#include <iostream>
#include <stdlib.h>
// #include "operators.h"
// #include "metadata.h"

/* Introscpect */

static herr_t S3_introspect_opt_query(void *obj, H5VL_subclass_t cls,
                                      int op_type, uint64_t *flags);

static herr_t S3_get_conn_cls(void *obj, H5VL_get_conn_lvl_t lvl,
                              const struct H5VL_class_t **conn_cls);

/* The VOL class struct */
static const H5VL_class_t template_class_g = {
    3,                                     /* VOL class struct version */
    S3_VOL_CONNECTOR_VALUE,                /* value                    */
    S3_VOL_CONNECTOR_NAME,                 /* name                     */
    1,                                     /* version                  */
    0,                                     /* capability flags         */
    S3VLINITIALIZE::s3VL_initialize_init,  /* initialize               */
    S3VLINITIALIZE::s3VL_initialize_close, /* terminate                */
    {
        /* info_cls */
        (size_t)0, /* size    */
        NULL,      /* copy    */
        NULL,      /* compare */
        NULL,      /* free    */
        NULL,      /* to_str  */
        NULL,      /* from_str */
    },
    {
        /* wrap_cls */
        S3VLDatasetCallbacks::S3VL_get_object,  /* get_object   */
        NULL,                                   /* get_wrap_ctx */
        S3VLDatasetCallbacks::S3VL_wrap_object, /* wrap_object  */
        NULL,                                   /* unwrap_object */
        NULL,                                   /* free_wrap_ctx */
    },
    {
        /* attribute_cls */
        NULL, /* create       */
        NULL, /* open         */
        NULL, /* read         */
        NULL, /* write        */
        NULL, /* get          */
        NULL, /* specific     */
        NULL, /* optional     */
        NULL  /* close        */
    },
    {
        /* dataset_cls */
        S3VLDatasetCallbacks::S3VL_dataset_create,   /* create       */
        S3VLDatasetCallbacks::S3VL_dataset_open,     /* open         */
        S3VLDatasetCallbacks::S3VL_dataset_read,     /* read         */
        S3VLDatasetCallbacks::S3VL_dataset_write,    /* write        */
        S3VLDatasetCallbacks::S3VL_dataset_get,      /* get          */
        S3VLDatasetCallbacks::S3VL_dataset_specific, /* specific     */
        NULL,                                        /* optional     */
        S3VLDatasetCallbacks::S3VL_dataset_close     /* close        */
    },
    {
        /* datatype_cls */
        NULL, /* commit       */
        NULL, /* open         */
        NULL, /* get_size     */
        NULL, /* specific     */
        NULL, /* optional     */
        NULL  /* close        */
    },
    {
        /* file_cls */
        S3VLFileCallbacks::S3VL_file_create, /* create       */
        S3VLFileCallbacks::S3VL_file_open,   /* open         */
        S3VLFileCallbacks::S3VL_file_get,    /* get          */
        NULL,                                /* specific     */
        NULL,                                /* optional     */
        S3VLFileCallbacks::S3VL_file_close   /* close        */
    },
    {
        /* group_cls */
        NULL,                               /* create       */
        S3VLGroupCallbacks::S3VLgroup_open, /* open         */
        S3VLGroupCallbacks::S3VLgroup_get,  /* get          */
        // NULL,
        NULL,                               /* specific     */
        NULL,                               /* optional     */
        S3VLGroupCallbacks::S3VLgroup_close // NULL /* close        */
    },
    {
        /* link_cls */
        NULL,                             /* create       */
        NULL,                             /* copy         */
        NULL,                             /* move         */
        S3VLGroupCallbacks::S3VLlink_get, /* get          */
        NULL, // S3VLGroupCallbacks::S3VLlink_specific, /* specific     */
        NULL  /* optional     */
    },
    {
        /* object_cls */
        S3VLDatasetCallbacks::S3VL_obj_open, /* open         */
        NULL,                                /* copy         */
        S3VLDatasetCallbacks::S3VL_obj_get,  /* get          */
        NULL,                                /* specific     */
        NULL                                 /* optional     */
    },
    {
        /* introscpect_cls */
        S3_get_conn_cls,        /* get_conn_cls  */
        NULL,                   /* get_cap_flags */
        S3_introspect_opt_query /* opt_query     */
    },
    {
        /* request_cls */
        NULL, /* wait         */
        NULL, /* notify       */
        NULL, /* cancel       */
        NULL, /* specific     */
        NULL, /* optional     */
        NULL  /* free         */
    },
    {
        /* blob_cls */
        NULL, /* put          */
        NULL, /* get          */
        NULL, /* specific     */
        NULL  /* optional     */
    },
    {
        /* token_cls */
        NULL, /* cmp          */
        NULL, /* to_str       */
        NULL  /* from_str     */
    },
    NULL /* optional     */
};

/* These two functions are necessary to load this plugin using
 * the HDF5 library.
 */

static herr_t S3_introspect_opt_query(void *obj, H5VL_subclass_t cls,
                                      int op_type, uint64_t *flags) {
  return ARRAYMORPH_SUCCESS;
}

static herr_t S3_get_conn_cls(void *obj, H5VL_get_conn_lvl_t lvl,
                              const struct H5VL_class_t **conn_cls) {
  *conn_cls = new H5VL_class_t;
  return ARRAYMORPH_SUCCESS;
}

H5PL_type_t H5PLget_plugin_type(void) { return H5PL_TYPE_VOL; }
const void *H5PLget_plugin_info(void) { return &template_class_g; }

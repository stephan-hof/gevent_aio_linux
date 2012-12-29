#include "Python.h"

/* for tming */
#include <sys/time.h>

/* For direct io */
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <unistd.h>
#include <linux/aio_abi.h>
#include <sys/syscall.h>
#include <sys/eventfd.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <malloc.h>


static unsigned int PAGE_SIZE = 4096;

static int
io_setup(unsigned int maxevents, aio_context_t *ctx)
{
    return syscall(SYS_io_setup, maxevents, ctx);
}

static int
io_destroy(aio_context_t ctx)
{
    return syscall(SYS_io_destroy, ctx);
}

static int
io_submit(aio_context_t ctx, long number, struct iocb **aiocbs)
{
    return syscall(SYS_io_submit, ctx, number, aiocbs);
}


static int
io_getevents(
    aio_context_t ctx,
    long min_nr,
    long nr,
    struct io_event *events,
    struct timespec *tmo)
{
    return syscall(SYS_io_getevents, ctx, min_nr, nr, events, tmo);
}


typedef struct {
    PyObject_HEAD
    aio_context_t aio_ctx;
    int event_fd;
} Context;

/* This struct is the 'callback data' send to io_submit */
typedef struct {
    struct iocb aiocb;
    char *buf;
    PyObject *callback;
} callback_data_t;

static PyObject *
Context_new(PyTypeObject *type, PyObject *args, PyObject *kwargs)
{
    Context *self;
    self = (Context*) type->tp_alloc(type, 0);
    self->aio_ctx = 0;
    /* TODO: Only register for read events, the kernel does the writing */
    /* TODO: Check how I have to prepare the io_strucutre  so that the kernel
     * notifies eventfd. ngxin is my friedn here
     */
    self->event_fd = 0;

    return (PyObject*) self;
}


static int
Context_init(Context *self, PyObject *args, PyObject *kwargs)
{

    if (io_setup(1000, &self->aio_ctx) == -1) {
        PyErr_SetFromErrno(PyExc_OSError);
        return -1;
    }

    if ((self->event_fd = eventfd(0, EFD_NONBLOCK)) == -1) {
        PyErr_SetFromErrno(PyExc_OSError);
        return -1;
    }

    return 0;
}

static void
Context_dealloc(Context *self)
{
    if (self->aio_ctx > 0) {
        io_destroy(self->aio_ctx);
    }

    if (self->event_fd > 0) {
        close(self->event_fd);
    }

    self->ob_type->tp_free(self);
}

static PyObject*
submit_async(
    Context *self,
    int opcode,
    int fd,
    char *buf,
    int size,
    int offset,
    PyObject *callback)
{

    /* Helper to send the args correctly to io_submit */
    struct iocb *piocb[1];

    callback_data_t *data = malloc(sizeof(callback_data_t));
    memset(data, 0, sizeof(callback_data_t));

    Py_INCREF(callback);
    data->callback = callback;
    data->buf = buf;

    data->aiocb.aio_lio_opcode = opcode;
    data->aiocb.aio_fildes = fd;
    data->aiocb.aio_buf = (uint64_t) (uintptr_t) data->buf;
    data->aiocb.aio_nbytes = size;
    data->aiocb.aio_offset = offset;
    data->aiocb.aio_flags = IOCB_FLAG_RESFD;
    data->aiocb.aio_resfd = self->event_fd;

    data->aiocb.aio_data = (uint64_t) (uintptr_t) data;

    piocb[0] = &data->aiocb;
    if (io_submit(self->aio_ctx, 1, piocb) != 1) {
        Py_DECREF(callback);
        free(data->buf);
        free(data);
        return PyErr_SetFromErrno(PyExc_OSError);
    }
    Py_RETURN_NONE;
}

static PyObject*
Context_submit_read(Context *self, PyObject *args, PyObject *kwargs)
{
    int fd, offset, size;
    PyObject *callback;

    if (!PyArg_ParseTuple(args, "iiiO", &fd, &offset, &size, &callback)) {
        return NULL;
    }

    char *buf = memalign(PAGE_SIZE, size);
    if (buf == NULL) {
        return PyErr_SetFromErrno(PyExc_OSError);
    }
    memset(buf, 0, size);

    return submit_async(self, IOCB_CMD_PREAD, fd, buf, size, offset, callback);
}

static PyObject*
Context_submit_write(Context *self, PyObject *args, PyObject *kwargs)
{
    int fd, offset, size;
    char *data_to_write;
    PyObject *callback;
    if (!PyArg_ParseTuple(args, "iit#O", &fd, &offset, &data_to_write, &size, &callback)) {
        return NULL;
    }

    char *buf = memalign(PAGE_SIZE, size);
    if (buf == NULL) {
        return PyErr_SetFromErrno(PyExc_OSError);
    }
    strncpy(buf, data_to_write, size);

    return submit_async(self, IOCB_CMD_PWRITE, fd, buf, size, offset, callback);
}

static PyObject*
Context_get_finished_cbs(Context *self)
{
    /* Counter for the for-loop */
    int i;
    /* number of events after io_getevents */
    int events_ready;

    struct iocb *aiocb;
    callback_data_t *callback;
    PyObject *tmp;

    PyObject *result;
    struct io_event event[64];

    memset(event, 0, sizeof(struct io_event) * 64);

    struct timespec ts = {0,0};
    uint64_t signaled_events;

    int bytes_read = read(self->event_fd, &signaled_events, 8);
    if (bytes_read == -1) {
        if (errno == EAGAIN) {
            /* Nothing here, wrong wakeup */
            return PyList_New(0);
        }
        return PyErr_SetFromErrno(PyExc_OSError);
    }
    if (bytes_read != 8) {
        return PyErr_Format(
            PyExc_OSError,
            "Reading from eventfd yields only %i bytes",
            bytes_read);
    }

    result = PyList_New(0);
    while (signaled_events) {
        events_ready = io_getevents(self->aio_ctx, 1, 64, event, &ts);

        if (events_ready == -1) {
            return PyErr_SetFromErrno(PyExc_OSError);
        }

        if (events_ready < 0) {
            return PyErr_Format(PyExc_OSError, "Strange res");
        }

        if (events_ready == 0 && signaled_events > 0) {
            Py_DECREF(result);
            return PyErr_Format(
                PyExc_OSError,
                "No events ready but signal count is still %llu",
                signaled_events);
        }

        if (events_ready == 0) {
            return result;
        }

        if (events_ready > signaled_events) {
            signaled_events = 0;
        }
        else {
            signaled_events -= events_ready;
        }

        for (i = 0; i < events_ready; i++) {
            callback = (callback_data_t*) (uintptr_t) event[i].data;

            aiocb = (struct iocb*) (uintptr_t) event[i].obj;

            if (aiocb->aio_lio_opcode == IOCB_CMD_PREAD) {
                if (event[i].res <= 0) {
                    tmp = Py_BuildValue(
                            "LNO",
                            event[i].res,
                            PyString_FromString(""),
                            callback->callback);
                }
                else {
                    tmp = Py_BuildValue(
                            "LNO",
                            event[i].res,
                            PyString_FromStringAndSize(
                                (const char *)(uintptr_t)aiocb->aio_buf,
                                (long long)event[i].res),
                            callback->callback);
                }
            }
            else if (aiocb->aio_lio_opcode == IOCB_CMD_PWRITE) {
                tmp = Py_BuildValue("LOO", event[i].res, Py_None, callback->callback);
            }

            /* TODO Error handling */
            PyList_Append(result, tmp);

            Py_DECREF(tmp);
            Py_DECREF(callback->callback);
            free(callback->buf);
            free(callback);
        }
    }
    return result;
}

static PyObject *
Context_get_eventfd(Context *self)
{
    return PyInt_FromLong(self->event_fd);
}

static PyObject *
Context_open_direct(Context *self, PyObject *args)
{
    char *filename;
    int flags;
    mode_t mode;
    if (!PyArg_ParseTuple(args, "si|i", &filename, &flags, &mode)) {
        return NULL;
    }

    int fd = open(filename, flags | O_DIRECT, mode);
    return PyInt_FromLong(fd);
}

static PyMethodDef Context_methods[] = {
    {"submit_write", (PyCFunction)Context_submit_write, METH_VARARGS, ""},
    {"submit_read", (PyCFunction)Context_submit_read, METH_VARARGS, ""},
    {"get_finished_cbs", (PyCFunction)Context_get_finished_cbs, METH_NOARGS, ""},
    {"get_eventfd", (PyCFunction)Context_get_eventfd, METH_NOARGS, ""},
    {"open_direct", (PyCFunction)Context_open_direct, METH_VARARGS, ""},
    {NULL, NULL, 0, NULL}
};

static PyTypeObject ContextType = {
    PyObject_HEAD_INIT(NULL)
    0,                         /*ob_size*/
    "Context",                 /*tp_name*/
    sizeof(Context),           /*tp_basicsize*/
    0,                         /*tp_itemsize*/
    (destructor)Context_dealloc, /*tp_dealloc*/
    0,                         /*tp_print*/
    0,                         /*tp_getattr*/
    0,                         /*tp_setattr*/
    0,                         /*tp_compare*/
    0,                         /*tp_repr*/
    0,                         /*tp_as_number*/
    0,                         /*tp_as_sequence*/
    0,                         /*tp_as_mapping*/
    0,                         /*tp_hash */
    0,                         /*tp_call*/
    0,                         /*tp_str*/
    0,                         /*tp_getattro*/
    0,                         /*tp_setattro*/
    0,                         /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,/*tp_flags*/
    "",                        /* tp_doc */
    0,                         /* tp_traverse */
    0,                         /* tp_clear */
    0,                         /* tp_richcompare */
    0,                         /* tp_weaklistoffset */
    0,                         /* tp_iter */
    0,                         /* tp_iternext */
    Context_methods,           /* tp_methods */
    0,                         /* tp_members */
    0,                         /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    (initproc)Context_init,    /* tp_init */
    0,                         /* tp_alloc */
    Context_new,               /* tp_new */
};



PyMODINIT_FUNC
init_linux_aio(void){
    PyObject* module;

    if (PyType_Ready(&ContextType) < 0) {
        return;
    }

    module = Py_InitModule("_linux_aio", NULL);
    if (module == NULL) {
        return;
    }

    Py_INCREF((PyObject*) &ContextType);
    PyModule_AddObject(module, "Context", (PyObject*)&ContextType);
}

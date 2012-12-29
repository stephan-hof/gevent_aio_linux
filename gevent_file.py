import _linux_aio
import gevent.core
import gevent.hub

import time
import os
import errno
class Context(object):
    def __init__(self, hub=None):
        self._context_time = [0, 0]
        if hub is None:
            self.hub = gevent.hub.get_hub()

        self._context = _linux_aio.Context()
        self._aio_ready_event = gevent.core.read_event(
            handle=self._context.get_eventfd(),
            callback=self._aio_ready,
            persist=True)

    def _aio_ready(self, event, eventytype):

        start = time.time()
        finished = []
        while not finished:
            try:
                finished = self._context.get_finished_cbs()
            except Exception:
                print 'fuck'
        self._context_time[0] += (time.time() - start)

        for ret_code, ret, task in finished:
            task.switch((ret_code, ret))

    def open_file(self, name, mode="r"):
        fd = open(name, mode)
        return File(self, fd.fileno(), fd)

    def open_direct_write(self, name):
        fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_DIRECT)
        return File(self, fd)

    def open_direct_read(self, name):
        fd = os.open(name, os.O_RDONLY | os.O_DIRECT)
        if fd < 0:
            raise Exception((fd, errno.errorcode[fd]))
        return File(self, fd)


    def submit_write(self, fd, offset, data, callback):
        start = time.time()
        self._context.submit_write(fd, offset, data, callback)
        self._context_time[1] += (time.time() - start)

    def submit_read(self, fd, offset, size, callback):
        start = time.time()
        self._context.submit_read(fd, offset, size, callback)
        self._context_time[1] += (time.time() - start)

class File(object):
    def __init__(self, context, fd, fdhandle=None):
        self.context = context
        self._offset = 0
        self._fd = fd
        # needed to keep the refence alive, only the numbe would kill the reference
        self.fdhandle = fdhandle

    def write(self, data):
        self.context.submit_write(
            self._fd,
            self._offset,
            data,
            gevent.hub.getcurrent())
        write_res, none  = self.context.hub.switch()
        if write_res < 0:
            write_res = -write_res
            raise Exception((write_res, errno.errorcode[write_res]))
        elif write_res != len(data):
            print 'write res', write_res
        else:
            self._offset += write_res
        return write_res

    def read(self, size):
        self.context.submit_read(
            self._fd,
            self._offset,
            size,
           gevent.hub.getcurrent())

        read_res, data = self.context.hub.switch()
        if read_res < 0:
            read_res = -read_res
            raise Exception((read_res, errno.errorcode[read_res]))
        else:
            self._offset += read_res
        return data

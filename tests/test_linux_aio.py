import inspect
import unittest
import _linux_aio


class TestLinuxAio(unittest.TestCase):
    def test_import(self):
        self.assertTrue(inspect.ismodule(_linux_aio))

    def test_init(self):
        self.assertIsInstance(_linux_aio.Context(), _linux_aio.Context)

    def test_submit_write(self):
        ctx = _linux_aio.Context()
        with open('/tmp/test', 'w') as fd:
            fd = fd.fileno()
            self.assertIsNone(ctx.submit_write(fd, 0, 'abcdefg', None))
            self.assertIsNone(ctx.submit_write(fd, 7, 'stephan', None))

        with open('/tmp/test', 'r') as fd:
            self.assertEqual('abcdefgstephan', fd.read())

    def test_get_finished_cbs(self):
        ctx = _linux_aio.Context()
        callbacks = [object(), object()]
        with open('/tmp/test', 'w') as fd:
            fd = fd.fileno()
            ctx.submit_write(fd, 0, 'abcdefg', callbacks[0])
            ctx.submit_write(fd, 7, 'stephan', callbacks[1])


        ret = ctx.get_finished_cbs()
        ref = [(7, None, callbacks[0]), (7, None, callbacks[1])]
        self.assertEqual(ref, ret)

    def test_get_eventfd(self):
        ctx = _linux_aio.Context()
        self.assertGreater(ctx.get_eventfd(), 0)

    def test_submit_read(self):
        ctx = _linux_aio.Context()
        with open('/tmp/test', 'w+') as fd:
            fd.write('123456789')
            fd.seek(0)
            ctx.submit_read(fd.fileno(), 0, 5, None)

        ret = ctx.get_finished_cbs()
        ref = [(5L, '12345', None)]
        self.assertEqual(ref, ret)

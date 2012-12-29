import sys
import os
import gevent
import gevent_file

context = gevent_file.Context()
DO_DIRECT = False

def io_bound(filename):
    if DO_DIRECT:
        fd = context.open_direct_read(filename)
    else:
        fd = context.open_file(filename, 'r')
    for _ in xrange(580):
        fd.read(1024*1024)
    print context._context_time, sum(context._context_time)

if len(sys.argv) != 3:
    print 'Usage: direct,nodirect  directory'
    sys.exit(1)

if sys.argv[1] == 'direct':
    DO_DIRECT = True

target_dir = sys.argv[2]
print DO_DIRECT, target_dir

greenlets = [
    gevent.spawn(io_bound, os.path.join(target_dir, 'foo1')),
    gevent.spawn(io_bound, os.path.join(target_dir, 'foo2')),
    gevent.spawn(io_bound, os.path.join(target_dir, 'foo3')),
]
gevent.joinall(greenlets)

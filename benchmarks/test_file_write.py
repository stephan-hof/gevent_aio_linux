import os
import sys
import time
import gevent
import gevent_file

DO_DIRECT = False
context = gevent_file.Context()
def io_bound_green(filename):
    if DO_DIRECT:
        fd = context.open_direct_write(filename)
    else:
        fd = context.open_file(filename, 'w' )

    for _ in xrange(10 * 1024):
        fd.write('x' * 4096)
    print context._context_time, sum(context._context_time)

full_time = 0
def io_bound_block(filename):
    global full_time
    fd = open(filename, 'w')
    for _ in xrange(10 * 1024):

        start = time.time()
        fd.write('x' * 4096)
        full_time += (time.time() - start)

        gevent.sleep(0)
    print full_time

if len(sys.argv) != 4:
    print 'Usage: green,block  direct,nodirect  path'
    sys.exit(1)

if sys.argv[1] == 'green':
    io_bound = io_bound_green
else:
    io_bound = io_bound_block

if sys.argv[2] == 'direct':
    DO_DIRECT = True

target_dir = sys.argv[3]

print io_bound, DO_DIRECT, target_dir

greenlets = [
    gevent.spawn(io_bound, os.path.join(target_dir, 'foo1')),
    gevent.spawn(io_bound, os.path.join(target_dir, 'foo2')),
    gevent.spawn(io_bound, os.path.join(target_dir, 'foo3')),
]
gevent.joinall(greenlets)

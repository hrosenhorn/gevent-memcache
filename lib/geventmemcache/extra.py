import gevent
from gevent import Timeout
from gevent import Greenlet, GreenletExit, queue
import logging

class TaskletPool(object):
    log = logging.getLogger('TaskletPool')

    GAMMA = 0.995
    TRESHOLD = 2.0
    INIT_WORKERS = 2

    def __init__(self):
        self._queue = queue.Queue()
        self._workers = []
        for i in range(self.INIT_WORKERS):
            self._add_worker()

        #self._adjuster = Tasklet.interval(1.0, self._adjust, daemon = True)()
        self._queue_len = 0.0

        def _adjuster():
            while True:
                gevent.sleep(1)
                self._adjust()

        gevent.spawn(_adjuster)


    def _add_worker(self):
        self._workers.append(gevent.spawn(self._worker))

    def _adjust(self):
        self._queue_len = (self.GAMMA * self._queue_len) + ((1.0 - self.GAMMA) * self._queue.qsize())
        x = self._queue_len / len(self._workers)
        if x > self.TRESHOLD:
            self._add_worker()

    def _worker(self):
        while True:
            try:
                f, args, kwargs = self._queue.get()

                f(*args, **kwargs)

            except GreenletExit:
                raise
            except:
                self.log.exception("in taskpool worker")
                gevent.sleep(1.0)


    def defer(self, f, *args, **kwargs):
        self._queue.put((f, args, kwargs))

class DeferredQueue(object):
    log = logging.getLogger('DeferredQueue')

    __slots__ = ['_queue', '_working']

    def __init__(self):
        self._queue = queue.Queue()
        self._working = False

    def _pump(self):
        try:
            while not self._queue.empty():
                try:
                    f, args, kwargs = self._queue.get()
                    f(*args, **kwargs)
                except GreenletExit:
                    raise
                except:
                    self.log.exception("in deferred queue")
        finally:
            self._working = False

    def defer(self, f, *args, **kwargs):
        self._queue.put((f, args, kwargs))
        if not self._working:
            self._working = True
            GreenletExtra.defer(self._pump)

class GreenletExtra(object):
    _tasklet_pool = None

    @classmethod
    def _defer(cls, f, *args, **kwargs):
        cls._tasklet_pool.defer(f, *args, **kwargs)

    @classmethod
    def defer(cls, f, *args, **kwargs):
        #first time init the tasklet pool, next time _defer is used directly
        cls.defer = cls._defer
        cls._tasklet_pool = TaskletPool()
        cls._tasklet_pool.defer(f, *args, **kwargs)

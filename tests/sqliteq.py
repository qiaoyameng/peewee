import os
import threading
import time
from functools import partial

try:
    import gevent
    from gevent.event import Event as GreenEvent
except ImportError:
    gevent = None

from peewee import *
from playhouse.sqliteq import ResultTimeout
from playhouse.sqliteq import SqliteQueueDatabase
from playhouse.sqliteq import WriterPaused

from .base import BaseTestCase
from .base import TestModel
from .base import db_loader
from .base import get_sqlite_db
from .base import skip_if


get_db = partial(db_loader, 'sqlite', db_class=SqliteQueueDatabase)
db = get_sqlite_db()


class User(TestModel):
    name = TextField(unique=True)

    class Meta:
        table_name = 'threaded_db_test_user'


class BaseTestQueueDatabase(object):
    database_config = {}
    n_rows = 20
    n_threads = 20

    def setUp(self):
        super(BaseTestQueueDatabase, self).setUp()
        User._meta.database = db
        with db:
            db.create_tables([User], safe=True)

        User._meta.database = \
                self.database = get_db(**self.database_config)

        # Sanity check at startup.
        self.assertEqual(self.database.queue_size(), 0)

    def tearDown(self):
        super(BaseTestQueueDatabase, self).tearDown()
        User._meta.database = db
        with db:
            User.drop_table()
        if not self.database.is_closed():
            self.database.close()
        if not db.is_closed():
            db.close()
        filename = db.database
        if os.path.exists(filename):
            os.unlink(filename)

    def test_query_error(self):
        self.database.start()
        curs = self.database.execute_sql('foo bar baz')
        self.assertRaises(OperationalError, curs.fetchone)
        self.database.stop()

    def test_integrity_error(self):
        self.database.start()
        u = User.create(name='u')
        self.assertRaises(IntegrityError, User.create, name='u')

    def test_query_execution(self):
        qr = User.select().execute()
        self.assertEqual(self.database.queue_size(), 0)

        self.database.start()

        try:
            users = list(qr)
            huey = User.create(name='huey')
            mickey = User.create(name='mickey')

            self.assertTrue(huey.id is not None)
            self.assertTrue(mickey.id is not None)
            self.assertEqual(self.database.queue_size(), 0)

        finally:
            self.database.stop()

    def create_thread(self, fn, *args):
        raise NotImplementedError

    def create_event(self):
        raise NotImplementedError

    def test_multiple_threads(self):
        def create_rows(idx, nrows):
            for i in range(idx, idx + nrows):
                User.create(name='u-%s' % i)

        total = self.n_threads * self.n_rows
        self.database.start()
        threads = [self.create_thread(create_rows, i, self.n_rows)
                   for i in range(0, total, self.n_rows)]
        [t.start() for t in threads]
        [t.join() for t in threads]

        self.assertEqual(User.select().count(), total)
        self.database.stop()

    def test_pause(self):
        event_a = self.create_event()
        event_b = self.create_event()

        def create_user(name, event, expect_paused):
            event.wait()
            if expect_paused:
                self.assertRaises(WriterPaused, lambda: User.create(name=name))
            else:
                User.create(name=name)

        self.database.start()

        t_a = self.create_thread(create_user, 'a', event_a, True)
        t_a.start()
        t_b = self.create_thread(create_user, 'b', event_b, False)
        t_b.start()

        User.create(name='c')
        self.assertEqual(User.select().count(), 1)

        # Pause operations but preserve the writer thread/connection.
        self.database.pause()

        event_a.set()
        self.assertEqual(User.select().count(), 1)
        t_a.join()

        self.database.unpause()
        self.assertEqual(User.select().count(), 1)

        event_b.set()
        t_b.join()
        self.assertEqual(User.select().count(), 2)

        self.database.stop()

    def test_stop_returns_true(self):
        self.database.start()
        User.create(name='a')
        self.assertTrue(self.database.stop())

    def test_restart(self):
        self.database.start()
        User.create(name='a')
        self.database.stop()
        self.database._results_timeout = 0.0001

        self.assertRaises(ResultTimeout, User.create, name='b')
        self.assertEqual(User.select().count(), 1)

        self.database.start()  # Will execute the pending "b" INSERT.
        self.database._results_timeout = None

        User.create(name='c')
        self.assertEqual(User.select().count(), 3)
        self.assertEqual(sorted(u.name for u in User.select()),
                         ['a', 'b', 'c'])

    def test_waiting(self):
        D = {}

        def create_user(name):
            D[name] = User.create(name=name).id

        threads = [self.create_thread(create_user, name)
                   for name in ('huey', 'charlie', 'zaizee')]
        [t.start() for t in threads]

        def get_users():
            D['users'] = [(user.id, user.name) for user in User.select()]

        tg = self.create_thread(get_users)
        tg.start()
        threads.append(tg)

        self.database.start()
        [t.join() for t in threads]
        self.database.stop()

        self.assertEqual(sorted(D), ['charlie', 'huey', 'users', 'zaizee'])

    def test_next_method(self):
        self.database.start()

        User.create(name='mickey')
        User.create(name='huey')
        query = iter(User.select().order_by(User.name))
        self.assertEqual(next(query).name, 'huey')
        self.assertEqual(next(query).name, 'mickey')
        self.assertRaises(StopIteration, lambda: next(query))

        self.assertEqual(
            next(self.database.execute_sql('PRAGMA journal_mode'))[0],
            'wal')

        self.database.stop()

    def test_atomic_unsupported_outside_transaction(self):
        self.assertRaises(ValueError, self.database.atomic)
        self.assertRaises(ValueError, self.database.transaction)
        self.assertRaises(ValueError, self.database.savepoint)

    def test_execute_transaction(self):
        self.database.start()

        def callback():
            User.create(name='huey')
            # Reads observe the unit's own uncommitted writes.
            self.assertEqual(User.select().count(), 1)
            User.create(name='mickey')
            return User.select().count()

        self.assertEqual(self.database.execute_transaction(callback), 2)
        self.assertEqual(
            sorted(u.name for u in User.select()), ['huey', 'mickey'])
        self.database.stop()

    def test_execute_transaction_savepoint(self):
        self.database.start()

        def callback():
            User.create(name='huey')
            try:
                with self.database.atomic():
                    User.create(name='mickey')
                    raise RuntimeError('rollback the savepoint')
            except RuntimeError:
                pass
            User.create(name='zaizee')
            return 'ok'

        self.assertEqual(self.database.execute_transaction(callback), 'ok')
        self.assertEqual(
            [u.name for u in User.select().order_by(User.name)],
            ['huey', 'zaizee'])
        self.database.stop()

    def test_execute_transaction_error(self):
        self.database.start()

        def bad_callback():
            User.create(name='huey')
            raise ValueError('nope')

        self.assertRaises(
            ValueError, self.database.execute_transaction, bad_callback)
        # The failed unit was rolled back.
        self.assertEqual(User.select().count(), 0)

        # The writer thread still processes subsequent work.
        User.create(name='mickey')
        self.assertEqual(
            self.database.execute_transaction(lambda: 'still ok'), 'still ok')
        self.assertEqual(User.select().count(), 1)
        self.database.stop()

    def test_execute_transaction_recursive(self):
        self.database.start()

        def callback():
            return self.database.execute_transaction(lambda: 'nested')

        self.assertRaises(
            RuntimeError, self.database.execute_transaction, callback)
        # The writer thread still processes subsequent work.
        self.assertEqual(self.database.execute_transaction(lambda: 'ok'), 'ok')
        self.database.stop()


class TestThreadedDatabaseThreads(BaseTestQueueDatabase, BaseTestCase):
    database_config = {'use_gevent': False}

    def tearDown(self):
        self.database._results_timeout = None
        super(TestThreadedDatabaseThreads, self).tearDown()

    def create_thread(self, fn, *args):
        t = threading.Thread(target=fn, args=args)
        t.daemon = True
        return t

    def create_event(self):
        return threading.Event()

    def test_timeout(self):
        @self.database.func()
        def slow(n):
            time.sleep(n)
            return 'slept %0.2f' % n

        self.database.start()

        # Make the result timeout very small, then call our function which
        # will cause the query results to time-out.
        self.database._results_timeout = 0.001
        def do_query():
            # Prepend a space so that we can force it through the threaded
            # pipeline, otherwise it would execute normally.
            cursor = self.database.execute_sql(' select slow(?)', (0.01,))
            self.assertEqual(cursor.fetchone()[0], 'slept 0.01')

        self.assertRaises(ResultTimeout, do_query)
        self.database.stop()

    def test_thread_helper_is_threading(self):
        from playhouse.sqliteq import GreenletHelper
        from playhouse.sqliteq import ThreadHelper
        self.assertIsInstance(self.database._thread_helper, ThreadHelper)
        self.assertNotIsInstance(self.database._thread_helper, GreenletHelper)

    def test_transaction_not_interleaved(self):
        self.database.start()
        in_txn = threading.Event()
        release = threading.Event()
        results = {}

        def callback():
            in_txn.set()
            self.assertTrue(release.wait(5))
            first = User.create(name='t1').id
            User.create(name='t2')
            last = User.create(name='t3').id
            return first, last

        def run_txn():
            results['txn'] = self.database.execute_transaction(callback)

        def run_write():
            results['write'] = User.create(name='w').id

        t_txn = threading.Thread(target=run_txn)
        t_txn.daemon = True
        t_txn.start()
        self.assertTrue(in_txn.wait(5))

        # Enqueue an ordinary write while the transaction unit is blocked
        # mid-callback. It must wait for the unit to finish.
        t_write = threading.Thread(target=run_write)
        t_write.daemon = True
        t_write.start()
        time.sleep(0.1)
        self.assertTrue(t_write.is_alive())

        release.set()
        t_txn.join(5)
        t_write.join(5)

        first, last = results['txn']
        # The unit's rows have contiguous ids -- no interleaved commits.
        self.assertEqual(last - first, 2)
        # The queued write ran after the transaction unit, in queue order.
        self.assertEqual(results['write'], last + 1)
        self.database.stop()

    def test_execute_transaction_timeout(self):
        self.database.start()
        started = threading.Event()
        release = threading.Event()

        def callback():
            started.set()
            release.wait(5)
            User.create(name='huey')
            return 'done'

        def release_soon():
            time.sleep(0.3)
            release.set()

        t = threading.Thread(target=release_soon)
        t.daemon = True
        t.start()

        # The caller's wait times out while the callback is still running.
        self.assertRaises(
            ResultTimeout,
            self.database.execute_transaction, callback, 0.1)
        self.assertTrue(started.is_set())

        # The callback is not killed: it finishes and commits.
        for _ in range(100):
            if User.select().count() == 1:
                break
            time.sleep(0.05)
        self.assertEqual(User.select().count(), 1)

        # The writer thread still processes subsequent work.
        self.assertEqual(self.database.execute_transaction(lambda: 'ok'), 'ok')
        self.database.stop()

    def test_pause_waits_for_transaction(self):
        self.database.start()
        started = threading.Event()
        release = threading.Event()
        results = {}

        def callback():
            started.set()
            release.wait(5)
            User.create(name='txn')
            return 'ok'

        def run_txn():
            results['txn'] = self.database.execute_transaction(callback)

        t_txn = threading.Thread(target=run_txn)
        t_txn.daemon = True
        t_txn.start()
        self.assertTrue(started.wait(5))

        t_pause = threading.Thread(target=self.database.pause)
        t_pause.daemon = True
        t_pause.start()
        time.sleep(0.1)
        # Pause cannot take effect until the unit completes.
        self.assertTrue(t_pause.is_alive())

        release.set()
        t_pause.join(5)
        t_txn.join(5)
        self.assertFalse(t_pause.is_alive())
        self.assertEqual(results['txn'], 'ok')

        self.assertRaises(WriterPaused, User.create, name='nope')
        self.database.unpause()
        User.create(name='after')
        self.assertEqual(
            sorted(u.name for u in User.select()), ['after', 'txn'])
        self.database.stop()

    def test_stop_waits_for_transaction(self):
        self.database.start()
        started = threading.Event()
        release = threading.Event()
        results = {}

        def callback():
            started.set()
            release.wait(5)
            User.create(name='txn')
            return 'committed'

        def run_txn():
            results['txn'] = self.database.execute_transaction(callback)

        t_txn = threading.Thread(target=run_txn)
        t_txn.daemon = True
        t_txn.start()
        self.assertTrue(started.wait(5))

        t_stop = threading.Thread(target=self.database.stop)
        t_stop.daemon = True
        t_stop.start()
        time.sleep(0.1)
        # Stop cannot take effect until the unit completes.
        self.assertTrue(t_stop.is_alive())

        release.set()
        t_stop.join(5)
        t_txn.join(5)
        self.assertFalse(t_stop.is_alive())
        self.assertEqual(results['txn'], 'committed')

        # The unit's commit is durable after the writer shut down.
        self.assertEqual([u.name for u in User.select()], ['txn'])


@skip_if(gevent is None, 'gevent not installed')
class TestThreadedDatabaseGreenlets(BaseTestQueueDatabase, BaseTestCase):
    database_config = {'use_gevent': True}
    n_rows = 10
    n_threads = 40

    def create_thread(self, fn, *args):
        return gevent.Greenlet(fn, *args)

    def create_event(self):
        return GreenEvent()

    def test_thread_helper_is_greenlet(self):
        from playhouse.sqliteq import GreenletHelper
        self.assertIsInstance(self.database._thread_helper, GreenletHelper)

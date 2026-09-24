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


@skip_if(gevent is None, 'gevent not installed')
class TestThreadedDatabaseGreenlets(BaseTestQueueDatabase, BaseTestCase):
    database_config = {'use_gevent': True}
    n_rows = 10
    n_threads = 40

    def create_thread(self, fn, *args):
        return gevent.Greenlet(fn, *args)

    def create_event(self):
        return GreenEvent()


class BaseTestExecuteTransaction(object):
    database_config = {}

    def setUp(self):
        super(BaseTestExecuteTransaction, self).setUp()
        User._meta.database = db
        with db:
            db.create_tables([User], safe=True)

        User._meta.database = \
                self.database = get_db(**self.database_config)
        self.database.start()

    def tearDown(self):
        super(BaseTestExecuteTransaction, self).tearDown()
        if not self.database.is_stopped():
            self.database.stop()
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

    def create_thread(self, fn, *args):
        raise NotImplementedError

    def create_event(self):
        raise NotImplementedError

    def sleep(self, n):
        raise NotImplementedError

    def test_read_own_writes(self):
        def txn():
            User.create(name='huey')
            User.create(name='mickey')
            # Reads within the callback see uncommitted writes.
            return [u.name for u in User.select().order_by(User.name)]

        result = self.database.execute_transaction(txn)
        self.assertEqual(result, ['huey', 'mickey'])
        self.assertEqual(
            [u.name for u in User.select().order_by(User.name)],
            ['huey', 'mickey'])

    def test_units_and_writes_do_not_interleave(self):
        inside_a = self.create_event()
        release_a = self.create_event()
        b_done = self.create_event()
        c_done = self.create_event()
        observed = {}
        results = {}

        def txn_a():
            User.create(name='a1')
            inside_a.set()
            release_a.wait(10)
            User.create(name='a2')
            return 'a'

        def txn_b():
            observed['b'] = sorted(u.name for u in User.select())
            b_done.set()
            return 'b'

        def run_txn(key, fn):
            results[key] = self.database.execute_transaction(fn)

        def write_c():
            User.create(name='c')
            c_done.set()

        ta = self.create_thread(run_txn, 'a', txn_a)
        ta.start()
        self.assertTrue(inside_a.wait(10))

        tb = self.create_thread(run_txn, 'b', txn_b)
        tc = self.create_thread(write_c)
        tb.start()
        tc.start()

        # Neither the queued transaction unit nor the queued ordinary write
        # may execute while txn_a is still running.
        self.assertFalse(b_done.wait(0.2))
        self.assertFalse(c_done.wait(0.2))

        release_a.set()
        ta.join()
        tb.join()
        tc.join()

        self.assertEqual(results['a'], 'a')
        self.assertEqual(results['b'], 'b')
        # txn_b ran after txn_a committed, so it could never observe a1
        # without a2.
        self.assertTrue(set(('a1', 'a2')) <= set(observed['b']))
        self.assertEqual(User.select().count(), 3)

    def test_nested_atomic_as_savepoint(self):
        def txn():
            User.create(name='outer')
            try:
                with self.database.atomic():
                    User.create(name='inner')
                    raise RuntimeError('force savepoint rollback')
            except RuntimeError:
                pass
            return sorted(u.name for u in User.select())

        self.assertEqual(self.database.execute_transaction(txn), ['outer'])
        self.assertEqual([u.name for u in User.select()], ['outer'])

    def test_atomic_unsupported_outside_transaction(self):
        self.assertRaises(ValueError, self.database.atomic)
        self.assertRaises(ValueError, self.database.transaction)
        self.assertRaises(ValueError, self.database.savepoint)

    def test_exception_rolls_back_and_writer_continues(self):
        class TxnError(Exception):
            pass

        def bad_txn():
            User.create(name='rolled-back')
            raise TxnError('original error')

        # The original exception propagates to the caller.
        self.assertRaises(TxnError, self.database.execute_transaction, bad_txn)
        self.assertEqual(User.select().count(), 0)

        # The writer continues to process subsequent units and writes.
        self.assertEqual(self.database.execute_transaction(lambda: 'ok'), 'ok')
        User.create(name='after')
        self.assertEqual([u.name for u in User.select()], ['after'])

    def test_recursive_execute_transaction_raises(self):
        def txn():
            self.database.execute_transaction(lambda: None)

        self.assertRaises(ValueError, self.database.execute_transaction, txn)
        self.assertEqual(User.select().count(), 0)

    def test_timeout_does_not_cancel_callback(self):
        finished = self.create_event()

        def slow_txn():
            self.sleep(0.3)
            User.create(name='slow')
            finished.set()
            return 'done'

        # The caller gives up waiting, but the callback is not killed.
        self.assertRaises(ResultTimeout, self.database.execute_transaction,
                          slow_txn, 0.05)

        # The callback runs to completion and its writes are committed.
        self.assertTrue(finished.wait(10))
        # The commit happens after the callback returns, so poll briefly.
        for _ in range(100):
            if User.select().count() == 1:
                break
            self.sleep(0.05)
        self.assertEqual([u.name for u in User.select()], ['slow'])

    def test_stop_waits_for_transaction_unit(self):
        inside = self.create_event()
        release = self.create_event()
        stopped = self.create_event()
        results = {}

        def txn():
            User.create(name='in-flight')
            inside.set()
            release.wait(10)
            return 'committed'

        def run_txn():
            results['txn'] = self.database.execute_transaction(txn)

        def run_stop():
            results['stopped'] = self.database.stop()
            stopped.set()

        t_txn = self.create_thread(run_txn)
        t_txn.start()
        self.assertTrue(inside.wait(10))

        t_stop = self.create_thread(run_stop)
        t_stop.start()

        # stop() cannot take effect while the unit is still executing.
        self.assertFalse(stopped.wait(0.2))

        release.set()
        t_txn.join()
        t_stop.join()

        self.assertEqual(results['txn'], 'committed')
        self.assertTrue(results['stopped'])
        self.assertEqual([u.name for u in User.select()], ['in-flight'])

    def test_pause_waits_for_transaction_unit(self):
        inside = self.create_event()
        release = self.create_event()
        paused = self.create_event()
        results = {}

        def txn():
            inside.set()
            release.wait(10)
            User.create(name='in-flight')
            return 'committed'

        def run_txn():
            results['txn'] = self.database.execute_transaction(txn)

        def run_pause():
            self.database.pause()
            paused.set()

        t_txn = self.create_thread(run_txn)
        t_txn.start()
        self.assertTrue(inside.wait(10))

        t_pause = self.create_thread(run_pause)
        t_pause.start()

        # pause() cannot take effect while the unit is still executing.
        self.assertFalse(paused.wait(0.2))

        release.set()
        t_txn.join()
        t_pause.join()

        self.assertEqual(results['txn'], 'committed')
        self.assertTrue(paused.is_set())

        # While paused, queued writes are rejected.
        self.assertRaises(WriterPaused, lambda: User.create(name='nope'))

        self.database.unpause()
        User.create(name='after')
        self.assertEqual(sorted(u.name for u in User.select()),
                         ['after', 'in-flight'])


class TestExecuteTransactionThreads(BaseTestExecuteTransaction, BaseTestCase):
    database_config = {'use_gevent': False}

    def create_thread(self, fn, *args):
        t = threading.Thread(target=fn, args=args)
        t.daemon = True
        return t

    def create_event(self):
        return threading.Event()

    def sleep(self, n):
        time.sleep(n)

    @skip_if(gevent is not None, 'gevent is installed')
    def test_gevent_mode_requires_gevent(self):
        # gevent mode must fail loudly rather than silently falling back to
        # ordinary threads.
        self.assertRaises(ImportError, get_db, use_gevent=True)


@skip_if(gevent is None, 'gevent not installed')
class TestExecuteTransactionGreenlets(BaseTestExecuteTransaction, BaseTestCase):
    database_config = {'use_gevent': True}

    def create_thread(self, fn, *args):
        return gevent.Greenlet(fn, *args)

    def create_event(self):
        return GreenEvent()

    def sleep(self, n):
        gevent.sleep(n)

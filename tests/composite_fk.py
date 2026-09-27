"""
Tests for CompositeForeignKeyField: relationships declared over existing local
fields, targeting a composite primary key or multi-column unique key.
"""
import unittest

from peewee import *
from playhouse.test_utils import assert_query_count


db = SqliteDatabase(':memory:', pragmas={'foreign_keys': 1})


class Document(Model):
    tenant_id = IntegerField()
    document_no = CharField()
    title = CharField(default='')

    class Meta:
        database = db
        primary_key = CompositeKey('tenant_id', 'document_no')


class LineItem(Model):
    tenant_id = IntegerField(null=True)
    document_no = CharField(null=True)
    qty = IntegerField(default=0)
    document = CompositeForeignKeyField(
        Document, fields=('tenant_id', 'document_no'), backref='lines',
        on_delete='CASCADE')

    class Meta:
        database = db


class Account(Model):
    k1 = IntegerField()
    k2 = CharField()
    name = CharField(default='')

    class Meta:
        database = db
        indexes = ((('k1', 'k2'), True),)


class Entry(Model):
    k1 = IntegerField(null=True)
    k2 = CharField(null=True)
    account = CompositeForeignKeyField(
        Account, fields=('k1', 'k2'), rel_fields=('k1', 'k2'), backref='entries')

    class Meta:
        database = db


class LazyLineItem(Model):
    tenant_id = IntegerField(null=True)
    document_no = CharField(null=True)
    document = CompositeForeignKeyField(
        Document, fields=('tenant_id', 'document_no'), backref='+',
        lazy_load=False)

    class Meta:
        database = db


class Node(Model):
    grp = IntegerField()
    code = CharField()
    parent_grp = IntegerField(null=True)
    parent_code = CharField(null=True)
    parent = CompositeForeignKeyField(
        'self', fields=('parent_grp', 'parent_code'), rel_fields=('grp', 'code'),
        backref='children')

    class Meta:
        database = db
        primary_key = CompositeKey('grp', 'code')


class BaseCompositeTestCase(unittest.TestCase):
    requires = ()

    def setUp(self):
        super(BaseCompositeTestCase, self).setUp()
        db.create_tables(self.requires)

    def tearDown(self):
        db.drop_tables(self.requires)
        super(BaseCompositeTestCase, self).tearDown()


class TestCompositeForeignKeyDDL(BaseCompositeTestCase):
    requires = (Document, LineItem, Entry, Account)

    def test_create_table_generates_multi_column_fk(self):
        ctx = LineItem._schema._create_table()
        sql, _ = ctx.query()
        self.assertIn(
            'FOREIGN KEY ("tenant_id", "document_no") '
            'REFERENCES "document" ("tenant_id", "document_no") '
            'ON DELETE CASCADE', sql)

    def test_constraint_name(self):
        class X(Model):
            a = IntegerField()
            b = IntegerField()
            r = CompositeForeignKeyField(
                Document, fields=('a', 'b'), constraint_name='fk_x_doc',
                backref='+')

            class Meta:
                database = db

        sql, _ = X._schema._create_table().query()
        self.assertIn('CONSTRAINT "fk_x_doc" FOREIGN KEY', sql)

    def test_sqlite_foreign_keys_enforced(self):
        # Valid reference inserts cleanly.
        Document.create(tenant_id=1, document_no='A')
        LineItem.create(tenant_id=1, document_no='A')
        # Invalid reference is rejected by the database.
        self.assertRaises(
            IntegrityError, LineItem.create, tenant_id=9, document_no='ZZ')

    def test_on_delete_cascade(self):
        Document.create(tenant_id=1, document_no='A')
        LineItem.create(tenant_id=1, document_no='A')
        Document.delete().execute()
        self.assertEqual(LineItem.select().count(), 0)


class TestCompositeForeignKeyRelations(BaseCompositeTestCase):
    requires = (Document, LineItem)

    def setUp(self):
        super(TestCompositeForeignKeyRelations, self).setUp()
        # Same document_no in two tenants: grouping must not cross tenants.
        self.d1 = Document.create(tenant_id=1, document_no='A', title='t1-A')
        self.d2 = Document.create(tenant_id=2, document_no='A', title='t2-A')
        self.d3 = Document.create(tenant_id=1, document_no='B', title='t1-B')
        self.l1 = LineItem.create(tenant_id=1, document_no='A', qty=1)
        self.l2 = LineItem.create(tenant_id=2, document_no='A', qty=2)
        self.l3 = LineItem.create(tenant_id=1, document_no='B', qty=3)

    def test_lazy_load(self):
        item = LineItem.get(LineItem.id == self.l1.id)
        with assert_query_count(1):
            self.assertEqual(item.document.title, 't1-A')
        # Cached after first load.
        with assert_query_count(0):
            self.assertEqual(item.document.title, 't1-A')

    def test_no_cross_tenant_leak(self):
        self.assertEqual(self.l1.document.title, 't1-A')
        self.assertEqual(self.l2.document.title, 't2-A')
        self.assertEqual(self.l3.document.title, 't1-B')

    def test_assign_instance(self):
        item = LineItem(qty=9)
        item.document = self.d2
        self.assertEqual((item.tenant_id, item.document_no), (2, 'A'))
        item.save()
        item = LineItem.get(LineItem.id == item.id)
        self.assertEqual(item.document.title, 't2-A')

    def test_assign_none_clears_key(self):
        item = LineItem.get(LineItem.id == self.l1.id)
        item.document = None
        self.assertIsNone(item.tenant_id)
        self.assertIsNone(item.document_no)
        self.assertIsNone(item.document)

    def test_assign_wrong_type(self):
        item = LineItem()
        self.assertRaises(TypeError, setattr, item, 'document', 42)

    def test_assign_via_constructor(self):
        item = LineItem(document=self.d2, qty=4)
        self.assertEqual((item.tenant_id, item.document_no), (2, 'A'))
        self.assertIs(item.document, self.d2)

    def test_assign_unsaved_or_partial_key(self):
        item = LineItem()
        unsaved = Document()
        self.assertRaises(ValueError, setattr, item, 'document', unsaved)
        partial = Document(tenant_id=1)
        self.assertRaises(ValueError, setattr, item, 'document', partial)

    def test_null_semantics(self):
        item = LineItem()
        # All components None: no relation, no query.
        with assert_query_count(0):
            self.assertIsNone(item.document)
        # Mixed None: explicit error, no lenient query.
        item.tenant_id = 1
        with assert_query_count(0):
            self.assertRaises(ValueError, lambda: item.document)
        item.tenant_id = None
        item.document_no = 'A'
        with assert_query_count(0):
            self.assertRaises(ValueError, lambda: item.document)

    def test_cache_invalidation_on_component_change(self):
        item = LineItem.get(LineItem.id == self.l1.id)
        self.assertEqual(item.document.title, 't1-A')
        item.tenant_id = 2
        # Cache must not return the stale object.
        self.assertEqual(item.document.title, 't2-A')
        item.document_no = 'nope'
        self.assertRaises(Document.DoesNotExist, lambda: item.document)

    def test_backref(self):
        self.assertEqual(
            sorted(l.qty for l in self.d1.lines), [1])
        self.assertEqual(
            sorted(l.qty for l in self.d2.lines), [2])
        # Backref is a real query that can be filtered further.
        self.assertEqual(
            [l.qty for l in self.d1.lines.where(LineItem.qty > 5)], [])

    def test_backref_on_unsaved_instance(self):
        doc = Document()
        self.assertEqual(list(doc.lines), [])

    def test_join(self):
        q = (LineItem.select()
             .join(Document)
             .where(Document.title == 't2-A'))
        self.assertEqual([l.qty for l in q], [2])

    def test_join_backref(self):
        q = (Document.select()
             .join(LineItem)
             .where(LineItem.qty == 2))
        self.assertEqual([d.title for d in q], ['t2-A'])

    def test_join_with_alias(self):
        D = Document.alias()
        q = (LineItem.select(LineItem, D)
             .join(D, on=LineItem.document)
             .where(D.title == 't1-B'))
        items = list(q)
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].qty, 3)
        self.assertEqual(items[0].document.title, 't1-B')

    def test_prefetch_forward(self):
        with assert_query_count(2):
            items = prefetch(LineItem.select().order_by(LineItem.id), Document)
            titles = [i.document.title for i in items]
        self.assertEqual(titles, ['t1-A', 't2-A', 't1-B'])

    def test_prefetch_backref(self):
        with assert_query_count(2):
            docs = prefetch(
                Document.select().order_by(Document.tenant_id,
                                           Document.document_no),
                LineItem)
            grouped = [(d.title, sorted(l.qty for l in d.lines))
                       for d in docs]
        self.assertEqual(grouped, [
            ('t1-A', [1]),
            ('t1-B', [3]),
            ('t2-A', [2]),
        ])

    def test_prefetch_join_strategy(self):
        with assert_query_count(2):
            items = prefetch(LineItem.select().order_by(LineItem.id), Document,
                             prefetch_type=PREFETCH_TYPE.JOIN)
            titles = [i.document.title for i in items]
        self.assertEqual(titles, ['t1-A', 't2-A', 't1-B'])

    def test_with_related(self):
        with assert_query_count(2):
            items = list(LineItem.select()
                         .order_by(LineItem.id)
                         .with_related(Load(LineItem.document)))
            titles = [i.document.title for i in items]
        self.assertEqual(titles, ['t1-A', 't2-A', 't1-B'])

    def test_with_related_backref(self):
        with assert_query_count(2):
            docs = list(Document.select()
                        .order_by(Document.tenant_id, Document.document_no)
                        .with_related(Load(Document.lines)))
            grouped = [(d.title, sorted(l.qty for l in d.lines))
                       for d in docs]
        self.assertEqual(grouped, [
            ('t1-A', [1]),
            ('t1-B', [3]),
            ('t2-A', [2]),
        ])


class TestCompositeForeignKeyUniqueTarget(BaseCompositeTestCase):
    requires = (Account, Entry)

    def test_unique_key_target(self):
        a1 = Account.create(k1=1, k2='x', name='one-x')
        a2 = Account.create(k1=2, k2='x', name='two-x')
        e1 = Entry.create(k1=1, k2='x')
        e2 = Entry.create(k1=2, k2='x')
        self.assertEqual(e1.account.name, 'one-x')
        self.assertEqual(e2.account.name, 'two-x')
        self.assertEqual([e.id for e in a1.entries], [e1.id])
        ctx = Entry._schema._create_table()
        sql, _ = ctx.query()
        self.assertIn(
            'FOREIGN KEY ("k1", "k2") REFERENCES "account" ("k1", "k2")', sql)


class TestCompositeForeignKeyLazyLoad(BaseCompositeTestCase):
    requires = (Document, LazyLineItem)

    def test_lazy_load_disabled_returns_key(self):
        Document.create(tenant_id=1, document_no='A')
        LazyLineItem.create(tenant_id=1, document_no='A')
        item = LazyLineItem.get()
        with assert_query_count(0):
            self.assertEqual(item.document, (1, 'A'))


class TestCompositeForeignKeySelfReference(BaseCompositeTestCase):
    requires = (Node,)

    def test_self_reference(self):
        n1 = Node.create(grp=1, code='a')
        n2 = Node.create(grp=1, code='b', parent_grp=1, parent_code='a')
        n3 = Node.create(grp=2, code='a')
        n4 = Node.create(grp=2, code='b', parent_grp=2, parent_code='a')

        self.assertEqual(n2.parent.code, 'a')
        self.assertEqual(n2.parent.grp, 1)
        self.assertEqual(n4.parent.grp, 2)
        self.assertIsNone(n1.parent)
        self.assertEqual([c.code for c in n1.children], ['b'])
        self.assertEqual([c.code for c in n3.children], ['b'])

        # Auto-join through the self-referential relation (via alias).
        Parent = Node.alias()
        q = (Node.select()
             .join(Parent, on=Node.parent)
             .where(Parent.code == 'a'))
        self.assertEqual(sorted(n.grp for n in q), [1, 2])

    def test_self_reference_alias_join(self):
        Node.create(grp=1, code='a')
        Node.create(grp=1, code='b', parent_grp=1, parent_code='a')
        Parent = Node.alias()
        q = (Node.select(Node, Parent)
             .join(Parent, on=Node.parent)
             .order_by(Node.code))
        rows = [(n.code, n.parent.code) for n in q]
        self.assertEqual(rows, [('b', 'a')])


class TestCompositeForeignKeyValidation(unittest.TestCase):
    def build(self, **kwargs):
        class M(Model):
            a = IntegerField()
            b = IntegerField()
            rel = CompositeForeignKeyField(**kwargs)

            class Meta:
                database = db

    def test_unknown_local_field(self):
        self.assertRaises(
            ValueError, CompositeForeignKeyField(
                Document, fields=('a', 'nope')).bind, Document, 'rel')

    def test_field_count_mismatch(self):
        self.assertRaises(
            ValueError, CompositeForeignKeyField(
                Document, fields=('a', 'b', 'c'),
                rel_fields=('tenant_id', 'document_no')).bind, Document, 'rel')

    def test_too_few_fields(self):
        self.assertRaises(
            ValueError, CompositeForeignKeyField(
                Document, fields=('a',)).bind, Document, 'rel')

    def test_unknown_rel_field(self):
        self.assertRaises(
            ValueError, CompositeForeignKeyField(
                Document, fields=('a', 'b'),
                rel_fields=('tenant_id', 'nope')).bind, Document, 'rel')

    def test_non_unique_target(self):
        self.assertRaises(
            ValueError, CompositeForeignKeyField(
                Document, fields=('a', 'b'),
                rel_fields=('tenant_id', 'title')).bind, Document, 'rel')

    def test_no_composite_pk_requires_rel_fields(self):
        self.assertRaises(
            ValueError, CompositeForeignKeyField(
                Account, fields=('a', 'b')).bind, Document, 'rel')

    def test_string_model_must_be_self(self):
        self.assertRaises(
            ValueError, CompositeForeignKeyField, 'document', ('a', 'b'))


class TestCompositeForeignKeyInheritance(BaseCompositeTestCase):
    requires = (Document, LineItem)

    def test_subclass_inherits_relation(self):
        class SubLine(LineItem):
            class Meta:
                database = db

        self.assertIn(SubLine.document, SubLine._meta.refs)
        doc = Document.create(tenant_id=1, document_no='A')
        db.create_tables([SubLine])
        try:
            item = SubLine.create(tenant_id=1, document_no='A')
            self.assertEqual(item.document.document_no, 'A')
        finally:
            db.drop_tables([SubLine])

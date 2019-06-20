"""Defines methods to interact with the citation database"""

from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

from builtins import str
from builtins import object

import logging
import datetime as dt
import json
import os
import re
from collections import namedtuple

import sqlalchemy
from sqlalchemy import and_, update
from sqlalchemy.schema import UniqueConstraint
from sqlalchemy.inspection import inspect

from .database import engine, Session, Document, Journal, Snippet, Specimen




logger = logging.getLogger('speciminer')
logger.info('Loading queries.py')




class _Query(object):

    def __init__(self, max_length=5000, bulk=False):
        self.session = None
        self.length = 0
        self.max_length = max_length
        # Bulk save parameters
        self.bulk = bulk
        self._insert_mappings = {}
        self._update_mappings = {}
        self._objects = {}
        self._mappers = {}


    def __getattr__(self, attr):
        try:
            return object.__getattr__(attr)
        except AttributeError:
            if self.session is None:
                self.new()
            return getattr(self.session, attr)


    @staticmethod
    def row2dict(row):
        """Converts a row to a dict"""
        return {k: v for k, v in vars(row).items() if not k.startswith('_')}


    def get_unique(self, table, primary_only=False):
        """Identifies the fields in the unique constraint for a table"""
        if not primary_only:
            try:
                args = table.__table_args__
            except AttributeError:
                pass
            else:
                for arg in args:
                    if isinstance(arg, UniqueConstraint):
                        return [k.name for k in arg]
        return [k.name for k in inspect(table).primary_key]


    def new(self):
        if self.session is not None:
            self.session.commit().close()
        self.session = Session()
        return self


    def iterate(self):
        self.length += 1
        if self.length >= self.max_length:
            self.commit()


    def unique(self, table, primary_only=False, **kwargs):
        return {k: kwargs.get(k) for k in self.get_unique(table, primary_only=primary_only)}


    def key(self, table, primary_only=False, **kwargs):
        key = self.unique(table, primary_only=primary_only, **kwargs)
        key['table'] = table.__tablename__
        return json.dumps(key, sort_keys=True).lower()


    def get(self, table, **kwargs):
        unique = self.unique(table, **kwargs)
        result = self.query(table).filter_by(**unique).first()
        if not result:
            key = self.key(table, **kwargs)
            for mapping in [self._insert_mappings,
                            self._update_mappings,
                            self._objects]:
                try:
                    return mapping.get(table.__tablename__, mapping)[key]
                except KeyError as e:
                    continue
        return result


    def exists(self, table, **kwargs):
        return bool(self.get(table, **kwargs))


    def safe_add(self, table, **kwargs):
        result = self.get(table, **kwargs)
        return result if result else self.add(table, **kwargs)


    def add(self, table, **kwargs):
        if self.session is None:
            self.new()
        if not self.bulk:
            obj = table(**kwargs)
            self.session.add(obj)
            self.session.flush()
        elif isinstance(table, sqlalchemy.ext.declarative.DeclarativeMeta):
            obj = kwargs
            name = table.__tablename__
            key = self.key(table, **kwargs)
            self._mappers[name] = table
            self._insert_mappings.setdefault(name, {})[key] = obj
        else:
            obj = table(**kwargs)
            key = self.key(table, **kwargs)
            self._objects[key] = obj
        self.iterate()
        return obj


    def insert(self, table, **kwargs):
        session = self.new() if self.session is None else self.session
        return self.add(table, **kwargs)


    def keys_to_cols(self, table, **kwargs):
        stmt = {}
        for col in getattr(table, 'c'):
            key = str(col).split('.')[-1]
            try:
                stmt[col] = kwargs.pop(key)
            except KeyError:
                pass
        if kwargs:
            raise ValueError('WHERE statment could not incorporate the'
                             ' following keys: {}'.format(kwargs))
        return stmt


    def update_where(self, table, where, **kwargs):
        session = self.new() if self.session is None else self.session
        session.commit()
        if isinstance(where, dict):
            eq = self.keys_to_cols(table, **where)
            where = [k == v for k, v in eq.items()]
        if len(where) > 1:
            where = and_(*where)
        else:
            where = where[0]
        stmt = update(table).where(where).values(**kwargs)
        session.execute(stmt)
        session.commit()
        return kwargs


    def update(self, table, **kwargs):
        where = kwargs.pop('where', None)
        if where:
            return self.update_where(table.__table__, where, **kwargs)
        else:
            session = self.new() if self.session is None else self.session
            name = table.__tablename__
            key = self.key(table, primary_only=True, **kwargs)
            self._mappers[name] = table
            self._update_mappings.setdefault(name, {})[key] = kwargs
            self.iterate()
            return kwargs


    def upsert(self, table, primary_key='id', **kwargs):
        session = self.new() if self.session is None else self.session
        if not isinstance(primary_key, list):
            primary_key = [primary_key]
        fltr = {k: kwargs[k] for k in primary_key if kwargs.get(k) is not None}
        if fltr and self.session.query(table).filter_by(**fltr).first():
            return self.update(table, **kwargs)
        else:
            return self.insert(table, **kwargs)


    def delete(self, table, **kwargs):
        session = self.new() if self.session is None else self.session
        #keys = [k.name for k in inspect(table).primary_key]
        #fltr = {k: kwargs.get(k) for k in keys}
        session.query(table).filter_by(**kwargs).delete()
        self.iterate()


    def commit(self):
        if self.session is not None and self.length:
            # Save bulk transactions
            if self._objects:
                for key, vals in self._objects.items():
                    vals = list(vals.values())
                    logger.debug('Saving {:,} records in {}'.format(len(vals), key))
                    self.session.bulk_save_objects(self._mappers[key], vals)
                self._objects = {}
            if self._insert_mappings:
                for key, vals in self._insert_mappings.items():
                    vals = list(vals.values())
                    logger.debug('Inserting {:,} records into {}'.format(len(vals), key))
                    self.session.bulk_insert_mappings(self._mappers[key], vals)
                self._insert_mappings = {}
            if self._update_mappings:
                for key, vals in self._update_mappings.items():
                    vals = list(vals.values())
                    logger.debug('Updating {:,} records in {}'.format(len(vals), key))
                    self.session.bulk_update_mappings(self._mappers[key], vals)
                self._update_mappings = {}
            self.session.commit()
            # Notify user of size and time of commit
            now = dt.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
            print('Committed changes (n={}, {})'.format(self.length, now))
            self.length = 0
        return self


    def close(self):
        if self.session is not None:
            self.session.close()
        self.session = None


    def drop(self, table):
        table.__table__.drop(engine)


class Query(_Query):

    def __init__(self, *args, **kwargs):
        super(Query, self).__init__(*args, **kwargs)


    def get_document(self, gdd_id):
        """Checks if a document with given gdd_id has already been checked"""
        return self.query(Document.id).filter(Document.id == gdd_id).first()


    def read_bibjson(self, fp):
        """Reads bibliography data the bibjson file provided by GeoDeepDive"""
        for bib in json.load(open(fp, 'r')):
            doi = [b['id'] for b in bib.get('identifier', []) if b['type'] == 'doi']
            kwargs = {
                'id': bib['_gddid'],
                'doi': doi[0] if doi else None,
                'title': bib.get('title'),
                'journal': bib.get('journal', {}).get('name'),
                'year': bib.get('year')
            }
            self.upsert(Document, **kwargs)
            if kwargs['journal']:
                self.upsert(Journal, 'title', **{'title': kwargs['journal']})
        self.commit().close()


    def get_topics(self, snippet_id):
        """Gets the topic of the journal or paper associated with a snippet"""
        query = self.query(Document.topic.label('doc_topic'),
                           Journal.topic.label('jour_topic'),
                           Journal.title) \
                    .join(Journal, Journal.title == Document.journal) \
                    .join(Snippet, Snippet.doc_id == Document.id) \
                    .filter(Snippet.id == snippet_id)
        row = query.first()
        if row is not None:
            return [s.strip('?') for s in [row.doc_topic, row.jour_topic] if s]
        return []

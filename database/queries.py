"""Defines methods to interact with the database of citations"""

import datetime as dt
import json
import os
import re

from sqlalchemy import and_

from database import Session, Document, Journal, Snippet, Specimen


class _Query(object):

    def __init__(self, max_length=5000):
        self.session = None
        self.length = 0
        self.max_length = max_length


    def __getattr__(self, attr):
        try:
            return object.__getattr__(attr)
        except AttributeError:
            if self.session is None:
                self.new()
            return getattr(self.session, attr)


    def new(self):
        if self.session is not None:
            self.session.commit().close()
        self.session = Session()
        return self


    def iterate(self):
        self.length += 1
        if self.length >= self.max_length:
            self.commit()


    def add(self, transaction):
        if self.session is None:
            self.new()
        self.session.add(transaction)
        self.iterate()


    def insert(self, tableclass, **kwargs):
        session = self.new() if self.session is None else self.session
        rec = tableclass(**kwargs)
        self.add(rec)
        return rec


    def upsert(self, tableclass, primary_key='id', **kwargs):
        session = self.new() if self.session is None else self.session
        if not isinstance(primary_key, list):
            primary_key = [primary_key]
        fltr = {k: kwargs[k] for k in primary_key if kwargs.get(k) is not None}
        if fltr and self.session.query(tableclass).filter_by(**fltr).first():
            return self.update(tableclass, primary_key[0], **kwargs)
        else:
            return self.insert(tableclass, **kwargs)


    def update(self, tableclass, primary_key='id', **kwargs):
        session = self.new() if self.session is None else self.session
        table = tableclass.__table__
        self.iterate()
        return session.execute(
                table.update() \
                     .where(getattr(table.c, primary_key) == kwargs[primary_key]) \
                     .values(**kwargs))


    def delete(self, tableclass, primary_key='id', **kwargs):
        session = self.new() if self.session is None else self.session
        session.query(tableclass).filter(primary_key=kwargs['primary_key']).delete()
        self.iterate()


    def commit(self):
        if self.session is not None:
            self.session.commit()
        self.length = 0
        now = dt.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        print 'Committed changes ({})'.format(now)
        return self


    def close(self):
        if self.session is not None:
            self.session.close()
        self.session = None


class Query(_Query):

    def __init__(self, *args, **kwargs):
        super(Query, self).__init__(*args, **kwargs)


    def read_bibjson(self, fp):
        """Reads bibliography data the bibjson file provided by GeoDeepDive"""
        for bib in json.load(open(fp, 'rb')):
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



if __name__ == '__main__':
    #Query().read_bibjson(os.path.join('output', 'bibjson'))
    print Query().get_topics(37328)

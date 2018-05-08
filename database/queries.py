"""Defines methods to interact with the database of citations"""

import re

from sqlalchemy import and_

from database import Session, Document, Journal, Snippet, Specimen


class Query(object):

    def __init__(self):
        self.session = None
        self.length = 0
        self.max_length = 250


    def new(self):
        if self.session is not None:
            self.session.commit().close()
        self.session = Session()
        return self.session


    def add(self, transaction):
        if self.session is None:
            self.session = self.new()
        self.session.add(transaction)
        # Commit a batch once the queue hits a certain size
        self.length += 1
        if self.length >= self.max_length:
            self.commit()


    def commit(self):
        if self.session is not None:
            self.session.commit()
        self.length = 0
        return self.session


    def close(self):
        if self.session is not None:
            self.session.close()
        self.session = None


    def get_document(self, doc_id=None):
        session = self.new() if self.session is None else self.session
        if doc_id is None:
            return session.query(Document).all()
        return session.query(Document).filter(Document.id == doc_id).first()


    def get_journal(self, title=None):
        session = self.new() if self.session is None else self.session
        if title is None:
            return session.query(Journal).all()
        return session.query(Journal).filter(Journal.title == title).first()


    def get_snippets(self, rec_id):
        session = self.new() if self.session is None else self.session
        rows = session.query(Snippet).filter(Snippet.rec_id == rec_id).all()
        return [row.snippet for row in rows]


    def get_specimen(self, spec_num=None, doc_id=None):
        session = self.new() if self.session is None else self.session
        if spec_num is None and doc_id is None:
            return session.query(Specimen).all()
        return session.query(Specimen).filter(and_(Specimen.spec_num == spec_num,
                                                   Specimen.doc_id == doc_id)).all()


    def update_document(self, doc_id, **kwargs):
        session = self.new() if self.session is None else self.session
        table = Document.__table__
        session.execute(table.update().where(table.c.id == doc_id).values(**kwargs))


    def update_specimen(self, rec_id, **kwargs):
        session = self.new() if self.session is None else self.session
        table = Specimen.__table__
        session.execute(table.update().where(table.c.id == rec_id).values(**kwargs))


    def add_journal(self, title):
        session = self.new() if self.session is None else self.session
        journal = Journal(title=title)
        if not self.get_journal(title):
            self.add(journal)


    def add_snippet(self, doc_id, snippet, rec_id=None):
        session = self.new() if self.session is None else self.session
        if not self.get_document(doc_id):
            doc = Document(id=doc_id)
            self.add(doc)
        warning = True if re.search(r'\* \d', snippet) else False
        rec = Snippet(snippet=snippet, doc_id=doc_id, rec_id=rec_id, warning=warning)
        self.add(rec)


    def add_citation(self, doc_id, spec_num, snippets, topic=None, num_specimens=None):
        session = self.new() if self.session is None else self.session
        # Insert document
        if not self.get_document(doc_id):
            doc = Document(id=doc_id, topic=topic, num_specimens=num_specimens)
            self.add(doc)
        # Insert specimen
        rec_id = spec_num
        if spec_num is not None:
            rec = Specimen(doc_id=doc_id, spec_num=spec_num)
            self.add(rec)
            session.flush()
            rec_id = rec.id
        # Insert snippets
        rows = []
        for snippet in set(snippets):
            self.add_snippet(doc_id, snippet, rec_id)

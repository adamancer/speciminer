import os
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (Column, Boolean, Integer, String, ForeignKey, create_engine)
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import UniqueConstraint


Base = declarative_base()


class Journal(Base):
    __tablename__ = 'journals'

    title = Column(String(collation='nocase'), primary_key=True)
    topic = Column(String)


class Document(Base):
    __tablename__ = 'documents'

    id = Column(String, primary_key=True)
    doi = Column(String)
    title = Column(String(collation='nocase'))
    journal = Column(String(collation='nocase'))
    year = Column(String)
    topic = Column(String)
    num_specimens = Column(Integer)


class Snippet(Base):
    __tablename__ = 'snippets'

    id = Column(Integer, primary_key=True)
    doc_id = Column(String, ForeignKey('documents.id'), nullable=False)
    snippet = Column(String(collation='nocase'), nullable=False)
    manual = Column(String(collation='nocase'))
    notes = Column(String(collation='nocase'))
    # Ensure that snippets aren't being duplicated
    __table_args__ = (
        UniqueConstraint('doc_id', 'snippet', name='_doc_snippet'),
    )


class Specimen(Base):
    __tablename__ = 'specimens'

    id = Column(Integer, primary_key=True)
    snippet_id = Column(String, ForeignKey('snippets.id'), nullable=False)
    verbatim = Column(String(collation='nocase'), nullable=False)
    spec_num = Column(String(collation='nocase'), nullable=False)


class Link(Base):
    __tablename__ = 'links'

    id = Column(Integer, primary_key=True)
    doc_id = Column(String, ForeignKey('documents.id'), nullable=False)
    spec_num = Column(String(collation='nocase'), nullable=False)
    ezid = Column(String)
    match_quality = Column(String(collation='nocase'))
    department = Column(String(collation='nocase'))
    notes = Column(String(collation='nocase'))
    # Ensure that specimens aren't being duplicated
    __table_args__ = (
        UniqueConstraint('doc_id', 'spec_num', name='_doc_spec'),
    )


# Get the full path to the output file
root = os.path.splitdrive(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))[1].lstrip('/\\')
root = root.replace('\\', '/')
try:
    os.mkdir(os.path.join(root, 'output'))
except OSError:
    pass
engine = create_engine('/'.join(['sqlite:///', root, 'output', 'specimens.db']))
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)

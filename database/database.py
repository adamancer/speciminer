import os
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (Column, Boolean, Integer, String, ForeignKey, create_engine)
from sqlalchemy.orm import sessionmaker


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
    rec_id = Column(Integer, ForeignKey('specimens.id'))
    snippet = Column(String(collation='nocase'), nullable=False)
    warning = Column(Boolean)


class Specimen(Base):
    __tablename__ = 'specimens'

    id = Column(Integer, primary_key=True)
    doc_id = Column(String, ForeignKey('documents.id'), nullable=False)
    spec_num = Column(String(collation='nocase'), nullable=False)
    ezid = Column(String)


# Get the full path to the output file
try:
    os.mkdir('output')
except OSError:
    pass
root = os.path.splitdrive(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))[1].lstrip('/\\')
root = root.replace('\\', '/')
engine = create_engine('/'.join(['sqlite:///', root, 'output', 'specimens.db']))
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)

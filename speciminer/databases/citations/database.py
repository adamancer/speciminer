"""Defines tables in the citation database"""
import logging
import os

from sqlalchemy import (
    Column,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from nmnh_ms_tools.config import CONFIG
from nmnh_ms_tools.databases.helpers import init_helper




logger = logging.getLogger(__name__)
Base = declarative_base()
Session = sessionmaker()




class Journal(Base):
    __tablename__ = 'journals'

    title = Column(String(collation='nocase'), primary_key=True)
    topic = Column(String)




class Document(Base):
    """Stores information about a publication or document"""
    __tablename__ = 'documents'

    url = Column(String, primary_key=True)
    publication_url = Column(String(collation='nocase'))
    kind = Column(String(collation='nocase'))
    authors = Column(String(collation='nocase'))
    title = Column(String(collation='nocase'), ForeignKey('journals.title'))
    year = Column(String)
    publication = Column(String(collation='nocase'))
    volume = Column(String(collation='nocase'))
    number = Column(String(collation='nocase'))
    pages = Column(String(collation='nocase'))
    doi = Column(String)
    topic = Column(String)
    num_specimens = Column(Integer)
    num_snippets = Column(Integer)




class Snippet(Base):
    """Stores information about a snippet from a document"""
    __tablename__ = 'snippets'

    id = Column(String, primary_key=True)
    doc_url = Column(String, ForeignKey('documents.url'), nullable=False)
    page_id = Column(String)
    snippet = Column(String(collation='nocase'), nullable=False)
    notes = Column(String(collation='nocase'))
    # Ensure that snippets aren't being duplicated
    __table_args__ = (
        UniqueConstraint('doc_url', 'page_id', 'snippet', name='_id_snippet'),
        Index('idx_snippets_doc_url', 'doc_url'),
        #Index('idx_snippets_page_id', 'page_id')
    )




class Specimen(Base):
    """Stores information about a specimen number found in a document"""
    __tablename__ = 'specimens'

    id = Column(String, primary_key=True)
    snippet_id = Column(String, ForeignKey('snippets.id'), nullable=False)
    verbatim = Column(String(collation='nocase'), nullable=False)
    spec_num = Column(String(collation='nocase'), nullable=False)
    __table_args__ = (
        UniqueConstraint('snippet_id', 'verbatim', 'spec_num', name='_spec_snippet'),
        Index('idx_specimens_snippet_id', 'snippet_id')
    )




class Link(Base):
    """Stores link to a catalog record"""
    __tablename__ = 'links'

    id = Column(String, primary_key=True)
    doc_url = Column(String, ForeignKey('documents.url'), nullable=False)
    verbatim = Column(String(collation='nocase'), nullable=False)
    spec_num = Column(String(collation='nocase'), nullable=False)
    ezid = Column(String)
    match_quality = Column(String(collation='nocase'))
    department = Column(String(collation='nocase'))
    has_similar_ref = Column(Integer)
    num_snippets = Column(Integer)
    notes = Column(String(collation='nocase'))
    # Ensure that specimens aren't being duplicated
    __table_args__ = (
        UniqueConstraint('doc_url', 'verbatim', 'spec_num', name='_doc_spec'),
    )




class DarwinCore(Base):
    """Stores basic DarwinCore metadata for a catalog record"""
    __tablename__ = 'dwc'

    id = Column(String, primary_key=True)
    higher_classification = Column(String(collation='nocase'))
    scientific_name = Column(String(collation='nocase'))
    type_status = Column(String(collation='nocase'))
    higher_geography = Column(String(collation='nocase'))
    verbatim_locality = Column(String(collation='nocase'))




def init_db(fp=None, tables=None):
    """Creates the database based on the given path"""
    global Base
    global Session
    if fp is None:
        fp = CONFIG.data.citations
    init_helper(fp, base=Base, session=Session, tables=tables)

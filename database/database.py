import os

import yaml
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (Column, Boolean, Integer, String, ForeignKey, create_engine)
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import Index, UniqueConstraint


Base = declarative_base()


class Journal(Base):
    __tablename__ = 'journals'

    title = Column(String(collation='nocase'), primary_key=True)
    topic = Column(String)


class Document(Base):
    __tablename__ = 'documents'

    id = Column(String, primary_key=True)
    source = Column(String)
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
    page_id = Column(String)
    start = Column(Integer)
    snippet = Column(String(collation='nocase'), nullable=False)
    manual = Column(String(collation='nocase'))
    notes = Column(String(collation='nocase'))
    # Ensure that snippets aren't being duplicated
    __table_args__ = (
        UniqueConstraint('doc_id', 'page_id', 'snippet', name='_id_snippet'),
        Index('idx_snippets_doc_id', 'doc_id'),
        Index('idx_snippets_page_id', 'page_id')
    )


class Specimen(Base):
    __tablename__ = 'specimens'

    id = Column(Integer, primary_key=True)
    snippet_id = Column(String, ForeignKey('snippets.id'), nullable=False)
    verbatim = Column(String(collation='nocase'), nullable=False)
    spec_num = Column(String(collation='nocase'), nullable=False)
    __table_args__ = (
        Index('idx_specimens_snippet_id', 'snippet_id'),
    )


class Link(Base):
    __tablename__ = 'links'

    id = Column(Integer, primary_key=True)
    doc_id = Column(String, ForeignKey('documents.id'), nullable=False)
    spec_num = Column(String(collation='nocase'), nullable=False)
    corrected = Column(String(collation='nocase'))
    ezid = Column(String)
    match_quality = Column(String(collation='nocase'))
    department = Column(String(collation='nocase'))
    num_snippets = Column(Integer)
    notes = Column(String(collation='nocase'))
    # Ensure that specimens aren't being duplicated
    __table_args__ = (
        UniqueConstraint('doc_id', 'spec_num', name='_doc_spec'),
    )


class Part(Base):
    __tablename__ = 'parts'

    id = Column(String, primary_key=True)
    source = Column(String)
    item_id = Column(Integer)
    part_id = Column(Integer)
    first_page = Column(Integer)
    min_page = Column(Integer)
    max_page = Column(Integer)
    # Define indexes
    __table_args__ = (
        Index('idx_parts_item_part_page_ids', 'item_id', 'part_id', 'min_page', 'max_page'),
    )


class Taxon(Base):
    __tablename__ = 'taxa'

    id = Column(Integer, primary_key=True)
    source_id = Column(String)
    taxon = Column(String)
    # Define indexes
    __table_args__ = (
        UniqueConstraint('source_id', 'taxon', name='_src_taxa'),
        Index('idx_taxa_source_id', 'source_id')
    )


def std_path(path, delim=None):
    """Standardizes a path to the the OS path delimiter"""
    if delim is None:
        delim = os.sep
    return path.replace('/', delim).replace('\\', delim)


def get_path(relpath, delim='/'):
    """Converts a relative to an absolute path"""
    relpath = std_path(relpath, delim=delim)
    root = os.path.dirname(os.path.realpath(__file__))
    path = os.path.realpath(os.path.join(root, relpath))
    return path




params = yaml.load(open(get_path('config.yml'), 'rb'))
if params['use'] == 'sqlite':
    path = get_path(params['sqlite']['path'])
    try:
        os.makedirs(os.path.dirname(path))
    except OSError:
        pass
    path = os.path.splitdrive(path)[1].lstrip('/\\')
    engine = create_engine('/'.join(['sqlite:///', path]))
elif params['use'] == 'mysql':
    #from sshtunnel import SSHTunnelForwarder
    params = params['mysql']
    #server = SSHTunnelForwarder(
    #    params['ssh_host'],
    #    ssh_username=params['ssh_user'],
    #    ssh_password=params['ssh_pass'],
    #    remote_bind_address=(params['db_host'], params['db_port'])
    #)
    #server.start()
    #params.update(db_host=server.local_bind_host,
    #              db_port=server.local_bind_port)
    mask = 'mysql+pymysql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}'
    engine = create_engine(mask.format(**params))
else:
    raise KeyError('Unrecognized backend: %s', params['use'])
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)

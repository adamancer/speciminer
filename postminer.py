"""Extends and summarizes data mined from GeoDeepDive"""

import re

import unidecode

from sqlalchemy import and_, or_

from documents import get_document
from specimens import get_specimens, filter_records, get_keywords
from database.database import Document, Journal, Link, Snippet, Specimen
from database.queries import Query


if __name__ == '__main__':
    db = Query(5000)
    # Clear links if desired
    if True:
        print 'Clearing old links...'
        for row in db.query(Link) \
                     .filter(and_(Link.match_quality != 'MATCHED_MANUALLY',
                                  Link.match_quality != None)).all():
            db.update(Link,
                      id=row.id,
                      ezid=None,
                      match_quality=None,
                      department=None)
    db.commit()
    # Zero specimen counts in documents, then recalculate for the data
    # already in links
    print 'Recalculating document counts...'
    for row in db.query(Document).filter(Document.num_specimens > 0).all():
        db.update(Document, id=row.id, num_specimens=0)
    docs = {}
    for row in db.query(Link.doc_id).filter(Link.ezid != None).all():
        docs.setdefault(row.doc_id, []).append(1)
    for doc_id, rows in docs.iteritems():
        db.update(Document, id=doc_id, num_specimens=len(rows))
    db.commit()
    # Match specimens to catalog records
    print 'Matching specimen numbers to collections records...'
    db.max_length = 250
    for row in db.query(Link) \
                 .filter(Link.match_quality == None) \
                 .order_by(Link.doc_id) \
                 .all():
        print 'Matching {}'.format(row.spec_num)
        match_quality = None
        records = get_specimens(row.spec_num)
        # Get all snippets from the paper that mention this specimen
        query = db.query(Link.spec_num, Snippet.snippet) \
                  .join(Specimen, Specimen.spec_num == Link.spec_num) \
                  .join(Snippet, and_(Snippet.doc_id == Link.doc_id,
                                      Specimen.snippet_id == Snippet.id)) \
                  .filter(and_(Link.spec_num == row.spec_num,
                               Link.doc_id == row.doc_id))
        snippets = [r.snippet.strip('... ') for r in query.all()]
        keywords = get_keywords(' '.join(snippets).lower())
        matches = filter_records(records, row.spec_num, keywords=keywords)
        match_quality = 'MATCHED_SNIPPET'
        if not matches:
            # Check results against paper
            doc = db.query(Document.title,
                           Document.topic.label('doc_topic'),
                           Journal.topic.label('jour_topic')) \
                    .join(Link, Link.doc_id == Document.id) \
                    .join(Journal, Document.journal == Journal.title) \
                    .filter(Link.id == row.id) \
                    .first()
            if doc is None:
                continue
            # Check results against title of paper
            if doc.title:
                keywords = get_keywords(doc.title.lower())
                matches = filter_records(records, row.spec_num, keywords=keywords)
                match_quality = 'MATCHED_TITLE'
            # Check results against topic of paper, then journal
            if doc.doc_topic and not matches:
                matches = filter_records(records, row.spec_num, keywords=keywords, dept=doc.doc_topic.rstrip('?'))
                match_quality = 'MATCHED_PAPER_TOPIC'
            if doc.jour_topic and not matches:
                matches = filter_records(records, row.spec_num, keywords=keywords, dept=doc.jour_topic.rstrip('?'))
                match_quality = 'MATCHED_JOURNAL_TOPIC'
        # Record matches if any of the quality checks yield a hit
        if matches:
            ezids = ' | '.join(matches)
            # Get department of matches
            records = [rec for rec in records if rec['occurrenceID'] in ezids]
            depts = list(set([rec['collectionCode'] for rec in records]))
            db.update(Link,
                      id=row.id,
                      ezid=ezids,
                      match_quality=match_quality,
                      department=depts[0])
        else:
            db.update(Link, id=row.id, match_quality='NO_MATCH')
        # Update journals
        if db.length > 200:
            docs = {}
            for row in db.query(Link.doc_id).filter(Link.ezid != None).all():
                docs.setdefault(row.doc_id, []).append(1)
            for doc_id, rows in docs.iteritems():
                db.update(Document, id=doc_id, num_specimens=len(rows))
    db.commit()
    # Update journals
    print 'Recalcualting document counts...'
    docs = {}
    for row in db.query(Link.doc_id).filter(Link.ezid != None).all():
        docs.setdefault(row.doc_id, []).append(1)
    for doc_id, rows in docs.iteritems():
        db.update(Document, id=doc_id, num_specimens=len(rows))
    db.commit().close()

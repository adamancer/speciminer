"""Extends and summarizes data mined from GeoDeepDive"""

import re

from sqlalchemy import and_

from documents import get_document
from specimens import get_specimens, filter_records
from database.database import Document, Journal, Link, Snippet, Specimen
from database.queries import Query


def get_keywords(text, minlen=5, blacklist=None):
    if blacklist is None:
        blacklist = ['genus', 'sp']
    keywords = []
    for word in text.split():
        word = word.strip('.')
        if (re.search('^[A-Za-z]+$', word)
            and len(word) >= minlen
            and word not in blacklist):
            keywords.append(word)
    return set(keywords)


if __name__ == '__main__':
    db = Query(50)
    for row in db.query(Link) \
                 .filter(Link.ezid == None) \
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
                  .filter(Link.spec_num == row.spec_num)
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
                matches = filter_records(records, row.spec_num, dept=doc.doc_topic.rstrip('?'))
                match_quality = 'MATCHED_PAPER_TOPIC'
            if doc.jour_topic and not matches:
                matches = filter_records(records, row.spec_num, dept=doc.jour_topic.rstrip('?'))
                match_quality = 'MATCHED_JOURNAL_TOPIC'
        # Record matches if any of the quality checks yield a hit
        if matches:
            ezids = ' | '.join(matches)
            # Get department of matches
            records = [rec for rec in records if rec['occurrenceID'] in ezids]
            depts = list(set([rec['collectionCode'] for rec in records]))
            print row.spec_num, '=>', match_quality
            db.update(Link,
                      id=row.id,
                      ezid=ezids,
                      match_quality=match_quality,
                      department=depts[0])
    db.commit().close()

"""Migrates data from the old to the current specimen database

Copy to the main directory to run
"""

import re

from sqlalchemy import distinct
from sqlalchemy.exc import IntegrityError

from specimens import Parser
from database.database import (Document as DstDocument,
                               Journal as DstJournal,
                               Snippet as DstSnippet,
                               Specimen as DstSpecimen)
from database.queries import Query as DstQuery
from database.v20180512.database.database import (Document as SrcDocument,
                                                  Journal as SrcJournal,
                                                  Snippet as SrcSnippet)
from database.v20180512.database.queries import Query as SrcQuery


def get_target(snippet):
    snippet = snippet.replace('**', '')
    i = len(snippet) / 2 - 5
    j = len(snippet)
    pre = snippet[:i]
    target = snippet[i:].replace('**', '')
    # Check for occurrences of museum code later in the string
    parts = re.split(parser.regex['code'], target)
    target = ''.join(parts[:3])
    post = ''.join(parts[3:]) if len(parts) > 1 else ''
    return pre, target, post



db_from = SrcQuery()
db_to = DstQuery()
db_to.max_length = 5000

sess_from = db_from.new()
sess_to = db_to.new()


documents = sess_from.query(SrcDocument).all()
for doc in documents:
    sess_to.upsert(DstDocument,
                   id=doc.id,
                   topic=doc.topic if doc.topic else None,
                   num_specimens=doc.num_specimens)
sess_to.commit()

parser = Parser()
offset = 0
limit = 10000
hints = {}
while True:
    snippets = sess_from.query(SrcSnippet) \
                        .offset(offset) \
                        .limit(limit) \
                        .all()
    if not snippets:
        break
    print 'Checking results {:,} through {:,}...'.format(offset, offset + limit)
    for rec in snippets:
        # Check the hints dict to see if snippet has already been processed
        clean = rec.snippet.replace('**', '')
        if clean in hints:
            continue
        else:
            hints[clean] = 1
        pre, target, post = get_target(rec.snippet)
        matches = parser.findall(target)
        if matches:
            rows = sess_to.query(DstDocument.topic.label('doc_topic'),
                                 DstJournal.topic.label('jour_topic')) \
                          .join(DstJournal, DstJournal.title == DstDocument.journal) \
                          .join(DstSnippet, DstSnippet.doc_id == DstDocument.id) \
                          .filter(DstSnippet.doc_id == rec.doc_id)
            expand_short_ranges = True
            topics = rows.first()
            if topics:
                expand_short_ranges = 'ms' not in [s.strip('?') for s in topics if s]
            parsed = parser.parse(matches[0], expand_short_ranges=expand_short_ranges)
        # Add snippets that don't include a catalog number
        if not matches or not parsed:
            sess_to.add(DstSnippet(doc_id=rec.doc_id, snippet=rec.snippet))
            continue
        # Created snippets and specimen records for parsed catalog numbers
        text = pre + target.replace(matches[0], '**' + matches[0] + '**', 1) + post
        # Check if this snippet already exists
        exists = sess_to.query(DstSnippet) \
                        .filter_by(doc_id=rec.doc_id, snippet=text) \
                        .first()
        if not exists:
            # Create the snippet record
            snippet = DstSnippet(doc_id=rec.doc_id, snippet=text)
            sess_to.add(snippet)
            sess_to.flush()
            # Create the specimen record
            '''
            if not parsed or len(parsed[0]) <= 8:
                try:
                    print 'DOC ID:  ', rec.doc_id
                    print 'TOPICS:  ', topics
                    print 'SNIPPET: ', target.encode('cp1252')
                    print 'PARSED:  ', [p.encode('cp1252') for p in parsed]
                    print '-' * 60
                except UnicodeEncodeError:
                    pass
            '''
            for spec_num in parsed:
                spec = DstSpecimen(snippet_id=snippet.id,
                                   verbatim=matches[0],
                                   spec_num=spec_num)
                sess_to.add(spec)
    offset += limit
print len(hints)
sess_to.commit().close()

# Routine tasks
# 1. Re-parse existing snippets
# 2. Recalculate specimen counts per paper

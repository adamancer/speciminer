"""Extends and summarizes data mined from GeoDeepDive"""

from __future__ import division

import logging
import logging.config

import os

import requests_cache
import yaml
from sqlalchemy import and_, or_

from miners.link import get_specimens, filter_records, get_keywords
from database.database import Document, Journal, Link, Snippet, Specimen, Taxon
from database.queries import Query


requests_cache.install_cache(os.path.join('output', 'extend'))


def get_taxa_on_pages(pages):
    taxa = []
    for row in db.query(Taxon).filter(Taxon.source_id.in_(set(pages))):
        taxa.append(row.taxon)
    return taxa



def match_spec_num(spec_num, doc_id, dept=None):
    """Matches specimen to catalog record based on snippets where it occurs"""
    print 'Matching {}...'.format(spec_num)
    match_quality = None
    records = get_specimens(spec_num)
    if dept is not None:
        records = [r for r in records if r['collectionCode'] == dept]
    # Get all snippets from the current paper that mention this specimen
    query = db.query(Link.spec_num,
                     Link.corrected,
                     Snippet.snippet,
                     Snippet.page_id,
                     Taxon.taxon) \
              .join(Specimen, Specimen.spec_num == Link.spec_num) \
              .join(Snippet, and_(Snippet.doc_id == Link.doc_id,
                                  Specimen.snippet_id == Snippet.id)) \
              .join(Taxon, Taxon.source_id == Snippet.page_id) \
              .filter(and_(Link.spec_num == spec_num,
                           Link.doc_id == doc_id))
    snippets = []
    taxa = []
    for row in query.all():
        snippets.append(row.snippet.strip('. '))
        taxa.append(row.taxon)
    # Pull keywords from the snippets to use for matching
    keywords = get_keywords(' '.join(snippets).lower())
    matches = filter_records(records, spec_num, keywords=keywords)
    match_quality = 'Matched snippet'
    if not matches:
        spec_num = spec_num.rsplit('-', 1)[0]
        matches = filter_records(records, spec_num, keywords=keywords)
    # Check results against paper
    if not matches:
        # Check results against pages on which this specimen appears
        if taxa:
            keywords = get_keywords(' '.join(list(set(taxa))))
            matches = filter_records(records, spec_num, keywords=keywords)
            match_quality = 'Matched same page'
        # Confirm that the referenced document actually exists
        if not matches:
            query = db.query(Document.title,
                             Document.topic.label('doc_topic'),
                             Journal.topic.label('jour_topic')) \
                      .join(Journal, Document.journal == Journal.title) \
                      .filter(Document.id == doc_id)
            doc = query.first()
            # Check for BHL items with no journal title
            if doc is None:
                query = db.query(Document.title,
                                 Document.topic.label('doc_topic')) \
                          .filter(Document.id == doc_id)
                doc = query.first()
            if doc is None:
                return [], 'No document (id={})'.format(doc_id), snippets, records
        # Check results against title of paper
        if not matches and doc.title:
            keywords = get_keywords(doc.title.lower())
            matches = filter_records(records, spec_num, keywords=keywords)
            match_quality = 'Matched source title'
        # Check results against topic of paper, then journal
        #if not matches and doc.doc_topic:
        #    matches = filter_records(records, spec_num, keywords=keywords, dept=doc.doc_topic.rstrip('?'))
        #    match_quality = 'Matched source topic'
        if not matches and hasattr(doc, 'jour_topic') and doc.jour_topic:
            matches = filter_records(records, spec_num, keywords=keywords, dept=doc.jour_topic.rstrip('?'))
            match_quality = 'Matched series topic'
    ezids = []
    mqs = []
    for match, score in matches:
        ezids.append(match['occurrenceID'])
        mqs.append(score.summary(match_quality))
    #if len(set(mqs)) > 1:
    #    raise ValueError('%s', sorted(list(set(mqs))))
    # Check results against department
    if dept and matches:
        mqs[0] += ' (forced department={}).'.format(dept)
    return ezids, mqs[0] if matches else 'No match', snippets, records


def link_citation(row, dept=None):
    """Links a given citation to a catalog record"""
    spec_num = row.corrected if row.corrected else row.spec_num
    while ' 0' in spec_num:
        spec_num = spec_num.replace(' 0', ' ')
    if spec_num.startswith('USNH'):
        dept = 'Botany'
    matches, match_quality, snippets, records = match_spec_num(spec_num,
                                                               row.doc_id,
                                                               dept=dept)
    if not matches and '-' in spec_num:
        matches, match_quality, snippets, records = match_spec_num(spec_num.split('-')[0],
                                                                   row.doc_id,
                                                                   dept=dept)
    print '  {}'.format(match_quality)
    # Record matches if any of the quality checks yield a hit
    num_specimens = 0
    if matches:
        ezids = ' | '.join(matches)
        records = [rec for rec in records if rec['occurrenceID'] in ezids]
        depts = list(set([rec['collectionCode'] for rec in records]))
        db.update(Link,
                  id=row.id,
                  ezid=ezids,
                  match_quality=match_quality,
                  department=depts[0],
                  num_snippets=len(snippets))
        row.ezids = ezids
        row.match_quality = match_quality
        row.department = depts[0]
        num_specimens = 1
    else:
        db.update(Link,
                  id=row.id,
                  ezid=None,
                  match_quality='No match',
                  department=None,
                  num_snippets=len(snippets))
        row.match_quality = 'No match'
    # Update journals
    if db.length > db.max_length:
        docs = {}
        for doc in db.query(Link.doc_id).filter(Link.ezid != None).all():
            docs.setdefault(doc.doc_id, []).append(1)
        for doc_id, rows in docs.iteritems():
            db.update(Document, id=doc_id, num_specimens=len(rows))
        db.commit()
    # Update count for this article
    if num_specimens:
        db.update(Document, id=row.doc_id, num_specimens=num_specimens)
    return row


def clear_old_links(db):
    """Clears existing links"""
    # FIXME: Make this one query, you idiot
    for row in db.query(Link) \
                 .filter(and_(Link.match_quality != 'Matched manually',
                              Link.match_quality != None)).all():
        db.update(Link,
                  id=row.id,
                  ezid=None,
                  match_quality=None,
                  department=None,
                  num_snippets=None)
    db.commit()


def clear_failed_links(db):
    """Clears failed links"""
    query = db.query(Link).filter(Link.match_quality == 'NO_MATCH')
    for row in query.all():
        db.update(Link,
                  id=row.id,
                  ezid=None,
                  match_quality=None,
                  num_snippets=None)
    db.commit()


def assign_deparment(db):
    """Assigns the department based on other citations in a given paper"""
    # Map high-quality citations for each document
    docs = {}
    statuses = ('Matched same page', 'Matched snippet', 'Matched title')
    for row in db.query(Link).all():
        if row.match_quality.startswith(statuses):
            docs.setdefault(row.doc_id, []).append(row.department)
        else:
            docs.setdefault(row.doc_id, []).append(None)

    for doc_id, depts in docs.iteritems():
        dept = guess_department(depts)
        if dept is not None:
            db.update(Link, primary_key='doc_id', doc_id=doc_id, department=dept)
    db.commit()


def assign_department_to_doc(rows, depts):
    """Assigns department based on other citations in a single paper"""
    dept = guess_department(depts)
    if dept:
        for row in rows:
            if row.department != dept:
                link_citation(row, dept=dept)



def guess_department(depts):
    """Guesses the department based on other citations in the current paper"""
    counts = {}
    for dept in set(depts):
        counts[dept] = depts.count(dept)
    for dept, count in counts.iteritems():
        if ((count / len(depts) > 0.7 and count > 20)
            or (len(counts) == 1 and count >= 5)):
            return dept


def match_citations(db):
    db.max_length = 100
    doc_id = None
    rerun = []
    depts = []
    rows = db.query(Link) \
              .filter(or_(Link.match_quality == None), #, Link.match_quality == 'No match'),
                      or_(Link.spec_num.like('USNH%'),
                          Link.spec_num.like('USNM%'),
                          Link.spec_num.like('NMNH%'))) \
              .order_by(Link.doc_id) \
              .all()
    for i, row in enumerate(rows):
        print ('Matching row... ({:,}/{:,})'.format(i, len(rows)))
        # Check if this is a new document. If so, check if the previous
        # document can be assigned to one department and re-identify rows that
        # missed completely or that matched a different department. For
        # example, if a a document cites 100 specimens from mammals but has
        # 20 more unmatched specimens, it's a reasonable guess that those
        # specimens also come from mammals.
        if row.doc_id != doc_id:
            assign_department_to_doc(rerun, depts)
            # Reset doc info for new document
            doc_id = row.doc_id
            depts = []
            rerun = []
        # Process row
        row = link_citation(row)
        if row.department:
            depts.append(row.department)
        rerun.append(row)
    else:
        assign_department_to_doc(rerun, depts)
    db.commit()


def count_citations(db, reset=False):
    """Counts citations for each paper"""
    if reset:
        for row in db.query(Document).filter(Document.num_specimens > 0).all():
            db.update(Document, id=row.id, num_specimens=0)
    docs = {}
    for row in db.query(Link.doc_id).filter(Link.ezid != None).all():
        docs.setdefault(row.doc_id, []).append(1)
    for doc_id, rows in docs.iteritems():
        db.update(Document, id=doc_id, num_specimens=len(rows))
    db.commit()


def guess_department(depts):
    """Guesses the department based on other citations in the current paper"""
    counts = {}
    for dept in set(depts):
        if dept and not dept.endswith('*'):
            counts[dept] = depts.count(dept)
    # Skip if everything already assigned to the same department
    if len(counts) == 1 and counts.values()[0] == len(depts):
        return
    for dept, count in counts.iteritems():
        if ((count / len(depts) > 0.7 and count > 20)
            or (len(counts) == 1 and count >= 5)):
            return dept


def assign_department_from_related(db):
    """Assigns the department based info from a given paper"""
    print 'Assigning departments...'
    # Map high-quality citations for each document
    statuses = (
        'Matched snippet',
        'Matched same page',
        'Matched document title'
    )
    docs = {}
    i = 0
    for row in db.query(Link).all():
        if row.match_quality and row.match_quality.startswith(statuses):
            docs.setdefault(row.doc_id, []).append(row.department)
        else:
            docs.setdefault(row.doc_id, []).append(None)
        i += 1
        if not i % 25000:
            print ' {:,} rows examined'.format(i)
    for doc_id, depts in docs.iteritems():
        print 'Checking for a consistent department in {}...'.format(doc_id)
        dept = guess_department(depts)
        if dept is not None:
            print ' Assigned samples in {} to {}'.format(doc_id, dept)
            # Assigning a department will OVERWRITE matches for samples
            # from other departments for this document. Clear those
            # matches now.
            where = [
                Link.__table__.c.doc_id == doc_id,
                Link.__table__.c.department != dept
            ]
            db.update(Link, where=where, department=None, ezid=None, match_quality=None)
            # Now update all unmatched rows with the starred department name
            where = [Link.__table__.c.doc_id == doc_id,
                     or_(Link.__table__.c.match_quality == None,
                         Link.__table__.c.match_quality == 'No match')]
            db.update(Link,
                      where=where,
                      department=dept.rstrip('*') + '*',
                      notes='Assigned dept based on related specimens')
    db.commit()
    # NOTE: The fallback below is now handled by assign_department_from_doc
    # Failing the above, assign a department based on the document or journal
    # topic
    #print 'Assigning department from journal...'
    #topicker = Topicker()
    #for row in db.query(Document).filter(Document.topic != None).all():
    #    dept = topicker.depts.get(row.topic, row.topic).rstrip('*') + '*'
    #    print  'Assigned samples in {} to {}...'.format(doc_id, dept)
    #    db.update(Link,
    #              where={'doc_id': row.id, 'department': None},
    #              department=dept)
    #for row in db.query(Journal).filter(Journal.topic.like('%*')).all():
    #    topic = topicker[Journal.topic] + '*'
    #    db.update(Link, where={doc_id: doc_id, topic: None}, topic=topic)
    db.commit()


def assign_department_from_doc(db):
    print 'Assigning deparment based on source...'
    topicker = Topicker()
    # Update departments in links based on documents
    rows = db.query(Document.id, Document.topic) \
             .filter(Document.topic != None).all()
    for i, row in enumerate(rows):
        topic = row.topic.rstrip('*')
        dept = topicker.depts.get(topic, topic) + '*'
        db.update(Link,
                  where={'doc_id': row.id, 'department': None},
                  department=dept,
                  notes='Assigned dept based on document')
        i += 1
        if i and not i % 1000:
            print '  {:,}/{:,} documents examined'.format(i, len(rows))
    else:
        print '  {:,}/{:,} documents examined!'.format(i, len(rows))
    # Update departments in links based on journals
    rows = db.query(Document.id, Journal.title, Journal.topic) \
             .join(Journal, Document.title == Journal.title) \
             .filter(Journal.topic != None) \
             .all()
    for i, row in enumerate(rows):
        topic = row.topic.rstrip('*')
        dept = topicker.depts.get(topic, topic) + '*'
        db.update(Link,
                  where={'doc_id': row.id, 'department': None},
                  department=dept,
                  notes='Assigned dept based on journal')
        i += 1
        if i and not i % 1000:
            print '  {:,}/{:,} journals examined!'.format(i, len(rows))
    else:
        print '  {:,}/{:,} journals examined!'.format(i, len(rows))
    # Clear departments for specimens from other institutions
    where = [not_(or_(Link.__table__.c.spec_num.like('NMNH%'),
                      Link.__table__.c.spec_num.like('USNM%')))]
    db.update(Link, where=where, department=None, notes=None)
    db.commit()


def assign_journal_topic_from_doc(db):
    pass


def assign_doc_topic_from_journal(db):
    pass


def assign_doc_topic_from_links(db):
    # Back-populate topics from links
    topicker = Topicker()
    dept_to_code = {v: k for k, v in topicker.depts.iteritems()}
    doc_ids = {}
    for row in db.query(Link.doc_id, Link.department):
        doc_ids.setdefault(row.doc_id, []).append(row.department)
    for doc_id, depts in doc_ids.iteritems():
        populated = [dept.rstrip('*') for dept in depts if dept]
        if (len(set(populated)) == 1
            and (len(populated) > 5
                 or len(populated) > 1 and len(populated) == len(depts))):
            dept = populated[0]
            code = dept_to_code.get(dept, dept).rstrip('*')
            logging.debug('Setting topic on {}={}'.format(doc_id, code))
            db.update(Document, id=doc_id, topic=code + '*')
    db.commit()


def assign_doc_topic_from_title(db):
    """Assigns topic to documents and journals based on keywords in title"""
    max_length = db.max_length
    db.max_length = 1000
    topicker = Topicker()
    mask = u'{:,}/{:,} {} examined'
    # Examine documents
    rows = db.query(Document) \
             .filter(and_(Document.num_specimens != None,
                          Document.topic == None)) \
             .order_by(Document.title) \
             .all()
    for i, row in enumerate(rows):
        #mask = 'Analyzing "{}" ({:,}/{:,})...'
        #print mask.format(unidecode(row.title), i + 1, len(rows))W
        dept = topicker.get_department(row.title)
        if dept:
            db.update(Document, id=row.id, topic=dept + '*')
            #print '    + Assigned to {}'.format(dept)
        else:
            msg = u'No match: "{}"'.format(unidecode(row.title))
            print msg
            logging.debug(msg)
        i += 1
        if not i % 100:
            print mask.format(i, len(rows), 'documents')
    print mask.format(i, len(rows), 'documents')


def assign_journal_topic_from_title(db):
    max_length = db.max_length
    db.max_length = 1000
    topicker = Topicker()
    mask = u'{:,}/{:,} {} examined'
    # Examine journals
    rows = db.query(Journal) \
             .filter(Journal.topic == None) \
             .order_by(Journal.title) \
             .all()
    for i, row in enumerate(rows):
        #mask = 'Analyzing "{}" ({:,}/{:,})...'
        #print mask.format(unidecode(row.title), i + 1, len(rows))
        dept = topicker.get_department(row.title)
        if dept:
            db.update(Journal, title=row.title, topic=dept + '*')
            #print '    + Assigned to {}'.format(dept)
        else:
            msg = 'No match: "{}"'.format(unidecode(row.title))
            print msg
            logging.debug(msg)
        i += 1
        if not i % 100:
            print mask.format(i, len(rows), 'journal')
    print mask.format(i, len(rows), 'documents')
    db.commit()
    db.max_length = max_length




if __name__ == '__main__':
    # Configure logging and cache
    logging.config.dictConfig(yaml.load(open('logging.yml', 'rb')))
    # Set up database query engine
    db = Query(max_length=10000)
    # Clear links if desired
    if False:
        clear_old_links(db)
    # Assign department based on other specimens in the same paper
    if False:
        assign_department(db)
    # Match specimens to catalog records
    match_citations(db)
    count_citations(db, True)
    db.commit().close()

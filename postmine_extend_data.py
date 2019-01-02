"""Extends and summarizes data mined from GeoDeepDive"""

from __future__ import division

import logging
import logging.config

import os
import re

import requests_cache
import yaml
from sqlalchemy import and_, or_, not_
from sqlalchemy.exc import IntegrityError
from unidecode import unidecode

from miners.link import get_specimens, filter_records, get_keywords
from miners.topic import Topicker
from database.database import Document, Journal, Link, Snippet, Specimen, Taxon
from database.queries import Query


requests_cache.install_cache(os.path.join('..', '..', '..', '..', '..', '..', 'cache', 'topics'))


def get_taxa_on_pages(pages):
    """Get taxa on page in BHL"""
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
        records = [r for r in records if r['collectionCode'] == dept.strip('*')]
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
            match_quality = 'Matched document title'
        # Check results if department forced
        if not matches and dept and dept.endswith('*'):
            matches = filter_records(records, spec_num, dept=dept)
            match_quality = 'Matched related specimens'
        # Check results against topic of paper, then journal
        if not matches and doc.doc_topic:
            matches = filter_records(records, spec_num, keywords=keywords, dept=doc.doc_topic.rstrip('?*'))
            match_quality = 'Matched document topic'
        if not matches and hasattr(doc, 'jour_topic') and doc.jour_topic:
            matches = filter_records(records, spec_num, keywords=keywords, dept=doc.jour_topic.rstrip('?*'))
            match_quality = 'Matched journal topic'
    ezids = []
    mqs = []
    for match, score in matches:
        ezids.append(match['occurrenceID'])
        mqs.append(score.summary(match_quality))
    #if len(set(mqs)) > 1:
    #    raise ValueError('%s', sorted(list(set(mqs))))
    # Check results against department
    if dept and matches:
        mqs[0] += ' (forced {})'.format(dept.strip('*'))
        mqs[0] = mqs[0].replace(' (matched collection)', '')
    return ezids, mqs[0] if matches else 'No match', snippets, records


def link_citation(row, dept=None):
    """Links a given citation to a catalog record"""
    spec_num = row.corrected if row.corrected else row.spec_num
    while ' 0' in spec_num:
        spec_num = spec_num.replace(' 0', ' ')
    if spec_num.startswith('USNH'):
        dept = 'Botany'
    elif row.department and row.department.endswith('*'):
        dept = row.department
    matches, match_quality, snippets, records = match_spec_num(spec_num,
                                                               row.doc_id,
                                                               dept=dept)
    if not matches and '-' in spec_num:
        matches, match_quality, snippets, records = match_spec_num(spec_num.split('-')[0],
                                                                   row.doc_id,
                                                                   dept=dept)
    if not matches and re.search('\d[A-Z]$', spec_num):
        matches, match_quality, snippets, records = match_spec_num(spec_num[:-1],
                                                                   row.doc_id,
                                                                   dept=dept)

    print '  {}'.format(match_quality)
    # Record matches if any of the quality checks yield a hit
    num_specimens = 0
    if matches:
        ezids = ' | '.join(matches)
        records = [rec for rec in records if rec['occurrenceID'] in ezids]
        dept = list(set([rec['collectionCode'] for rec in records]))[0]
        if 'forced' in match_quality:
            dept += '*'
        db.update(Link,
                  id=row.id,
                  ezid=ezids,
                  match_quality=match_quality,
                  department=dept,
                  num_snippets=None)
        row.ezids = ezids
        row.match_quality = match_quality
        row.department = dept
        num_specimens = 1
    else:
        db.update(Link,
                  id=row.id,
                  ezid=None,
                  match_quality='No match',
                  department=None,
                  num_snippets=None)
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


def clear_bad_links(db):
    """Clears unmatched specimens"""
    where = {'match_quality': 'No match'}
    db.update(Link, where=where, department=None, ezid=None, match_quality=None)
    db.commit()


def clear_existing_links(db):
    """Clears existing links with NMNH specimens"""
    print 'Clearing existing links...'
    where = [
        Links.__table__.c.match_quality != 'Matched manually',
        Links.__table__.c.match_quality != None
    ]
    db.update(Link,
              where=where,
              ezid=None,
              department=None,
              match_quality=None,
              num_snippets=None,
              notes=None)
    db.commit()


def clear_failed_links(db):
    """Clears failed links"""
    db.update(Link,
              where={'match_quality': 'No match'},
              ezid=None,
              department=None,
              match_quality=None,
              num_snippets=None,
              notes=None)
    db.commit()


def clear_multiple_links(db):
    """Clears links that point to multiple objects"""
    where = [Link.__table__.c.ezid.like('%|%')]
    db.update(Link,
              where=where,
              ezid=None,
              department=None,
              match_quality=None,
              num_snippets=None,
              notes=None)
    db.commit()


def clear_starred_links(db):
    """Clears links made by forcing the department"""
    where = [Link.__table__.c.department.like('%*')]
    db.update(Link,
              where=where,
              ezid=None,
              department=None,
              match_quality=None,
              num_snippets=None,
              notes=None)
    db.commit()


def clear_dept_from_unmatched(db):
    """Clear extrapolated department names from links"""
    where = [and_(Link.__table__.c.department.like('%*'),
                  or_(Link.__table__.c.match_quality == None,
                      Link.__table__.c.match_quality == 'No match'))]
    db.update(Link, where=where, department=None)
    db.commit()


def flag_suspect_suffixes(db):
    """Identify and flag suspect suffixes"""
    db.update(Link, where={'corrected': '[BAD_SUFFIX]'}, corrected=None)
    pattern = re.compile(r'-([A-Z]{3,}|\])$')
    for row in db.query(Link.id, Link.spec_num).filter(Link.corrected == None):
        if pattern.search(row.spec_num):
            print 'Fixing {}...'.format(row.spec_num)
            spec_num = row.spec_num.rsplit('-', 1)[0]
            try:
                db.update(Link, id=row.id, spec_num=spec_num)
                db.commit()
            except IntegrityError:
                db.rollback()
                db.update(Link, id=row.id, corrected='[BAD_SUFFIX]')
                db.commit()
    # Perform the same operation in specimens. This is much simpler because
    # there is no onerous uniqueness constraint.
    for row in db.query(Specimen.id, Specimen.spec_num).all():
        if pattern.search(row.spec_num):
            print 'Fixing {}...'.format(row.spec_num)
            spec_num = row.spec_num.rsplit('-', 1)[0]
            db.update(Specimen, id=row.id, spec_num=spec_num)
    db.commit()


def flag_unlikely_suffixes(db):
    rows = db.query(Link) \
             .filter(Link.spec_num.like('%-%'),
                     Link.corrected == None)
    for i, row in enumerate(rows):
        print 'Fixing {}...'.format(row.spec_num)
        if row.department and row.department.startswith('Min'):
            continue
        spec_num = row.spec_num.rsplit('-', 1)[0]
        try:
            db.update(Link, id=row.id, spec_num=spec_num)
            db.commit()
        except IntegrityError:
            db.rollback()
            db.update(Link, id=row.id, corrected='[UNLIKELY_SUFFIX]')
            db.commit()
        if i and not i % 10000:
            print ' {:,} rows examined'
    db.commit()


def flag_truncations(db):
    """Identify and flag apparent truncations"""
    db.update(Link, where={'corrected': '[TRUNCATED]'}, corrected=None)
    query = db.query(Link.doc_id).filter(Link.corrected == None)
    for i, row in enumerate(query.distinct()):
        doc_id = row.doc_id
        rows = db.query(Link.id, Link.spec_num).filter_by(doc_id=doc_id)
        spec_nums = [row.spec_num for row in rows.distinct()]
        for row in rows:
            startswith = [s for s in spec_nums
                          if (s and
                              s != row.spec_num
                              and s.startswith(row.spec_num))]
            if startswith:
                print row.spec_num, '=>', startswith
                db.update(Link, id=row.id, corrected='[TRUNCATION]')
                #db.delete(Links, spec_num=row.spec_num, doc_id=doc_id)
        if i and not i % 10000:
            print ' {:,} rows examined'
    db.commit()


def match_citations(db):
    db.max_length = 100
    doc_id = None
    rerun = []
    depts = []
    rows = db.query(Link) \
              .filter(or_(Link.match_quality == None,
                          Link.match_quality == 'No match'),
                      or_(Link.spec_num.like('USNH%'),
                          Link.spec_num.like('USNM%'),
                          Link.spec_num.like('NMNH%'))) \
              .order_by(Link.doc_id) \
              .all()
    for i, row in enumerate(rows):
        print ('Matching row... ({:,}/{:,})'.format(i + 1, len(rows)))
        # NOTE: Moved this functionality to postflight
        # Check if this is a new document. If so, check if the previous
        # document can be assigned to one department and re-identify rows that
        # missed completely or that matched a different department. For
        # example, if a a document cites 100 specimens from mammals but has
        # 20 more unmatched specimens, it's a reasonable guess that those
        # specimens also come from mammals.
        #if row.doc_id != doc_id:
        #    assign_department_to_doc(rerun, depts)
        #    # Reset doc info for new document
        #    doc_id = row.doc_id
        #    depts = []
        #    rerun = []
        # Process row
        row = link_citation(row)
        #if row.department:
        #    depts.append(row.department)
        #rerun.append(row)
    #else:
    #    assign_department_to_doc(rerun, depts)
    db.commit()



def count_citations(db, reset=False):
    """Counts citations for each paper"""
    print 'Counting citations for each paper...'
    if reset:
        for row in db.query(Document).filter(Document.num_specimens > 0).all():
            db.update(Document, id=row.id, num_specimens=0)
    docs = {}
    for row in db.query(Link.doc_id).filter(Link.ezid != None).all():
        docs.setdefault(row.doc_id, []).append(1)
    for doc_id, rows in docs.iteritems():
        print ' {} => {:,}'.format(doc_id, len(rows))
        db.update(Document, id=doc_id, num_specimens=len(rows))
    db.commit()


def count_snippets(db):
    """Counts the number of snippets in which each specimen is found"""
    print 'Counting snippets for each specimen number...'
    snippets = {}
    for row in db.query(Specimen.snippet_id,
                        Specimen.spec_num,
                        Snippet.doc_id) \
                 .join(Snippet, Snippet.id == Specimen.snippet_id) \
                 .all():
        snippets.setdefault(row.doc_id, {}) \
                .setdefault(row.spec_num, []) \
                .append(row.snippet_id)
    for row in db.query(Link.id, Link.doc_id, Link.spec_num).all():
        num_snippets = len(set(snippets.get(row.doc_id, {}) \
                                       .get(row.spec_num, [])))
        db.update(Link, id=row.id, num_snippets=num_snippets)
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
    # Preflight
    #clear_existing_links(db)
    #clear_bad_links(db)
    clear_failed_links(db)
    #clear_multiple_links(db)
    clear_starred_links(db)
    clear_dept_from_unmatched(db)
    #flag_suspect_suffixes(db)
    #flag_unlikely_suffixes(db)
    #flag_truncations(db)
    # Assign topics
    if True:
        db.update(Document, where=[Document.__table__.c.topic.like('%*')], topic=None)
        db.update(Journal, where=[Journal.__table__.c.topic.like('%*')], topic=None)
        #assign_doc_topic_from_links(db)
        assign_doc_topic_from_title(db)
        assign_journal_topic_from_title(db)
        assign_department_from_related(db)
        assign_department_from_doc(db)
        #raw_input('paused')
    #count_citations(db, True)
    #count_snippets(db)
    # Main event`
    match_citations(db)
    #assign_doc_topic_from_links(db)
    #assign_department_from_related(db)
    #assign_department_from_doc(db)
    #clear_failed_links(db)
    #match_citations(db)
    # Postflight
    count_citations(db, True)
    count_snippets(db)
    db.commit().close()

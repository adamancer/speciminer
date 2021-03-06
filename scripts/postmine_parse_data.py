"""Re-parses snippets collected from the GeoDeepDive corpus"""

from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import os
import re
import sys

import requests
import requests_cache
import yaml
from unidecode import unidecode
from sqlalchemy import distinct, func
from sqlalchemy.exc import IntegrityError

sys.path.insert(0, '..')
from config.constants import CACHE_DIR
from miners.parser import Parser
from miners.topic import Topicker
from database.database import Document, Journal, Link, Snippet, Specimen
from database.queries import Query




logger = logging.getLogger('speciminer')
logger.info('Running postmine_parse_data.py')




def get_target(snippet):
    target = snippet.replace('**', '')[3:-3].rstrip()
    post = []
    while target and target[-1].isdigit():
        post.append(target[-1])
        target = target[:-1].rstrip()
    post = ''.join(post + ['...'])
    return '...', target, post


def clear_duplicate_snippets(db):
    print('Clearing duplicate snippets...')
    snippets = {}
    for row in db.query(Snippet).all():
        pre, target, post = get_target(row.snippet)
        text = pre + target + post
        page_id = row.page_id if row.page_id else ''
        key = '|'.join([row.doc_id, page_id, text])
        snippets.setdefault(key, []).append(row)
    for snippet, rows in snippets.items():
        if len(rows) > 1:
            for row in rows[1:]:
                print('Deleted {}'.format(row.id))
                db.delete(Snippet, id=row.id)
    db.commit()


def parse_snippets(db):
    print('Parsing snippets...')
    parser = Parser()
    for row in db.query(Snippet).all():
        # Update snippet
        pre, target, post = get_target(row.snippet)
        text = pre + target + post
        db.update(Snippet, id=row.id, snippet=text)
        # Add specimens
        matches = parser.findall(row.manual if row.manual else target)
        for match in matches:
            expand_short_ranges = 'ms' not in db.get_topics(row.id)
            parsed = parser.parse(match,
                                  expand_short_ranges=expand_short_ranges)
            if parsed:
                target = target.replace(match, '**' + match + '**')
                # Create new specimen records
                for spec_num in parsed:
                    db.add(Specimen,
                           snippet_id=row.id,
                           verbatim=match,
                           spec_num=spec_num)
    db.commit()


def link_specimens(db):
    print('Creating table of linkable specimens...')
    hints = {}
    query = db.query(Link.spec_num, Link.doc_id)
    for row in query.all():
        hints.setdefault(row.doc_id, {}).setdefault(row.spec_num, 1)

    query = db.query(Specimen.spec_num, Document.id) \
              .join(Snippet, Specimen.snippet_id == Snippet.id) \
              .join(Document, Snippet.doc_id == Document.id)
    for row in query.all():
        try:
            hints[row.id][row.spec_num]
        except KeyError:
            hints.setdefault(row.id, {}).setdefault(row.spec_num, 1)
            if len(row.spec_num) > 6:
                db.add(Link, spec_num=row.spec_num, doc_id=row.id)
    db.commit()




if __name__ == '__main__':    
    # Run script
    db = Query(max_length=5000, bulk=True)
    db.new()
    print('Parsing catalog numbers from snippets...')
    # Delete previously parsed catalog numbers
    print('Deleting existing catalog numbers...')
    db.query(Link).delete()
    db.query(Specimen).delete()
    db.commit()
    # Parse snippets
    clear_duplicate_snippets(db)
    parse_snippets(db)
    #link_specimens(db)
    db.commit().close()
    print('Done!')

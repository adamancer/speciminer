"""Re-parses snippets collected from the GeoDeepDive corpus"""

import re

from sqlalchemy import distinct, func
from sqlalchemy.exc import IntegrityError

from specimens import Parser
from database.database import Document, Journal, Link, Snippet, Specimen
from database.queries import Query


def get_target(snippet):
    target = snippet.replace('**', '')[3:-3].rstrip()
    post = []
    while target[-1].isdigit():
        post.append(target[-1])
        target = target[:-1].rstrip()
    post = ''.join(post + ['...'])
    return '...', target, post



db = Query()
db.new()

print 'Parsing catalog numbers from snippets...'
parser = Parser()
db.query(Specimen).delete()
db.commit()
for row in db.query(Snippet).all():
    pre, target, post = get_target(row.snippet)
    matches = parser.findall(row.manual if row.manual else target)
    for match in matches:
        expand_short_ranges = 'ms' not in db.get_topics(row.id)
        parsed = parser.parse(match, expand_short_ranges=expand_short_ranges)
        if parsed:
            target = target.replace(match, '**' + match + '**')
            # Create new specimen records
            for spec_num in parsed:
                spec = Specimen(snippet_id=row.id,
                                verbatim=match,
                                spec_num=spec_num)
                db.add(spec)
    # Update snippet
    text = pre + target + post
    db.update(Snippet, id=row.id, snippet=text)
db.commit()

print 'Creating table of linkable specimens...'
hints = {}
query = db.query(Specimen.spec_num, Document.id) \
          .join(Snippet, Specimen.snippet_id == Snippet.id) \
          .join(Document, Snippet.doc_id == Document.id)
for row in query.all():
    try:
        hints[row.id][row.spec_num]
    except KeyError:
        hints.setdefault(row.id, {}).setdefault(row.spec_num, 1)
        if len(row.spec_num) > 6:
            link = Link(spec_num=row.spec_num, doc_id=row.id)
            db.add(link)

db.commit().close()
print 'Done!'

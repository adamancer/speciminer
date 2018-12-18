import logging
logger = logging.getLogger(__name__)

import sys
sys.path.append('..')
sys.path.append('../..')

import json
import re

from flask import Flask, make_response, render_template, request
from sqlalchemy import and_

from database.database import Document, Journal, Link, Part, Specimen, Snippet
from database.queries import Query
from miners.bhl.bhl import route_request
from miners.link import get_specimens
from miners.parser import Parser


PARSER = Parser()

app = Flask(__name__)


@app.route('/finder', methods=['GET', 'POST'])
def finder():
    text = ''
    fmt = 'html'
    specimens = {}
    docinfo = None
    if request.method == 'POST':
        text = request.values.get('text').strip()
        fmt = request.values.get('format', fmt)
        if text.startswith('http') and len(text) < 128:
            item = route_request(text)
            specimens = item.specimens
            docinfo = item.docinfo
        else:
            specimens = {}
            snippets = PARSER.snippets(text, num_chars=100)
            for match, snips in snippets.iteritems():
                for spec_num in PARSER.parse(match):
                    specimens.setdefault(spec_num, []).extend(snips)
            docinfo = None
    if fmt == 'json':
        return json.dumps(specimens, indent=4, sort_keys=True)
    return render_template('finder.htm', text=text, specimens=specimens, docinfo=docinfo)


@app.route('/documents')
def documents():
    db = Query()
    query = db.query(Document) \
              .filter(and_(Document.num_specimens != None,
                           Document.num_specimens > 0)) \
              .order_by(Document.title)
    #query = db.query(Document) \
    #          .order_by(Document.title)
    documents = query.all()
    return render_template('documents.htm', documents=documents)


@app.route('/documents/<doc_id>')
def document(doc_id):
    db = Query()
    # Get document info
    query = db.query(Document.id,
                     Document.title,
                     Document.journal,
                     Document.doi,
                     Document.year,
                     Document.topic.label('doc_topic'),
                     Journal.topic.label('jour_topic')) \
              .distinct() \
              .filter(Document.id == doc_id)
    logging.debug(query)
    doc = query.first()
    # Get specimen info
    query = db.query(Snippet.snippet,
                     Link.id,
                     Link.spec_num,
                     Link.ezid,
                     Link.department,
                     Link.match_quality) \
              .join(Specimen, Specimen.snippet_id == Snippet.id) \
              .join(Link, Link.spec_num == Specimen.spec_num) \
              .filter(Snippet.doc_id == doc_id)
    logging.debug(query)
    rows = query.all()
    spec_nums = {}
    for row in rows:
        logging.debug(row.spec_num)
        ezids = [s.strip() for s in row.ezid.split('|')] if row.ezid else []
        try:
            spec_nums[row.spec_num]['ezids'].extend(ezids)
            spec_nums[row.spec_num]['ezids'] = sorted(list(set(spec_nums[row.spec_num]['ezids'])))
        except KeyError:
            spec_nums[row.spec_num] = {
                'spec_num': row.spec_num,
                'ezids': ezids,
                'dept': row.department,
                'match_quality': row.match_quality,
                'snippets': []
                }
        else:
            if not spec_nums[row.spec_num]['dept']:
                spec_nums[row.spec_num]['dept'] = row.department
            if not spec_nums[row.spec_num]['match_quality']:
                spec_nums[row.spec_num]['match_quality'] = row.match_quality
    spec_nums = spec_nums.values()
    # Find related snippets
    query = db.query(Snippet.snippet, Specimen.spec_num) \
              .join(Specimen, Specimen.snippet_id == Snippet.id) \
              .filter(and_(Snippet.doc_id == doc_id))
    snippets = {}
    for row in query.all():
        snippets.setdefault(row.spec_num, []).append(row.snippet)
    for spec_num in spec_nums:
        spec_num['snippets'] = snippets[spec_num['spec_num']]
    spec_nums.sort(key=lambda s: s['spec_num'])
    return render_template('document.htm', doc=doc, spec_nums=spec_nums)


@app.route('/specimens/<ezid>')
def specimen(ezid):
    db = Query()
    # Get basic info about this specimen from the portal
    metadata = get_specimens(guid=ezid)
    if metadata:
        metadata = metadata[0]
    query = db.query(Link.spec_num,
                     Link.ezid,
                     Link.doc_id,
                     Snippet.snippet,
                     Document.title) \
              .join(Specimen, Specimen.spec_num == Link.spec_num) \
              .join(Snippet, and_(Snippet.doc_id == Link.doc_id,
                                  Specimen.snippet_id == Snippet.id)) \
              .join(Document, Document.id == Snippet.doc_id) \
              .filter(Link.ezid.like('%' + ezid + '%'))
    rows = query.all()
    specimen = rows[0]
    documents = {}
    for row in rows:
        doc = documents.setdefault(row.doc_id, {})
        doc['title'] = row.title
        doc.setdefault('snippets', {}) \
           .setdefault(format_snippet(row.snippet), []) \
           .append(row.spec_num)
    return render_template('specimen.htm', specimen=specimen, metadata=metadata, documents=documents)


def format_snippet(snippet):
    return snippet.replace('**', '')
    return re.sub(r'\*\*(.*?)\*\*', r'<b>\1</b>', snippet)



if __name__ == '__main__':
    app.run(debug=True)
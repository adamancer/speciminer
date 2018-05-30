import re

from flask import Flask, make_response, render_template, request
from sqlalchemy import and_

import sys
sys.path.append('..')
from database.database import Document, Journal, Link, Specimen, Snippet
from database.queries import Query
from specimens import get_specimens


DB = Query()

app = Flask(__name__)

@app.route('/')
@app.route('/documents')
def index():
    query = DB.query(Document) \
              .filter(Document.num_specimens > 0) \
              .order_by(Document.title)
    documents = query.all()
    return render_template('documents.htm', documents=documents)


@app.route('/documents/<doc_id>')
def document(doc_id):
    query = DB.query(Document.id,
                     Document.title,
                     Document.journal,
                     Document.doi,
                     Document.year,
                     Document.topic.label('doc_topic'),
                     Journal.topic.label('jour_topic'),
                     Snippet.snippet,
                     Link.spec_num,
                     Link.ezid,
                     Link.department,
                     Link.match_quality) \
              .join(Journal, Journal.title == Document.journal) \
              .join(Snippet, Snippet.doc_id == Document.id) \
              .join(Link, Link.doc_id ==Document.id) \
              .filter(Document.id == doc_id)
    rows = query.all()
    doc = rows[0]
    spec_nums = {}
    for row in rows:
        ezids = [s.strip() for s in row.ezid.split('|')] if row.ezid else []
        try:
            spec_nums[row.spec_num]['ezids'].extend(ezids)
            spec_nums[row.spec_num]['ezids'] = sorted(list(set(spec_nums[row.spec_num]['ezids'])))
        except:
            spec_nums[row.spec_num] = {
                'spec_num': row.spec_num,
                'ezids': ezids,
                'dept': row.department,
                'match_quality': row.match_quality,
                'snippets': []
                }
    spec_nums = spec_nums.values()
    # Find related snippets
    query = DB.query(Snippet.snippet, Specimen.spec_num) \
              .join(Specimen, Specimen.snippet_id == Snippet.id) \
              .filter(and_(Snippet.doc_id == doc_id))
    snippets = {}
    for row in query.all():
        snippets.setdefault(row.spec_num, []).append(row.snippet)
    for spec_num in spec_nums:
        spec_num['snippets'] = snippets[spec_num['spec_num']]
    print spec_nums
    spec_nums.sort(key=lambda s: s['spec_num'])
    return render_template('document.htm', doc=doc, spec_nums=spec_nums)


@app.route('/specimens/<ezid>')
def specimen(ezid):
    # Get basic info about this specimen from the portal
    metadata = get_specimens(guid=ezid)
    if metadata:
        metadata = metadata[0]
    query = DB.query(Link.spec_num,
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

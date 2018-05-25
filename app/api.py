import re

from flask import Flask, make_response, render_template, request

import sys
sys.path.append('..')
from database.database import Document, Journal, Specimen, Snippet
from database.queries import Query


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
                     Specimen.spec_num) \
              .join(Journal, Journal.title == Document.journal) \
              .join(Snippet, Snippet.doc_id == Document.id) \
              .join(Specimen, Specimen.snippet_id == Snippet.id) \
              .filter(Document.id == doc_id)
    rows = query.all()
    doc = rows[0]
    spec_nums = []
    for row in rows:
        spec_nums.append(row.spec_num)
    spec_nums = sorted(list(set(spec_nums)))
    return render_template('document.htm', doc=doc, spec_nums=spec_nums)


@app.route('/specimens/<spec_num>')
def specimen(spec_num):
    spec_num = spec_num.replace('+', ' ')
    query = DB.query(Specimen.spec_num,
                     Snippet.doc_id,
                     Snippet.snippet,
                     Document.title) \
              .join(Snippet, Snippet.id == Specimen.snippet_id) \
              .join(Document, Document.id == Snippet.doc_id) \
              .filter(Specimen.spec_num == spec_num) \
              .order_by(Document.title)
    rows = query.all()
    specimen = rows[0]
    documents = {}
    for row in rows:
        doc = documents.setdefault(row.doc_id, {})
        doc['title'] = row.title
        doc.setdefault('snippets', {}) \
           .setdefault(format_snippet(row.snippet), []) \
           .append(row.spec_num)
    return render_template('specimen.htm', specimen=specimen, documents=documents)


def format_snippet(snippet):
    return snippet.replace('**', '')
    return re.sub(r'\*\*(.*?)\*\*', r'<b>\1</b>', snippet)



if __name__ == '__main__':
    app.run(debug=True)

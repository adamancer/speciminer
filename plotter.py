"""Plots citations and publications featuring USNM specimens"""

import os
import re

import matplotlib.pyplot as plt

from database.database import Document, Link, Journal
from database.queries import Query


DB = Query()


def round_down(val, base=5):
    return int(base * round(float(val) / base))


def round_up(val, base=5):
    return int(base * round(float(val) / base))


def slugify(val):
    return re.sub('[():/]', '', val.lower().strip().replace(' ', '_'))


def tabulate_citations(fn='citations.md'):
    depts = {
        'an': 'Anthropology',
        'bt': 'Botany',
        'br': 'Vertebrate Zoology: Birds',
        'en': 'Entomology',
        'fs': 'Vertebrate Zoology: Fishes',
        'hr': 'Vertebrate Zoology: Herpetology',
        'iz': 'Invertebrate Zoology',
        'mm': 'Vertebrate Zoology: Mammals',
        'ms': 'Mineral Sciences',
        'pl': 'Paleobiology'
    }
    citations = {}
    # Find citations
    query = DB.query(Link.department, Link.doc_id) \
              .filter(Link.department != None)
    for row in query.all():
        try:
            citations[row.department]
        except KeyError:
            citations[row.department] = {
                'citations': 1,
                'pubs_cited': [row.doc_id],
                'pubs_topic': []
            }
        else:
            citations[row.department]['citations'] += 1
            citations[row.department]['pubs_cited'].append(row.doc_id)
    # Find publications
    query = DB.query(Document.id,
                     Document.topic.label('doc_topic'),
                     Journal.topic.label('jour_topic')) \
              .join(Journal, Journal.title == Document.journal)
    for row in query.all():
        topic = row.doc_topic.rstrip('?') if row.doc_topic else None
        dept = depts.get(topic)
        if dept:
            citations[dept]['pubs_topic'].append(row.id)
    rows = []
    for dept, stats in citations.iteritems():
        rows.append([dept,
                     stats['citations'],
                     len(set(stats['pubs_cited'])),
                     len(set(stats['pubs_topic']))])
    colnames = ['Department', '# citations', '# pubs with citations', '# pubs on topic']
    _tabulate(os.path.join('docs', fn), rows, colnames)


def _tabulate(fp, rows, colnames=None, sortindex=0):
    assert colnames is None or len(rows[0]) == len(colnames)
    if sortindex is not None:
        rows.sort(key=lambda row: row[sortindex])
    if colnames:
        rows.insert(0, colnames)
    # Use the length of each cell to calculate padding
    cols = []
    for row in rows:
        for i, val in enumerate(row):
            try:
                cols[i].append(len(str(val)))
            except (AttributeError, IndexError):
                cols.append([len(str(val))])
    cols = [max(col) for col in cols]
    # Add border
    border = ['-' * (col + 2) for col in cols]
    #rows.insert(0, border)
    #rows.append(border)
    if colnames:
        rows.insert(1, border)
    # Write table to file
    with open(fp, 'wb') as f:
        for row in rows:
            # Left-justify text and right-justify numbers
            row = [val.ljust(cols[i]) if isinstance(val, basestring)
                   else str(val).rjust(cols[i]) for i, val in enumerate(row)]
            # Pad non-border cells
            row = [' {} '.format(val) if val.strip('-') else val for val in row]
            f.write(u'|{}|\n'.format('|'.join(row)))


def plot_citations(include=None):
    years = []
    data = {}
    depts = [r.department for r in DB.query(Link.department).distinct().all()]
    depts = sorted([dept for dept in depts if dept])
    for dept in depts:
        if include is not None and dept not in include:
            continue
        query = DB.query(Link.ezid, Document.year) \
                  .join(Document, Document.id == Link.doc_id) \
                  .filter(Link.department == dept)
        for row in query.all():
            data.setdefault(dept, {}).setdefault(int(row.year), []).append(1)
        for year, citations in data[dept].iteritems():
            data[dept][year] = len(citations)
            years.append(int(year))
    rng = '{}-{}'.format(min(years), max(years))
    years = [y for y in xrange(min(years), max(years) + 1)]
    return _plot(data, years, include,
                 title='Citations of USNM specimens ({})'.format(rng),
                 ylabel='# citations')



def plot_papers(include=None):
    years = []
    data = {}
    depts = [r.department for r in DB.query(Link.department).distinct().all()]
    depts = sorted([dept for dept in depts if dept])
    for dept in depts:
        if include is not None and dept not in include:
            continue
        query = DB.query(Link.ezid, Document.id, Document.year) \
                  .join(Document, Document.id == Link.doc_id) \
                  .filter(Link.department == dept)
        for row in query.all():
            data.setdefault(dept, {}).setdefault(int(row.year), []).append(row.id)
        for year, documents in data[dept].iteritems():
            data[dept][year] = len(set(documents))
            years.append(int(year))
    rng = '{}-{}'.format(min(years), max(years))
    years = [y for y in xrange(min(years), max(years) + 1)]
    return _plot(data, years, include,
                 title='Publications citing USNM specimens ({})'.format(rng),
                 ylabel='# publications')


def plot_all_papers():
    years = []
    data = {}
    query = DB.query(Document.id, Document.year)
    for row in query.all():
        data.setdefault('All', {}).setdefault(int(row.year), []).append(row.id)
    for year, documents in data['All'].iteritems():
        data['All'][year] = len(set(documents))
        years.append(int(year))
    rng = '{}-{}'.format(min(years), max(years))
    years = [y for y in xrange(min(years), max(years) + 1)]
    return _plot(data, years,
                 title='Publications mentioning USNM/NMNH ({})'.format(rng),
                 ylabel='# publications')



def _plot(data, years, include=None, **metadata):
    assert include is None or isinstance(include, list)
    labels = []
    rows = []
    for dept in sorted(data):
        if include is None or dept in include:
            vals = data[dept]
            labels.append(dept.replace('Vertebrate Zoology', 'VZ'))
            rows.append([vals.get(year, 0) for year in years])
    fig = plt.figure(figsize=(7.5, 3))
    ax = fig.add_subplot(111)
    width = 0.8
    bars = []
    bottom = []
    for row in rows:
        if bottom:
            bars.append(ax.bar(years, row, width, bottom=bottom))
            bottom =[bottom[i] + r for i, r in enumerate(row)]
        else:
            bars.append(ax.bar(years, row, width))
            bottom = row
    for key, val in metadata.iteritems():
        getattr(ax, 'set_' + key)(val)
    ax.set_xticks(xrange(round_down(min(years) - 5, 10),
                         round_up(max(years), 10) + 5, 10))
    plt.legend([bar[0] for bar in bars], labels, prop={'size': 7})
    dept = ' '.join(include) if include else ''
    fp = os.path.join('docs', slugify(dept + ' ' + metadata['title']) + '.png')
    fig.savefig(fp, dpi=300, bbox_inches='tight')
    plt.close()

if __name__ == '__main__':
    tabulate_citations()
    plot_citations()
    plot_papers()
    plot_all_papers()

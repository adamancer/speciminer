"""Plots citations and publications featuring USNM specimens"""

import os
import re

import matplotlib.pyplot as plt

from database.database import Document, Link
from database.queries import Query


DB = Query()


def round_down(val, base=5):
    return int(base * round(float(val) / base))


def round_up(val, base=5):
    return int(base * round(float(val) / base))


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


def _plot(data, years, include=None, **metadata):
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
    slug = re.sub('[()]', '', metadata['title'].lower().replace(' ', '_'))
    fp = os.path.join('docs', slug + '.png')
    fig.savefig(fp, dpi=300, bbox_inches='tight')

include = None#['Anthropology']
plot_citations(include=include)
plot_papers(include=include)

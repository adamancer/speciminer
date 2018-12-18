"""Plots citations and publications featuring USNM specimens"""

import os
import re

import matplotlib as mpl
import matplotlib.pyplot as plt
from cycler import cycler
from sqlalchemy import and_, or_

from database.database import Document, Link, Journal
from database.queries import Query


DB = Query()
QUALITY = 'Match%'


def parse_year(val):
    """Attempts to parse a four-digit year from a value"""
    try:
        return int(val[:4])
    except (TypeError, ValueError):
        return 0


def get_years(years):
    try:
        years.remove(0)
    except ValueError:
        pass
    return [y for y in xrange(min(years), max(years) + 1)]


def round_down(val, base=5):
    return int(base * round(float(val) / base))


def round_up(val, base=5):
    return int(base * round(float(val) / base))


def slugify(val):
    """Converts an arbitary value to something suitable for a filename"""
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
              .filter(and_(Link.department != None,
                           Link.match_quality.like(QUALITY)))
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
            try:
                citations[dept]['pubs_topic'].append(row.id)
            except:
                citations[dept] = {
                    'citations': 0,
                    'pubs_cited': [],
                    'pubs_topic': [row.id]
                }
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
                  .filter(and_(Link.department == dept,
                               Link.match_quality.like(QUALITY)))
        for row in query.all():
            data.setdefault(dept, {}).setdefault(parse_year(row.year), []).append(1)
        for year, citations in data.get(dept, {}).iteritems():
            if year:
                data[dept][year] = len(citations)
                years.append(int(year))
    # Add specimen numbers that occur in multiple snippets but could
    # not be assigned
    query = DB.query(Link.ezid, Document.year) \
              .join(Document, Document.id == Link.doc_id) \
              .filter(and_(~Link.match_quality.like(QUALITY),
                           or_(Link.num_snippets > 2,
                               Link.spec_num.like('__N_ ______'))))
    dept = 'Unassigned'
    for row in query.all():
        data.setdefault(dept, {}).setdefault(parse_year(row.year), []).append(1)
    for year, citations in data.get(dept, {}).iteritems():
        data[dept][year] = len(citations)
        years.append(int(year))
    years = get_years(years)
    rng = '{}-{}'.format(min(years), max(years))
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
                  .filter(and_(Link.department == dept,
                               Link.match_quality.like(QUALITY)))
        for row in query.all():
            data.setdefault(dept, {}).setdefault(parse_year(row.year), []).append(row.id)
        if data:
            for year, documents in data[dept].iteritems():
                if year:
                    data[dept][year] = len(set(documents))
                    years.append(int(year))
    # Add specimen numbers that occur in multiple snippets but could
    # not be assigned
    query = DB.query(Link.ezid, Document.id, Document.year) \
              .join(Document, Document.id == Link.doc_id) \
              .filter(and_(~Link.match_quality.like(QUALITY),
                           or_(Link.num_snippets > 2,
                               Link.spec_num.like('__N_ ______'))))
    dept = 'Unassigned'
    for row in query.all():
        data.setdefault(dept, {}).setdefault(parse_year(row.year), []).append(row.id)
    if data:
        for year, documents in data[dept].iteritems():
            if year:
                data[dept][year] = len(set(documents))
                years.append(int(year))
    years = get_years(years)
    rng = '{}-{}'.format(min(years), max(years))
    return _plot(data, years, include,
                 title='Publications citing USNM specimens ({})'.format(rng),
                 ylabel='# publications')


def plot_all_papers():
    years = []
    data = {}
    query = DB.query(Document.id, Document.year)
    for row in query.all():
        data.setdefault('All', {}).setdefault(parse_year(row.year), []).append(row.id)
    for year, documents in data['All'].iteritems():
        data['All'][year] = len(set(documents))
        years.append(int(year))
    years = get_years(years)
    rng = '{}-{}'.format(min(years), max(years))
    return _plot(data, years,
                 title='Publications mentioning USNM/NMNH ({})'.format(rng),
                 ylabel='# publications')



def _plot(data, years, include=None, **metadata):
    assert include is None or isinstance(include, list)
    labels = []
    rows = []
    depts = sorted(data)
    # Shuffle unassigned to the end of the list if it exists
    try:
        depts.append(depts.pop(depts.index('Unassigned')))
    except ValueError:
        pass
    for dept in depts:
        if include is None or dept in include:
            vals = data[dept]
            labels.append(dept.replace('Vertebrate Zoology', 'VZ'))
            rows.append([vals.get(year, 0) for year in years])
    fig = plt.figure(figsize=(7.5, 3))
    ax = fig.add_subplot(111)
    ax.grid(axis='y', color=(0.3, 0.3, 0.3, 1), linestyle='-', linewidth=0.1, zorder=1)
    width = 0.8
    bars = []
    bottom = []
    for row in rows[::-1]:
        if bottom:
            bars.append(ax.bar(years, row, width, bottom=bottom, zorder=2))
            bottom =[bottom[i] + r for i, r in enumerate(row)]
        else:
            bars.append(ax.bar(years, row, width))
            bottom = row
    for key, val in metadata.iteritems():
        getattr(ax, 'set_' + key)(val)
    ax.set_xticks(xrange(round_down(min(years) - 5, 10),
                         round_up(max(years), 10) + 5, 10))
    plt.legend([bar[0] for bar in bars[::-1]], labels, prop={'size': 7})
    dept = ' '.join(include) if include else ''
    fp = os.path.join('docs', slugify(dept + ' ' + metadata['title']) + '.png')
    fig.savefig(fp, dpi=300, bbox_inches='tight')
    plt.close()

if __name__ == '__main__':
    # Set colorblind-safe palette
    colors = [
        #(0, 0, 0),
        (0, 73, 73),
        (0, 146, 146),
        (255, 109, 182),
        (255, 182, 219),
        (73, 0, 146),
        (0, 109, 219),
        (182, 109, 255),
        (109, 182, 255),
        (182, 219, 255),
        (146, 0, 0),
        (146, 73, 0),
        (219, 209, 0),
        (36, 255, 36),
        (255, 255, 109),
    ]
    colors = [[c / 255. for c in color] for color in colors]
    mpl.rcParams['axes.prop_cycle'] = cycler(color=colors)
    # Construct tables and pltos
    tabulate_citations()
    plot_citations()
    plot_papers()
    plot_all_papers()

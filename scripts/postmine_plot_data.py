"""Plots citations and publications featuring NMNH specimens"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

from builtins import next
from builtins import zip
from builtins import str
from builtins import range
from past.builtins import basestring

import csv
import glob
import logging
import os
import pprint as pp
import re
import shutil
import sys

import matplotlib as mpl
import matplotlib.pyplot as plt
from cycler import cycler
from sqlalchemy import and_, or_

sys.path.insert(0, '..')
from config.constants import OUTPUT_DIR
from database.database import Document, Link, Journal
from database.queries import Query




logger = logging.getLogger('speciminer')
logger.info('Running postmine_plot_data.py')


DB = Query()
QUALITY_LIKE = 'Match%'
MIN_YEAR = None
MAX_YEAR = None
DEPTS = None

DEPTMAP = {
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

COLORS = [
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
][:len(DEPTMAP) + 1]

COLORMAP = {k: v for k, v in zip(sorted(DEPTMAP.values()) + ['Unassigned'], COLORS[::-1])}


def parse_year(val):
    """Attempts to parse a four-digit year from a value"""
    try:
        return int(val[:4])
    except (TypeError, ValueError):
        return 0


def parse_dept(dept, include=None):
    try:
        dept = dept.rstrip('*')
    except AttributeError:
        return
    else:
        dept = DEPTMAP.get(dept, dept)
        if dept not in list(DEPTMAP.keys()) + list(DEPTMAP.values()):
            return
        if include and dept not in include:
            return
        return dept


def get_years(years):
    try:
        years.remove(0)
    except ValueError:
        pass
    min_yr = min(years) if not MIN_YEAR else MIN_YEAR
    max_yr = max(years) if not MAX_YEAR else MAX_YEAR
    return [y for y in range(min_yr, max_yr + 1) if min_yr <= y <= max_yr]


def round_down(val, base=5):
    """Rounds value down to nearest base"""
    return int(base * round(float(val) / base))


def round_up(val, base=5):
    """Rounds value up to nearest base"""
    return int(base * round(float(val) / base))


def slugify(val):
    """Converts an arbitary value to something suitable for a filename"""
    val = val.replace(' .', '.').strip(' _')
    return re.sub('[():/, ]+', '_', val.lower()).strip('_')


def tabulate_citations(fn='citations.md', include=None):
    citations = {}
    # Find citations
    obj_matches = get_object_matches(include=include)
    for dept, years in obj_matches.items():
        for _, doc_ids in years.items():
            for doc_id in doc_ids:
                try:
                    citations[dept]['matched_object'] += 1
                    citations[dept]['pubs_citing'].append(doc_id)
                except KeyError:
                    citations[dept] = {
                        'matched_object': 1,
                        'matched_dept': 0,
                        'pubs_citing': [doc_id],
                        'pubs_on_topic': []
                    }
    dept_matches = get_dept_matches(include=include)
    for dept, years in dept_matches.items():
        for _, doc_ids in years.items():
            for doc_id in doc_ids:
                try:
                    citations[dept]['matched_dept'] += 1
                    citations[dept]['pubs_citing'].append(doc_id)
                except KeyError:
                    citations[dept] = {
                        'matched_object': 0,
                        'matched_dept': 1,
                        'pubs_citing': [doc_id],
                        'pubs_on_topic': []
                    }
    # Find publications
    query = DB.query(Document.id,
                     Document.topic.label('doc_topic'),
                     Journal.topic.label('jour_topic')) \
              .join(Journal, Journal.title == Document.journal)
    for row in query.all():
        topic = row.doc_topic.rstrip('?*') if row.doc_topic else None
        dept = DEPTMAP.get(topic)
        if dept:
            try:
                citations[dept]['pubs_on_topic'].append(row.id)
            except:
                citations[dept] = {
                    'matched_object': 0,
                    'matched_dept': 0,
                    'pubs_citing': [],
                    'pubs_on_topic': [row.id]
                }
    rows = []
    for dept, stats in citations.items():
        rows.append([dept,
                     stats['matched_object'],
                     stats['matched_dept'],
                     len(set(stats['pubs_citing'])),
                     len(set(stats['pubs_on_topic']))])
    colnames = ['Department', '# matched object', '# matched dept', '# pubs with citations', '# pubs on topic']
    _tabulate(os.path.join('plots', fn), rows, colnames)


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
    with open(fp, 'w') as f:
        for row in rows:
            # Left-justify text and right-justify numbers
            row = [val.ljust(cols[i]) if isinstance(val, basestring)
                   else str(val).rjust(cols[i]) for i, val in enumerate(row)]
            # Pad non-border cells
            row = [' {} '.format(val) if val.strip('-') else val for val in row]
            f.write(u'|{}|\n'.format('|'.join(row)))


def is_good_catnum(val):
    good = re.match('^(NMNH|USNM) ([A-Z] ?)?\d{5,6}([A-Z]|-\d{1,2})?$', val, flags=re.I)
    if not good:
        good = re.match('^(NMNH|USNM) type no\. \d{3,6}$', val, flags=re.I)
    #print val, '=>', bool(good)
    return bool(good)


def get_object_matches(include=None):
    """Gets citations that strongly match a specific object"""
    data = {}
    depts = [parse_dept(r.department, include=include)
             for r in DB.query(Link.department).distinct().all()]
    depts = sorted(list(set([dept for dept in depts if dept])))
    for dept in depts:
        if include is not None and dept not in include:
            continue
        query = DB.query(Link.ezid, Document.id, Document.year) \
                  .join(Document, Document.id == Link.doc_id) \
                  .filter(and_(Link.department.like(dept + '%'),
                               Link.ezid != None,
                               Link.match_quality.like(QUALITY_LIKE)))
        for row in query.all():
            data.setdefault(dept, {}) \
                .setdefault(parse_year(row.year), []) \
                .append(row.id)
    return data


def get_dept_matches(include=None):
    """Gets citations that match a dept and have a high-quality catnum"""
    data = {}
    depts = [parse_dept(r.department, include=include)
             for r in DB.query(Link.department).distinct().all()]
    depts = sorted(list(set([dept for dept in depts if dept])))
    for dept in depts:
        if include is not None and dept not in include:
            continue
        query = DB.query(Link.ezid,
                         Link.spec_num,
                         Link.corrected,
                         Document.id,
                         Document.year) \
                  .join(Document, Document.id == Link.doc_id) \
                  .filter(Link.department.like(dept + '%'),
                          Link.doc_id.like('gdd%'),
                          or_(Link.spec_num.like('NMNH%'),
                              Link.spec_num.like('USNM%')),
                          or_(Link.ezid != None,
                              Link.num_snippets >= 1))
        for row in query.all():
            spec_num = row.corrected if row.corrected else row.spec_num
            if row.ezid or is_good_catnum(spec_num):
                data.setdefault(dept, {}) \
                    .setdefault(parse_year(row.year), []) \
                    .append(row.id)
    return data


def get_sample(like, dept=None):
    data = {}
    fltr = [Link.spec_num.like(like)]
    if dept is not None:
        fltr.append(Link.department.like(dept + '%'))
    query = DB.query(Link.spec_num,
                     Link.department,
                     Link.doc_id,
                     Document.year) \
              .join(Document, Document.id == Link.doc_id) \
              .filter(*fltr)
    for row in query:
        row_dept = row.department.rstrip('*') if row.department else 'Unassigned'
        data.setdefault(row_dept, {}) \
            .setdefault(parse_year(row.year), []) \
            .append(row.doc_id)
    return data


def get_topics(include=None):
    """Gets citations that match a dept and have a high-quality catnum"""
    data = {}
    depts = [parse_dept(r.topic, include=include)
             for r in DB.query(Document.topic).distinct().all()]
    depts = sorted(list(set([dept for dept in depts if dept])))
    lookup = {v: k for k, v in DEPTMAP.items()}
    for dept in depts:
        if include is not None and dept not in include:
            continue
        code = lookup[dept.rstrip('*')]
        query = DB.query(Document).filter(Document.topic.like(code + '%'))
        for row in query.all():
            data.setdefault(dept, {}) \
                .setdefault(parse_year(row.year), []) \
                .append(row.id)
    return data


def get_unassigned_citations():
    """Gets specimen nums that occur in multiple snippets but have no dept"""
    query = DB.query(Link.ezid,
                     Link.spec_num,
                     Link.corrected,
                     Document.id,
                     Document.year) \
              .join(Document, Document.id == Link.doc_id) \
              .filter(and_(~Link.match_quality.like(QUALITY_LIKE),
                           or_(Link.num_snippets >= 2,
                               or_(Link.spec_num.like('NMNH%'),
                                   Link.spec_num.like('USNM%')))))
    unassigned = {}
    for row in query.all():
        spec_num = row.corrected if row.corrected else row.spec_num
        if is_good_catnum(spec_num):
            unassigned.setdefault('Unassigned', {}) \
                      .setdefault(parse_year(row.year), []) \
                      .append(row.id)
    return unassigned


def plot_citations(include=None, exact=True, normalize=False):
    if exact:
        data = get_object_matches(include)
    else:
        data = get_dept_matches(include)
    unassigned = get_unassigned_citations()
    if unassigned:
        data.update(unassigned)
    years = []
    for dept in data:
        for year, citations in data.get(dept, {}).items():
            data[dept][year] = len(citations)
            years.append(int(year))
    if normalize:
        data, years = _normalize(data)
    title = set_title('Citations of NMNH specimens', years,
                      include=include, exact=exact, normalize=normalize)
    ylabel = set_label('# pubs', normalize=normalize)
    return _bar(data, years, include=include, title=title, ylabel=ylabel)


def plot_papers(include=None, exact=True, normalize=False):
    if exact:
        data = get_object_matches(include)
    else:
        data = get_dept_matches(include)
    unassigned = get_unassigned_citations()
    if unassigned:
        data.update(unassigned)
    years = []
    for dept in data:
        for year, citations in data.get(dept, {}).items():
            data[dept][year] = len(set(citations))
            years.append(int(year))
    if normalize:
        data, years = _normalize(data)
    title = set_title('Publications citing NMNH specimens', years,
                      include=include, exact=exact, normalize=normalize)
    ylabel = set_label('# pubs', normalize=normalize)
    return _bar(data, years, include=include, title=title, ylabel=ylabel)


def plot_topics(include=None, normalize=False):
    data = get_topics(include)
    years = []
    for dept in data:
        for year, documents in data.get(dept, {}).items():
            data[dept][year] = len(set(documents))
            years.append(int(year))
    if normalize:
        data, years = _normalize(data)
    title = set_title('Publications by topic', years,
                     include=include, normalize=normalize)
    ylabel = set_label('# pubs', normalize=normalize)
    return _bar(data, years, include=include, title=title, ylabel=ylabel)


def plot_one_sample(catnum, like=None, dept=None):
    data = get_sample(like if like is not None else catnum, dept=dept)
    include = list(data.keys())
    years = []
    for dept in data:
        for year, documents in data.get(dept, {}).items():
            data[dept][year] = len(set(documents))
            years.append(int(year))
    title = set_title('Citations of {}'.format(catnum), years)
    ylabel = set_label('# pubs')
    return _bar(data, years, include=include, title=title, ylabel=ylabel)


def plot_all_papers(normalize=False):
    """Plots all papers that mentioned USNM/NMNH"""
    years = []
    data = {}
    query = DB.query(Document.id, Document.year) \
              .filter(Document.id.like('bhl%'))
    for row in query.all():
        data.setdefault('All', {}).setdefault(parse_year(row.year), []).append(row.id)
    for year, documents in data['All'].items():
        data['All'][year] = len(set(documents))
        years.append(int(year))
    if normalize:
        data, years = _normalize(data)
    title = set_title('Publications mentioning USNM/NMNH', years, normalize=normalize)
    ylabel = set_label('# pubs', normalize=normalize)
    return _bar(data, years, title=title, ylabel=ylabel)


def plot_corpii(include=None, refdata=None):
    data, years = read_corpii(os.path.join(OUTPUT_DIR, 'combined.csv'), include=include)
    years = get_years(years)
    name = ' and '.join(include)
    rng = '{}-{}'.format(min(years), max(years))
    return _bar(data,
                years,
                refdata=refdata,
                title='Publications per year in {} ({})'.format(name, rng),
                ylabel='# pubs')


def plot_loans(include=None):
    data, years = read_loans(os.path.join(OUTPUT_DIR, 'combined.csv'), include=include)
    years = get_years(years)
    title = set_title('NMNH outgoing loans', years, include=include)
    return _bar(data,
                years,
                title=title,
                ylabel='# loans')


def plot_pubs_per_loan(include=None):
    objects = get_dept_matches(include=include)
    loans = read_loans(os.path.join(OUTPUT_DIR, 'combined.csv'), include=include)
    # Normalize data for scatter plot
    normalized = {}
    years = []
    for dept, data in objects.items():
        for year, docs in data.items():
            if year <= 1997:
                count = len(set(docs))
                try:
                    normalized.setdefault(dept, {})[year] = count / loans[0][dept][year - 2]
                except (KeyError, ZeroDivisionError):
                    pass
                else:
                    years.append(int(year))
    return _scatter(normalized,
                    years,
                    title= set_title('Publications per loan', years),
                    ylabel='publications per loan')


def oxford_comma(vals):
    assert isinstance(vals, list)
    vals = [s.strip() for s in vals if s.strip()]
    if len(vals) == 1:
        return vals[0]
    return ', '.join(vals[:-1]) + ', and ' + vals[-1]


def set_title(title, years, include=None, exact=False, normalize=False):
    if include:
        title = title.replace('NMNH', 'NMNH ' + oxford_comma(include))
    title = title.rstrip(' ') + ' ({})'
    trailer = []
    years = get_years(years)
    trailer.append('{}-{}'.format(min(years), max(years)))
    if exact:
        trailer.insert(0, 'exact')
    if normalize:
        trailer.insert(0, 'norm')
    return title.format(', '.join(trailer).strip())


def set_label(label, normalize=False):
    if normalize:
        label += ' per pub in corpus'
    return label


def read_corpii(fp, include=None):
    if include is None:
        include = ['BHL', 'GDD']
    if not isinstance(include, list):
        include = [include]
    years = []
    data = {}
    with open(fp, 'r') as f:
        rows = csv.reader(f, delimiter=',')
        keys = next(rows)
        for row in rows:
            rowdata = {k: v for k, v in zip(keys, row)}
            for key in include:
                data.setdefault(key, {})[parse_year(rowdata['Year'])] = int(rowdata[key])
            years.append(int(rowdata['Year']))
    return data, years


def read_loans(fp, include=None):
    data = {}
    years = []
    with open(fp, 'r') as f:
        rows = csv.reader(f, delimiter=',')
        keys = next(rows)
        for row in rows:
            rowdata = {k: v for k, v in zip(keys, row)}
            for dept in keys[3:]:
                dept = parse_dept(dept, include=include)
                if dept:
                    count = int(rowdata[dept]) if rowdata[dept] else 0
                    data.setdefault(dept, {}).setdefault(parse_year(rowdata['Year']), count)
            years.append(int(rowdata['Year']))
    return data, years


def find_examples():
    docs = {}
    for row in DB.query(Link).filter(Link.doc_id.like('g%')).order_by(Link.doc_id):
        doc = docs.setdefault(row.doc_id, [])
        doc.append(row.match_quality)
        if row.department:
            doc.append(row.department)
    #for doc_id, matches in docs.iteritems():
    #    print doc_id, matches


def _normalize(data, include=None):
    corpii = read_corpii(os.path.join(OUTPUT_DIR, 'combined.csv'))
    # Combine data from the two corpii
    combined = {}
    for key, corpus in corpii[0].items():
        for year, count in corpus.items():
            try:
                combined[year] += count
            except KeyError:
                combined[year] = count
    normalized = {}
    years = []
    for key, stats in data.items():
        for year, count in stats.items():
            if MIN_YEAR <= year <= MAX_YEAR:
                normalized.setdefault(key, {})[year] = count / combined[year]
                years.append(int(year))
    return normalized, years


def _scatter(data, years, include=None, **metadata):
    assert include is None or isinstance(include, list)
    labels = []
    rows = []
    depts = sorted(data)
    years = get_years(years)
    # Shuffle unassigned to the end of the list if it exists
    if include:
        try:
            depts.remove('Unassigned')
        except ValueError:
            pass
    try:
        depts.append(depts.pop(depts.index('Unassigned')))
    except ValueError:
        pass
    set_colors(depts)
    for dept in depts:
        if include is None or dept in include:
            vals = data[dept]
            labels.append(dept.replace('Vertebrate Zoology', 'VZ'))
            rows.append([vals.get(year, 0) for year in years])
    fig = plt.figure(figsize=(8, 6))
    ax = fig.add_subplot(111)
    ax.grid(axis='y', color=(0.3, 0.3, 0.3, 1), linestyle='-', linewidth=0.1, zorder=1)
    width = 0.8
    colors = []
    points = []
    for row, label in zip(rows, labels)[::-1]:
        try:
            color = colors.pop(0)
        except:
            colors = [c['color'] for c in list(mpl.rcParams['axes.prop_cycle'])]
            color = colors.pop(0)
        for x, y in zip(years, row):
            if y:
                points.append(ax.plot(x, y, 'o', color=color))
    for key, val in metadata.items():
        getattr(ax, 'set_' + key)(val)
    ax.set_xticks(range(round_down(min(years) - 5, 10),
                         round_up(max(years), 10) + 5, 10))
    # Only include legend if more than one series is being plotted
    legend = []
    for point in points:
        if point[0]._color not in [pt[0]._color for pt in legend]:
            legend.insert(0, point)
    if len(legend) > 1:
        plt.legend([pt[0] for pt in legend], labels, prop={'size': 7})
    #dept = ' '.join(include) if include else ''
    fp = os.path.join('plots', slugify(metadata['title']) + '.png')
    fig.savefig(fp, dpi=300, bbox_inches='tight', transparent=True)
    plt.close()



def _bar(data, years, include=None, refdata=None, **metadata):
    assert include is None or isinstance(include, list)
    depts = sorted(data.keys())
    years = get_years(years)
    # Shuffle unassigned to the end of the list if it exists
    if include and 'Unassigned' not in include:
        try:
            depts.remove('Unassigned')
        except ValueError:
            pass
    try:
        depts.append(depts.pop(depts.index('Unassigned')))
    except ValueError:
        pass
    set_colors(depts)
    labels = []
    rows = []
    for dept in depts:
        if include is None or dept in include:
            vals = data[dept]
            labels.append(dept.replace('Vertebrate Zoology', 'VZ'))
            rows.append([vals.get(year, 0) for year in years])
    fig = plt.figure(figsize=(8, 3))
    ax = fig.add_subplot(111)
    ax.grid(axis='x', color=(0.7, 0.7, 0.7, 1), linestyle='-', linewidth=0.1, zorder=1)
    ax.grid(axis='y', color=(0.7, 0.7, 0.7, 1), linestyle='-', linewidth=0.1, zorder=1)
    width = 0.8
    bars = []
    bottom = []
    if refdata:
        row = [refdata[year] for year in years]
        bars.append(ax.bar(years, row, width, zorder=1, color=(0.9, 0.9, 0.9)))
        labels.append('Model')
    for row in rows[::-1]:
        if bottom:
            bars.append(ax.bar(years, row, width, bottom=bottom, zorder=2))
            bottom =[bottom[i] + r for i, r in enumerate(row)]
        else:
            bars.append(ax.bar(years, row, width))
            bottom = row
    for key, val in metadata.items():
        getattr(ax, 'set_' + key)(val)
    #ax.set_yticks([0, 100, 200, 300, 400, 500, 600])
    #ax.set_xticks(range(round_down(min(years) - 5, 10),
    #                     round_up(max(years), 10) + 5, 10))
    # Only include legend if more than one series is being plotted
    if len(bars) > 1:
        #for label, bar in zip(labels, bars[::-1]):
        #    print label, bar[0]._facecolor
        plt.legend([bar[0] for bar in bars[::-1]], labels, prop={'size': 7})
    #dept = ' '.join(include) if include else ''
    fp = os.path.join('plots', slugify(metadata['title']) + '.png')
    fig.savefig(fp, dpi=300, bbox_inches='tight', transparent=True)
    plt.close()


def set_colors(depts=None):
    try:
        colors = [COLORMAP[dept] for dept in depts][::-1]
    except:
        colors = COLORS
    #import pprint as pp
    #pp.pprint(COLORMAP)
    #pp.pprint(colors)
    colors = [[c / 255. for c in color] for color in colors]
    mpl.rcParams['axes.prop_cycle'] = cycler(color=colors)
    return colors


def model_pubs(num, year=2018):
    model = {year: num}
    while year >= MIN_YEAR:
        year -= 1
        num = int(num / 1.035) if year <= 2007 else int(num / 1.055)
        model[year] = num
    return model


if __name__ == '__main__':
    # Construct tables and plots
    #plot_one_sample('NMNH 111312', '% 111312%')
    #plot_one_sample('NMNH 113498', '% 113498%')
    #raw_input('paused')
    plot_all_papers(normalize=False)
    plot_all_papers(normalize=True)
    refdata = model_pubs(750000)
    plot_corpii(['BHL'])
    plot_corpii(['GDD'])
    plot_corpii(['BHL', 'GDD'], refdata=refdata)
    output_dir = os.path.join('plots', 'pubdata')
    try:
        os.mkdir(output_dir)
    except OSError:
        shutil.rmtree(output_dir)
        os.mkdir(output_dir)
    for fp in glob.iglob('plots/*'):
        shutil.move(fp, output_dir)
    # Plot by department
    for include in [None] + list(DEPTMAP.values()):
        print('Plotting {}...'.format(include))
        if include is not None:
            include = [include]
        tabulate_citations()
        for normalize in [False, True]:
            plot_citations(include=include, normalize=normalize)
            plot_citations(include=include, exact=False, normalize=normalize)
            plot_papers(include=include, normalize=normalize)
            plot_papers(include=include, exact=False, normalize=normalize)
            plot_topics(include=include, normalize=normalize)
            #break
        plot_loans(include=include)
        plot_pubs_per_loan(include=include)
        # Copy to department-specific folder
        name = 'all' if include is None else include[0]
        output_dir = os.path.join('plots', slugify(name))
        try:
            os.mkdir(output_dir)
        except OSError:
            shutil.rmtree(output_dir)
            os.mkdir(output_dir)
        for fp in glob.iglob('plots/*.*'):
            shutil.move(fp, output_dir)

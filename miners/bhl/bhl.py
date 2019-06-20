""""Defines tools for working with the BHL v3 endpoint"""
from __future__ import print_function
from __future__ import unicode_literals

from builtins import str
from builtins import range

import logging
import os
import re
import time
from collections import OrderedDict, namedtuple

import requests
import yaml
from lxml import etree
from sqlalchemy import and_, or_

from .database.database import Document, Journal, Part, Snippet, Specimen, Taxon
from .database.queries import Query
from .miners.parser import Parser




logger = logging.getLogger('speciminer')
logger.info('Loading bhl.py')




API_KEY = yaml.safe_load(open(os.path.join(
                            os.path.dirname(__file__),
                            '..',
                            '..',
                            'api_keys.yml'), 'r'))['bhl_api_key']
PARSER = Parser()
DB = Query()


Indexed = namedtuple('Indexed', ['specimens', 'docinfo', 'indexed'])
FakeReponse = namedtuple('FakeResponse', ['content', 'text', 'status_code'])

def _get(op, **params):
    params['op'] = op
    params['apikey'] = API_KEY
    url = 'https://www.biodiversitylibrary.org/api3'
    response = requests.get(url, params=params)
    if hasattr(response, 'from_cache') and not response.from_cache:
        time.sleep(5)
    if response.status_code == 200:
        logger.info('Request to %s succeeded', response.url)
    else:
        logger.info('Request to %s failed (%s)', op, response.url)
    return response


def get(op, **params):
    kill_codes = [400, 401, 402, 403, 404, 500]
    for i in range(1, 13):
        response = _get(op, **params)
        if response.status_code == 200:
            break
        elif response.status_code in kill_codes:
            logger.error('Failed to resolve %s (code=%s)', response.url, response.status_code)
            break
        else:
            logger.error('Failed to resolve %s (code=%s). Retrying...', response.url, response.status_code)
            print('Retrying in {} seconds...'.format(2**i))
            time.sleep(2**i)
    return etree.fromstring(response.content)


def get_matching_items(searchterm, searchtype='F', page=1):
    assert searchterm
    params = {
        'searchterm': searchterm,
        'searchtype': searchtype,
        'page': page
    }
    return get('PublicationSearch', **params)


def get_item_metadata(item_id, **kwargs):
    assert item_id
    params = {
        'pages': 'true',
        'ocr': 'true',
        'parts': 'true'
    }
    params['id'] = item_id
    params.update(**kwargs)
    return get('GetItemMetadata', **params)


def get_part_metadata(part_id, **kwargs):
    assert part_id
    params = {
        'pages': 'true',
        'names': 'true'
    }
    params.update(**kwargs)
    params['id'] = part_id
    return get('GetPartMetadata', **params)


def get_page_metadata(page_id, **kwargs):
    assert page_id
    params = {
        'ocr': 'true',
        'names': 'true'
    }
    params['pageid'] = page_id
    params.update(**kwargs)
    return get('GetPageMetadata', **params)


def get_title_metadata(title_id, **kwargs):
    assert title_id
    return get('GetTitleMetadata', id=title_id)


def get_title_ris(title_id, automap=True):
    raise Exception('GetTitleRIS is deprecated in BHL v3')
    assert title_id
    root = get('GetTitleRIS', titleid=title_id)
    if automap:
        return map_ris(root)
    return root


def populate_url_lookup():
    records = DB.query(Part.item_id, Part.part_id)
    lookup = {}
    for rec in records:
        for kind, id_ in (('item', rec.item_id), ('part', rec.part_id)):
            url = 'https://www.biodiversitylibrary.org/{}/{}'.format(kind, id_)
            lookup[url] = 1
    return lookup


def parse_item(item_id, part_id=None):
    """Returns metadata, parts, and pages for the given item or part"""
    root = get_item_metadata(item_id)
    # Get metadata for each part
    parts = root.xpath('/Response/Result/Item/Parts/Part')
    logger.info('Item %s contains %s parts', item_id, len(parts))
    if part_id is not None:
        parts = [p for p in parts if xmlget(p, 'PartID') == str(part_id)]
    parts = [map_part(part) for part in parts]
    # Save item metadata if processing the whole item
    if part_id is None:
        parts.insert(0, map_item(root))
    # Get text from each page
    pages = {}
    for page in root.xpath('/Response/Result/Item/Pages/Page'):
        pdata = map_page(page)
        pages[pdata['page_id']] = pdata['text']
    logger.info('Item %s contains %s pages', item_id, len(pages))
    # Map the pages to each part/item
    for part in parts:
        part['item_id'] = item_id
        part['source'] = 'BHL'
        # List comp throws KeyError if page number not found in pages dict
        #part['pages'] = OrderedDict([(p, pages[p]) for p in part['page_nums']])
        part['pages'] = OrderedDict()
        for page_num in part['page_nums']:
            try:
                part['pages'][page_num] = pages[page_num]
            except KeyError:
                logger.error('Page not found: %s', page_num)
        part['first_page'] = part['page_nums'][0]
        part['min_page'] = min(part['page_nums'])
        part['max_page'] = max(part['page_nums'])
        del part['page_nums']
    return parts


def extract_names():
    DB.bulk = True
    rows = DB.query(Snippet.page_id).distinct()
    for i, row in enumerate(rows):
        print('Checking for names in {} ({:,}/{:,})...'.format(row.page_id, i, rows.count()))
        if row.page_id is not None:
            page_id = int(row.page_id.split(':')[-1])
            root = get_page_metadata(page_id, name='true', ocr='false')
            taxa = [n.text for n in root.xpath('/Response/Result/Page/Names/Name/NameFound')]
            for taxon in taxa:
                DB.safe_add(Taxon, source_id=row.page_id, taxon=taxon)
    DB.commit()



def extract_items(searchterm, page=None, aggressive=True):
    print('Searching BHL for "{}"...'.format(searchterm))
    num_per_page = None
    if page is None:
        page = 1
        num_per_page = 200
    total = 0
    while True:
        print('Checking publications on page {}...'.format(page))
        root = get_matching_items(searchterm, page=page)
        publications = root.xpath('/Response/Result/Publication')
        total += len(publications)
        logger.info('Found {} publications for {} (total={})'.format(len(publications), searchterm, total))
        for pub in publications:
            key = '{}Url'.format(xmlget(pub, 'BHLType'))
            url = xmlget(pub, key)
            # Has the url already been checked?
            try:
                lookup[url]
            except KeyError:
                print('Processing {}...'.format(url, key))
                try:
                    route_request(url, aggressive=aggressive)
                except Exception as e:
                    logger.error('Failed to process {} ({})'.format(url, str(e)))
                lookup[url] = 1
        #print '{:,} records processed!'.format(total)
        page += 1
        if len(publications) != num_per_page:
            break
    #print '{:,} records processed!'.format(total)


def save_item(parts):
    if len(parts) > 1:
        parts[0]['pages'] = {}
    for part in parts:
        save_part(part)
    DB.commit()


def save_part(part):
    DB.bulk = False
    # Create a Journal record
    keys = ['journal']
    rec = {'title': v for k, v in part.items() if k in keys}
    if rec:
        DB.safe_add(Journal, **rec)
    # Create a Part record
    keys = ['id', 'source', 'item_id', 'part_id', 'first_page', 'min_page', 'max_page']
    rec = {k: v for k, v in part.items() if k in keys}
    DB.safe_add(Part, **rec)
    # Create a Document record
    keys = ['id', 'source', 'title', 'journal', 'year', 'doi']
    rec = {k: v for k, v in part.items() if k in keys}
    doc = DB.safe_add(Document, **rec)
    for page_id, text in part['pages'].items():
        # Create Snippet and Specimen records
        snippets = PARSER.snippets(text, num_chars=100)
        for match, snips in snippets.items():
            spec_nums = PARSER.parse(match)
            # Get unique snippets
            for snip in snips:
                DB.bulk = False
                rec = DB.safe_add(Snippet,
                                  snippet=snip.text,
                                  doc_id=doc.id,
                                  page_id='bhl:page:{}'.format(page_id),
                                  start=snip.start)
                DB.bulk = True
                for spec_num in spec_nums:
                    DB.safe_add(Specimen,
                                verbatim=match,
                                spec_num=spec_num,
                                snippet_id=rec.id)


def map_ris(root):
    ris = {}
    for line in xmlget(root, '/Response/Result', default='').splitlines():
        key, val = [s.strip() for s in line.split('-', 1)]
        ris[key] = val
        if line.startswith('ER'):
            break
    return {
        'title': ris.get('TI', '').rstrip('.'),
        'year': ris.get('PY', ''),
        'doi': ris.get('DO', '')
    }


def map_item(item):
    item_id = xmlget(item, '/Response/Result/Item/ItemID')
    title_id = xmlget(item, '/Response/Result/Item/TitleID')
    # Get item-level bibliographic information
    pub = get_title_metadata(title_id)
    title = xmlget(pub, '/Response/Result/Title/FullTitle')
    year = xmlget(item, '/Response/Result/Item/Year')
    metadata = {
        'id': 'bhl:item:{}'.format(item_id),
        'item_id': 'bhl:item:{}'.format(item_id),
        'title': title.rstrip('. '),
        'year': year
    }
    # Make request to get pages for this item
    xpath = '/Response/Result/Item/Pages/Page'
    pages = [xmlget(p, 'PageID', int) for p in item.xpath(xpath)]
    logger.info('Mapping %s pages to item %s', len(pages), item_id)
    metadata['page_nums'] = pages
    return metadata


def map_part(part):
    part_id = xmlget(part, 'PartID', int)
    # Get basic metadata for this part
    metadata = {
        'id': 'bhl:part:{}'.format(part_id),
        'part_id': part_id,
        'title': xmlget(part, 'Title', default='').rstrip('.'),
        'journal': xmlget(part, 'ContainerTitle', default='').rstrip('.'),
        'year': xmlget(part, 'Date'),
        'doi': xmlget(part, 'Doi')
    }
    # Make request to get pages for this part
    root = get_part_metadata(part_id)
    xpath = '/Response/Result/Part/Pages/Page'
    pages = [xmlget(p, 'PageID', int) for p in root.xpath(xpath)]
    if pages:
        logger.info('Mapping %s pages to part %s', len(pages), part_id)
        metadata['page_nums'] = pages
        return metadata
    print(etree.tostring(root, pretty_print=True))
    raise Exception


def map_page(page):
    page_id = xmlget(page, 'PageID', int)
    text = xmlget(page, 'OcrText', default='', coerce=str)
    text = re.sub(' \n+', ' ', text if text else '').strip()
    return {
        'page_id': page_id,
        'text': text
    }


def xmlget(root, path, default=None, coerce=None, stripchars=None):
    """Returns value at path in root, returning a default if not found"""
    try:
        val = root.xpath(path)[0].text
    except IndexError:
        val = default
    if val is not None and stripchars:
        val = val.strip(stripchars)
    if coerce:
        val = coerce(val)
    return val


def route_request(url, aggressive=True):
    """Routes BHL requests to the appropriate endpoint"""
    parsed = requests.compat.urlparse(url)
    if parsed.netloc == 'www.biodiversitylibrary.org':
        kind, id_ = parsed.path.strip('/').split('/')
        # Has this item already been indexed?
        item = db_find(**{'{}_id'.format(kind): id_})
        if item.indexed:
            return item
        # Item has not been indexed, so do that now
        if kind == 'page':
            page = get_page_metadata(id_, ocr='false', names='false')
            item_id = xmlget(page, '/Response/Result/Page/ItemID')
            logger.info('Mapped page {} to item {}'.format(id_, item_id))
        elif kind == 'part':
            part = get_part_metadata(id_)
            item_id = xmlget(part, '/Response/Result/Part/ItemID')
            logger.info('Mapped part {} to item {}'.format(id_, item_id))
        else:
            item_id = id_
        if item_id:
            if aggressive or kind == 'item':
                parts = parse_item(item_id)
            elif kind == 'part':
                parts = parse_item(item_id, part_id=id_)
            save_item(parts)
            #return route_request(url, aggressive=aggressive)
            return db_find(**{'{}_id'.format(kind): id_})
        else:
            # Catch the small number of records that have no item id
            logger.info('No item id: {}'.format(url))


def db_find(item_id=0, part_id=0, page_id=0):
    """Checks if item/part/page has already been indexed"""
    assert item_id or part_id or page_id
    item_id, part_id, page_id = [int(i) for i in (item_id, part_id, page_id)]
    kwargs = {'item_id': item_id, 'part_id': part_id, 'page_id': page_id}
    kwargstr = str({k: v for k, v in kwargs.items() if v})
    # Check if this item has any snippets associated with it
    rows = db_snippets(item_id, part_id, page_id)
    specimens = {}
    if rows:
        doc_id = None
        for row in rows:
            specimens.setdefault(row.spec_num, []).append(row.snippet)
            if doc_id is None or row.doc_id.startswith('bhl:item'):
                doc_id = row.doc_id
        docinfo = db_docinfo(doc_id)
        logger.info('Returned %s specimens from database for %s', len(specimens), kwargstr)
        return Indexed(specimens, docinfo, True)
    # If no snippets found, check if item has been indexed
    rows = db_parts(item_id, part_id, page_id)
    for row in rows:
        docinfo = db_docinfo(row.id)
        logger.info('Returned doc %s from database for %s', row.id, kwargstr)
        return Indexed(specimens, docinfo, True)
    # No record of this document exists
    logger.info('No data in database for %s', kwargstr)
    return Indexed(None, None, False)


def db_snippets(item_id=0, part_id=0, page_id=0):
    """Returns snippets associated with an item/part/page"""
    query = DB.query(Specimen.spec_num,
                     Snippet.snippet,
                     Snippet.doc_id) \
              .join(Snippet, Snippet.id == Specimen.snippet_id) \
              .join(Part, Part.id == Snippet.doc_id) \
              .filter(
                  or_(Part.item_id == item_id,
                      Part.part_id == part_id,
                      Snippet.page_id == 'bhl:page:{}'.format(page_id))
              )
    kwargs = {'item_id': item_id, 'part_id': part_id, 'page_id': page_id}
    kwargstr = str({k: v for k, v in kwargs.items() if v})
    logger.debug('Query for %s: %s', kwargstr, str(query))
    return query.all()


def db_parts(item_id=0, part_id=0, page_id=0):
    query = DB.query(Part.id) \
              .filter(
                  or_(Part.item_id == item_id,
                      Part.part_id == part_id,
                      and_(Part.min_page <= page_id,
                           Part.max_page >= page_id))
               )
    kwargs = {'item_id': item_id, 'part_id': part_id, 'page_id': page_id}
    kwargstr = str({k: v for k, v in kwargs.items() if v})
    logger.debug('Query for %s: %s', kwargstr, str(query))
    return query.all()


def db_docinfo(doc_id):
    query = DB.query(Document.title,
                     Document.year,
                     Document.journal,
                     Snippet.page_id,
                     Part.item_id,
                     Part.part_id) \
              .join(Part, Part.id == Document.id) \
              .filter(Document.id == doc_id) \
              .order_by(Snippet.page_id) \
              .distinct()
    results = query.all()
    docinfo = {}
    for row in results:
        docinfo['title'] = row.title
        docinfo['journal'] = row.journal
        docinfo['year'] = row.year
        bhl_mask = 'https://www.biodiversitylibrary.org/{}/{}'
        docinfo['url'] = bhl_mask.format('item', row.item_id)
    logger.debug(str(query))
    return docinfo


lookup = populate_url_lookup()


if __name__ == '__main__':
    print(lookup['https://www.biodiversitylibrary.org/part/236613'])

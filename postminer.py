"""Extends and summarizes data mined from GeoDeepDive"""

import re

from documents import get_document
from specimens import get_specimens, filter_records
from database.queries import Query


if __name__ == '__main__':
    db = Query()
    db.max_length = 50
    for doc in db.get_document():
        if doc.title:
            continue
        metadata = get_document(doc.id)
        dois = [n['id'] for n in metadata.get('identifier', []) if n['type'] == 'doi']
        db.add_journal(metadata.get('journal'))
        db.update_document(doc.id, **{
            'doi': dois[0] if dois else None,
            'title': metadata.get('title'),
            'journal': metadata.get('journal'),
            'year': metadata.get('year')
        })
    db.commit()
    for specimen in db.get_specimen():
        if specimen.ezid:
            continue
        doc = db.get_document(specimen.doc_id)
        keywords = set([re.sub('[^a-z]', '', w.lower()) for w
                        in doc.title.split() if len(w) >= 5])
        records = get_specimens(specimen.spec_num)
        ezids = filter_records(records, specimen.spec_num, keywords=keywords)
        if len(ezids) > 1:
            records = get_specimens(specimen.spec_num, dept=doc.topic.strip('?'))
            check_dept = filter_records(records, specimen.spec_num)
            if check_dept or not doc.topic.endswith('?'):
                ezids = check_dept
        if len(ezids) == 1:
            db.update_specimen(specimen.id, ezid=ezids[0])
    db.commit().close()

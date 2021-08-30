"""Defines functions used to mine a JSTOR/Portico export"""
import csv
import glob
import os

from nmnh_ms_tools.records import Reference

from .core import Miner
from ..databases.citations import Document




class JSTORMiner(Miner):
    """Tools to mine a JSTOR/Portico export"""

    def __init__(self, path):
        super().__init__()
        self.doc_path = glob.glob(os.path.join(path, "*documents.csv"))[0]
        self.sent_path = glob.glob(os.path.join(path, "*sentences.csv"))[0]


    def mine(self):
        """Mines specimen numbers downloaded from a JSTOR/Portico export"""
        docs = self.read_docs()

        # Find snippets
        with open(self.sent_path, "r", encoding="utf-8-sig", newline="") as f:

            # Get rid of nulls in the JSTOR file
            rows = csv.reader([l.replace("\x00", "[NUL]") for l in f])
            keys = next(rows)

            # Find unique text/doc/page combinations
            unique = {}
            for row in rows:
                rowdict = dict(zip(keys, row))

                # Read and parse document
                doc = docs[rowdict["id"]]
                if not isinstance(doc, Reference):
                    docs[rowdict["id"]] = Reference(doc, False)
                    doc = docs[rowdict["id"]]
                    self.save_document(doc)
                doc_url = doc.url

                page_id = ""
                if rowdict["page_seq"]:
                    page_id = f'{doc_url}#{rowdict["page_seq"]}'

                unique[(rowdict["text"], doc.url, page_id)] = 1

            # Search for catalog numbers in each unique snippet
            for text, doc_url, page_id in unique:
                self.find_snippets(text,
                                   doc_id=doc_url,
                                   page_id=page_id,
                                   num_chars=10000)

        self.session.commit()


    def read_docs(self):
        """Reads document metadata associated with a JSTOR/Portico export"""
        docs = {}
        with open(self.doc_path, "r", encoding="utf-8-sig", newline="") as f:
            rows = csv.reader(f, dialect="excel")
            keys = next(rows)
            for row in rows:
                rowdict = dict(zip(keys, row))
                docs[rowdict["id"]] = rowdict
        return docs

"""Defines functions used to mine the xDD/GeoDeepDive corpus"""
import csv
import datetime as dt
import json
import logging
import re
import sys

from nmnh_ms_tools.bots import GeoDeepDiveBot
from nmnh_ms_tools.records import Reference

from .core import Miner




logger = logging.getLogger(__name__)

# Increase maximum field size for CSV
max_size = sys.maxsize
while True:
    try:
        csv.field_size_limit(max_size)
        break
    except OverflowError:
        max_size = max_size // 2




class GeoDeepDiveMiner(Miner):
    """Tools for mining specimen numbers from the xDD/GeoDeepDive API"""

    def __init__(self):
        super().__init__()
        self.bot = GeoDeepDiveBot()
        self.source = "xDD"


    def download(self, terms, **kwargs):
        """Downloads snippets from the xDD API"""
        keys = None

        params = {
            "clean": "",
            "fragment_limit": 10000,
            "full_results": "",
            "no_word_stemming": ""
        }
        params.update(kwargs)

        response = self.bot.get_snippets(terms, **params)

        # Initialize CSV file based on initial response
        timestamp = dt.datetime.now().strftime("%Y%m%dT%H%M%S")
        try:
            path = f"xdd_{timestamp}_{response.json['success']['scrollId']}.csv"
        except KeyError:
            path = f"xdd_{timestamp}.csv"

        keys = ["_gddid", "doi", "highlight"]

        with open(path, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.writer(f, dialect="excel")
            writer.writerow(keys)

        rows = []
        count = 0
        while True:

            rows.extend(response)

            # Write rows in batches of 1000
            if len(rows) >= 1000:
                with open(path, "a", encoding="utf-8-sig", newline="") as f:
                    writer = csv.writer(f, dialect="excel")
                    for row in rows:
                        row["highlight"] = self.clean_highlight(row["highlight"])
                        writer.writerow([row.get(k, "") for k in keys])
                count += len(rows)
                print(f"{count:,} rows written to CSV!")

                rows = []


            # Scroll to next page if exists
            scroll_id = response.json.get("success", {}).get("scrollId")
            if scroll_id:
                response = self.bot.get_snippets(scroll_id=scroll_id)
            else:
                break

        if rows:
            with open(path, "a", encoding="utf-8-sig", newline="") as f:
                writer = csv.writer(f, dialect="excel")
                for row in rows:
                    row["highlight"] = self.clean_highlight(row["highlight"])
                    writer.writerow([row.get(k, "") for k in keys])


    def mine(self, path):
        """Mines specimen numbers downloaded from the xDD corpus"""
        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            rows = csv.reader(f, dialect="excel")
            keys = next(rows)
            for row in rows:
                rowdict = dict(zip(keys, row))

                if rowdict["doi"]:
                    doc = Reference(rowdict["doi"])
                else:
                    response = self.bot.get_article(rowdict["_gddid"])
                    doc = Reference(response.json["success"]["data"][0])
                self.save_document(doc)
                for text in json.loads(rowdict["highlight"]):
                    self.find_snippets(re.sub(r"</?em.*?>", "", text),
                                       doc_id=doc.url,
                                       page_id="",
                                       num_chars=10000)
        self.session.commit()


    def clean_highlight(self, highlight):
        """Strips HTML from a highlight"""
        # FIXME: HTML stripping does not work
        #highlight = [re.sub(r"</?em.*>", "", h) for h in highlight]
        return json.dumps(highlight, ensure_ascii=False)

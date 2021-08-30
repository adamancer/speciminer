"""Defines bot to interact with BHL v3 API"""
import logging
import re

from lxml import etree

from nmnh_ms_tools.bots.core import Bot, JSONResponse
from nmnh_ms_tools.config import CONFIG
from nmnh_ms_tools.records import Reference




logger = logging.getLogger(__name__)




class BHLBot(Bot):
    """Defines methods to interact with BHL v3 API"""

    def __init__(self, *args, **kwargs):
        self.api_key = kwargs.pop("api_key", CONFIG.bots.bhl_api_key)
        if not self.api_key:
            raise ValueError("BHL API key required")
        kwargs.setdefault("wrapper", BHLResponse)
        super().__init__(*args, **kwargs)


    def get_item_metadata(self, item_id, **kwargs):
        """Makes a request to the GetItemMetadata endpoint"""
        params = {
            "op": "GetItemMetadata",
            "pages": "true",
            "ocr": "true",
            "parts": "true"
        }
        params["id"] = item_id
        params.update(**kwargs)
        return self._query_bhl(**params)


    def get_part_metadata(self, part_id, **kwargs):
        """Makes a request to the GetPartMetadata endpoint"""
        params = {
            "op": "GetPartMetadata",
            "pages": "true",
            "names": "true"
        }
        params.update(**kwargs)
        params["id"] = part_id
        return self._query_bhl(**params)


    def get_page_metadata(self, page_id, **kwargs):
        """Makes a request to the GetPageMetadata endpoint"""
        params = {
            "op": "GetPageMetadata",
            "ocr": "true",
            "names": "true"
        }
        params["pageid"] = page_id
        params.update(**kwargs)
        return self._query_bhl(**params)


    def get_title_metadata(self, title_id):
        """Makes a request to the GetTitleMetadata endpoint"""
        params = {
            "op": "GetTitleMetadata",
            "id": title_id
        }
        return self._query_bhl(**params)


    def publication_search(self, searchterm, searchtype="F", page=1):
        """Makes a request to the PublicationSearch endpoint"""
        params = {
            "op": "PublicationSearch",
            "searchterm": searchterm,
            "searchtype": searchtype,
            "page": page
        }
        return self._query_bhl(**params)


    def publication_search_advanced(self, **kwargs):
        """Makes a request to the PublicationSearchAdvanced endpoint"""
        params = {
            "op": "PublicationSearchAdvanced"
        }
        # Set operation for fields that support that option
        for key in ("notes", "text", "title"):
            try:
                val, op = self._get_op(kwargs[key])
                kwargs[key] = val
                kwargs.setdefault("{key}op", op)
            except KeyError:
                pass
        params.update(**kwargs)
        return self._query_bhl(**params)


    def get_item(self, item_id):
        """Summarizes information related to an item from multiple endpoints"""

        # FIXME: Importing this at the top creates a circular import
        from nmnh_ms_tools.records import Person, Reference

        # Retrieve basic item metadata
        item = self.get_item_metadata(item_id)[0]

        # Most bibliographic info for items is kept in the title record,
        # so integrate that into the item
        title = self.get_title_metadata(item["TitleID"])[0]
        item["Title"] = title["FullTitle"]
        for key in ["Genre", "PublisherName"]:
            try:
                item.setdefault(key, title[key])
            except KeyError:
                pass

        # Create reference
        ref = Reference(item)
        ref.content = {p["PageID"]: p["OcrText"] for p in item["Pages"]}

        # Extract info from any associated parts
        ref.parts = []
        ref.taxa = []
        for part in item.get("Parts", []):
            part = self.get_part(part["PartID"], pages=ref.content)
            part.publication = ref.title
            part.publication_url = ref.url

            ref.parts.append(part)
            ref.taxa.extend(part.taxa)

        return ref


    def get_part(self, part_id, pages=None):
        """Summarizes information related to an part from multiple endpoints"""

        # FIXME: Importing this at the top creates a circular import
        from nmnh_ms_tools.records import Person, Reference

        # COnvert part to a reference
        part = self.get_part_metadata(part_id)[0]
        ref = Reference(part)

        # Get pages from the item record if not provided
        if pages is None:
            try:
                parent = self.get_item_metadata(part["ItemID"])[0]
                ref.publication_url = parent["ItemUrl"].replace('www.', '', 1)
                pages = {p["PageID"]: p["OcrText"] for p in parent["Pages"]}
            except KeyError:
                # Some indexed publications do not have their full text
                # accessible through the API
                pages = {}

        # A part has no children but this attribute should exist anyway to
        # be constent with get_item
        ref.parts = []

        # Limit pages to those appearing in this part
        page_ids = {p["PageID"] for p in part["Pages"]}
        ref.content = {k: v for k, v in pages.items() if k in page_ids}

        # Get taxonomic names appearing in this part
        taxa = []
        for name in part["Names"]:
            taxa.extend([name["NameFound"], name["NameCanonical"]])
        ref.taxa = sorted(set([t for t in taxa if t]))

        return ref


    def get_publications(self, title):
        """Returns publications matching the given title"""
        pubs = self.publication_search_advanced(title=title, titleop="phrase")
        return [Reference(pub) for pub in pubs]


    def _query_bhl(self, **kwargs):
        """Queries specified BHL v3 webservice"""

        if not self.api_key:
            raise AttributeError("An API key is required")

        params = {
            "apikey": self.api_key,
            "format": "json",
        }
        params.update(**kwargs)
        return self.get("https://www.biodiversitylibrary.org/api3", params=params)


    @staticmethod
    def _get_op(term):
        """Prepares term or list of terms for search"""
        if isinstance(term, (list, tuple)):
            term = " ".join(term)
            return term, "all"
        return term, "phrase"




class BHLResponse(JSONResponse):
    """Defines container for results from a BHL API call"""

    def __init__(self, response, **kwargs):
        kwargs.setdefault("results_path", ["Result"])
        super().__init__(response, **kwargs)

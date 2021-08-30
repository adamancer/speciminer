"""Defines functions used to mine the BHL corpus"""
import logging

from nmnh_ms_tools.records import Reference

from .core import Miner
from ..bots import BHLBot
from ..databases.citations import Document




logger = logging.getLogger(__name__)




class BHLMiner(Miner):
    """Tools for mining specimen numbers from the BHL corpus"""

    def __init__(self):
        super().__init__()
        self.bot = BHLBot()
        self.source = "BHL"


    def mine(self, terms, maxpage=None, **kwargs):
        """Mines specimen numbers from the BHL corpus"""
        page = kwargs.pop("page", 1)
        # Loop until number of publications found falls below expected
        total = 0
        num_records = 200
        while num_records == 200 and (maxpage is None or page <= maxpage):
            logger.info(f"Checking publications on page {page}...")

            # Route to endpoint based on complexity of query
            if isinstance(terms, (list, tuple)) and kwargs:
                records = self.bot.publication_search_advanced(
                    text=terms, page=page, **kwargs
                )
            else:
                records = self.bot.publication_search(terms, page=page)

            # Get number of publications in the results
            num_records = len(records)
            total += num_records
            logger.info(f"Found {num_records} records matching"
                        f" '{terms}' (total={total})")

            # Process each publication based on its URL
            for rec in records:

                # The publication search returns the best metadata
                pub = Reference(rec)

                # Skip document if it's already in the database
                if self.session.query(Document).filter_by(url=pub.url).first():
                    logger.debug(f"{pub.url} already exists")
                    continue

                # Otherwise save document so it won't be re-checked
                pub_id = self.save_document(pub)

                # Resolve record based on type (part or item)
                method = f"get_{rec['BHLType'].lower()}"
                doc_id = rec[f"{rec['BHLType']}ID"]
                try:
                    doc = getattr(self.bot, method)(doc_id)
                except IndexError:
                    # Fails if full text is unavailable (?)
                    logger.warning(f"{rec['BHLType']}ID={doc_id} not found")
                    continue

                # Update the publication record based on the part/item
                if not pub.publication and pub.title != doc.title:
                    pub.publication = doc.title
                if pub.url != doc.publication_url:
                    pub.publication_url = doc.publication_url
                self.save_document(pub)

                # For items with no parts, use the item itself
                parts = doc.parts
                if not parts:
                    parts = [doc]

                # Look for specimen numbers in each part
                for part in parts:
                    if part.url != doc.url:
                        self.save_document(part)
                    for page_num, text in part.content.items():
                        if text:
                            page_url = f"https://biodiversitylibrary.org/page/{page_num}"
                            self.find_snippets(self.clean_text(text),
                                               doc_id=part.url,
                                               page_id=page_url)

                # Force a commit at lower than the nominal limit. This is
                # intended to ensure that bundles of records from one
                # publication are all committed at the same time.
                if len(self.session) >= 1000:
                    self.session.commit()

            page += 1

        # Perform a final commit
        self.session.commit()

        logger.info("Mining completed")

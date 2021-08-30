"""Defines shared methods for mining catalog numbers from a generic corpus"""
import hashlib
import logging
import re

from nmnh_ms_tools.tools.specimen_numbers.parser import Parser

from ..databases.citations import (
    Session, DarwinCore, Document, Journal, Link, Snippet, Specimen
)
from ..utils import SessionWrapper




logger = logging.getLogger(__name__)




class Miner:
    """Tools for mining catalog numbers from a generic corpus"""

    def __init__(self):
        self.bot = None
        self.source = "Unknown"
        self.parser = Parser()
        self.session = SessionWrapper(Session, limit=10000)
        self.session.order = [
            Journal, Document, Specimen, Snippet, Link, DarwinCore
        ]


    def mine(self, terms, maxpage=None, **kwargs):
        """Mines specimen numbers from the specified corpus"""
        raise NotImplementedError


    def clean_text(self, text):
        """Cleans up whitespace in text"""
        return re.sub(r"\s+", " ", text)


    def find_snippets(self, text, doc_id, page_id, **kwargs):
        """Finds, parses, and saves snippets with catalog numbers"""

        kwargs.setdefault("clean", True)

        spec_num_snippets = self.parser.snippets(text, **kwargs)
        for verbatim, snippets in spec_num_snippets.items():
            spec_nums = self.parser.parse(verbatim)
            for snippet in snippets:
                snippet_id = self.save_snippet(snippet, doc_id, page_id)
                for spec_num in spec_nums:
                    self.save_specimen(spec_num, verbatim, snippet_id)

            # Replace verbatim with placeholder of equal length
            text = text.replace(verbatim, " " * len(verbatim))

        # Find candidates missed by the parser
        pattern = r"\b({})\b".format("|".join(self.parser.codes))
        candidate_snippets = self.parser.snippets(
            text, pattern, num_chars=kwargs.get("num_chars", 64)
        )
        for verbatim, snippets in candidate_snippets.items():
            for snippet in snippets:
                self.save_snippet(snippet, doc_id, page_id)


    def save_document(self, document):
        """Saves document to citations database"""
        self.session.add(Document(
            url=document.url,
            publication_url=document.publication_url,
            kind=document.kind,
            authors=document.author_string(),
            title=document.title,
            publication=document.publication,
            year=document.year,
            volume=document.volume,
            number=document.number,
            pages=document.pages,
            doi=document.doi,
        ))
        if document.publication:
            self.save_journal(document.publication)
        return document.url


    def save_journal(self, title):
        """Saves journal to citations database"""
        self.session.add(Journal(title=title.title()))
        return title.title()


    def save_snippet(self, snippet, doc_id, page_id=""):
        snippet_id = hashlib.md5((doc_id + page_id + snippet.text).encode("utf-8")).hexdigest()
        self.session.add(Snippet(
            id=snippet_id,
            doc_url=doc_id,
            page_id=page_id,
            snippet=snippet.text,
        ))
        return snippet_id


    def save_specimen(self, spec_num, verbatim, snippet_id):
        """Saves specimen number to citations database"""
        specimen_id = hashlib.md5((snippet_id + spec_num).encode("utf-8")).hexdigest()
        self.session.add(Specimen(
            id=specimen_id,
            snippet_id=snippet_id,
            spec_num=spec_num,
            verbatim =verbatim,
        ))
        return specimen_id

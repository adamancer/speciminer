"""Finds catalog records for specimen numbers associated with the same person"""
import logging
import re

from nmnh_ms_tools.bots import GeoDeepDiveBot
from nmnh_ms_tools.records import (
    Citation, Citations, Person, People, Reference, References, Specimen, parse_names
)
from nmnh_ms_tools.utils import as_list

from .core import Matcher




logger = logging.getLogger(__name__)




class NameMatcher(Matcher):
    """Finds catalog records for specimen nums associated with same person"""

    def __init__(self, specimens, existing_references=None):
        super().__init__()

        if specimens:
            specimens = [s if isinstance(s, Specimen) else Specimen(s)
                         for s in specimens]
        self.specimens = specimens

        existing_references = as_list(existing_references)
        if existing_references:
            existing_references = References(existing_references)
        self.existing_references = existing_references

        self.references = {}

        self.bot = GeoDeepDiveBot()
        self.bot.install_cache()

        self._names = None
        self._orig_names = None
        self._new_names = None

        self.results = {}
        self.citations = {}


    @property
    def names(self):
        return self._names


    @names.setter
    def names(self, names):
        if isinstance(names, str):
            names = parse_names(names)
        else:
            names = People(names)

        names = names.unique(sort_values=True)

        # Keep list of original names
        if not self._names:
            self._orig_names = names[:]

        if names != self._names:
            self._names = names[:]


    def match(self, names=None, max_iterations=None):
        """Matches specimens to references based on associated people

        Specimens are pulled from the specimen attribute on the current
        instance. If no names are given, uses authors from references in
        the specimen record. Each iteration looks for new coauthors who have
        published on the same sample. The loop stops when no new coauthors
        are found.
        """
        self.names = names if names else self._new_names[:]
        if not self.names:
            raise ValueError("No names provided")

        # Iterate max iterations if set
        if max_iterations:
            max_iterations -= 1

        # Reset job variables populated by match_one
        self.citations = {}
        self.results = {}
        self.references = {}
        self.get_refs_by_names()

        # Match specimens to publications featuring at least one author
        # from the list of names
        self._new_names = People()
        for spec in self.specimens:
            self.match_one(spec)

        self._new_names = self._new_names.unique(sort_values=True)

        # The match ends when no new coauthors are returned or when the
        # maximum number of iterations is exceeded
        if (
            self.names == self._new_names
            or (max_iterations is not None and not max_iterations)
        ):
            # Remove duplicate references from the results container
            for key, bibs in self.results.items():
                self.results[key] = sorted(
                    [b for i, b in enumerate(bibs) if b not in bibs[:i]]
                )
            return self.results

        return self.match(max_iterations=max_iterations)


    def match_one(self, specimen, fields=None):
        """Matches specimen to references based on associated people"""
        if fields is None:
            fields = ['field_number', 'record_number']

        for field in fields:
            # Look for specimen numbers in snippets
            for spec_num in getattr(specimen, field):

                # Do not attempt to match short or simple numbers
                if spec_num.isnumeric() and len(spec_num) < 5:
                    continue

                spec_num_is_simple = not self.is_complex(spec_num)
                if spec_num_is_simple:
                    continue

                # Run a simple search, falling back to specific doc ids if 1000
                # or more snippets are returned
                snippets = self.bot.get_snippets(spec_num)
                if len(snippets) >= 1000:
                    snippets = self.bot.get_snippets(
                        spec_num, docid=self.references.keys()
                    )
                #print(f"Found {len(snippets):,} snippets matching {spec_num}")

                for snippet in snippets:

                    # Get the list of authors from the string provided by GDD
                    author_string = snippet.get("authors")
                    authors = parse_names(author_string) if author_string else []

                    if self.shares_name(authors, spec_num_is_simple):

                        sources = {
                            "snippet": " | ".join(snippet["highlight"]),
                            "title": snippet["title"]
                        }
                        matches = self.match_specimen(specimen, sources)
                        if not matches:
                            matches = self.match_specimen(
                                specimen, sources, spec_num_only=True
                            )

                        if matches:

                            # Use the longest, most specific statement
                            match = sorted(matches.values(), key=len)[-1]
                            match.add("author", "")

                            # Add authors who have worked on related samples
                            ref = Reference(snippet["doi"])
                            self._new_names.extend(ref.authors)

                            # Add citation to lookup, updating it if it already
                            # exists with this spec_num
                            citation = Citation(sources["snippet"], ref)
                            stmt = f"{spec_num}: {match}"

                            try:
                                self.citations[str(citation)].matches.append(stmt)
                            except KeyError:
                                citation.matches.append(stmt)
                                citation.emu_note_mask = (
                                    'This citation mentions the following'
                                    ' specimens:\n{}\n\nCitation found'
                                    ' using the GeoDeepDive API'
                                    ' (https://geodeepdive.org/api/snippets)'
                                )
                                self.citations[str(citation)] = citation

                            # Add the citation if the original reference is
                            # not already linked from the catalog record
                            if ref not in specimen.associated_references:
                                self.results.setdefault(specimen.occurrence_id, []) \
                                            .append(str(citation))
                            else:
                                print(f"{specimen.occurrence_id}: Already links {ref}")


    def shares_name(self, names, orig_names_only=False):
        """Checks if a collaborator is present"""
        ref_names = self._orig_names[:] if orig_names_only else self.names[:]
        for name in names:
            for ref_name in ref_names:
                if name.similar_to(ref_name):
                    return True
        return False


    def get_refs_by_names(self):
        """Finds references with authors matching each name"""
        print("----")
        for name in self._names:
            print(f"Finding refs by {name}...")
            for article in self.bot.get_articles(lastname=name.last):
                for name_ in article["author"]:
                    if (
                        name.last.lower() in name_["name"].lower()
                        and Person(name_["name"]).similar_to(name)
                    ):
                        doc_id = article["_gddid"]
                        self.references.setdefault(doc_id, []).append(name)
        return self.references


    def is_complex(self, spec_num):
        """Tests if specimen number is complex"""
        # Strip non-alphanumeric characters
        spec_num = re.sub(r'[^A-z0-9]', '', spec_num, flags=re.I)

        has_num = re.search(r'\d', spec_num)
        has_alpha = re.search(r'[A-z]', spec_num, flags=re.I)
        has_long_alpha = re.search(r'[A-z]{3}', spec_num, flags=re.I)
        has_long_num = re.search(r'\d{5}', spec_num)

        return (
            bool(has_long_alpha and has_num)
            or bool(has_long_num and has_alpha)
            or len(spec_num) >= 6 and not spec_num.isnumeric()
        )


    def report(self):
        """Compiles a list of citations keyed to specimen"""
        return {k: [self.citations[v] for v in v]
                for k, v in self.results.items()}

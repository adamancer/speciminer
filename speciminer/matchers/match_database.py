"""Finds catalog records matching records from the citations database"""
import csv
import hashlib
import logging
import re

import pandas as pd

from nmnh_ms_tools.records import (
    CatNum, CatNums, Citation, People, Reference, Specimen, get_author_and_year
)
from nmnh_ms_tools.utils import as_list

from .core import Matcher
from ..databases.citations import Session, DarwinCore, Link
from ..utils import SessionWrapper




logger = logging.getLogger(__name__)




MAX_DIFF = 1000
DEBUG_DOC_URL = None




class DatabaseMatcher(Matcher):
    """Finds catalog records matching records from the citations database"""

    def __init__(self):
        super().__init__()
        self.session = SessionWrapper(Session, limit=100)


    def match(self):
        """Matches specimen numbers in database to catalog records"""
        session = self.session

        documents = pd.read_sql_table("documents", con=self.session.bind, index_col="url")
        snippets = pd.read_sql_table("snippets", con=self.session.bind, index_col="id")
        specimens = pd.read_sql_table("specimens", con=self.session.bind, index_col="id")

        # Add doc_url column to specimens
        specimens = specimens.join(snippets, on="snippet_id") \
                             .join(documents, on="doc_url")
        specimens.sort_values(["doc_url", "spec_num"], inplace=True)

        print(specimens.iloc[0])

        for (doc_url, spec_num), rows in specimens.groupby([specimens.doc_url,
                                                            specimens.spec_num]):

            if DEBUG_DOC_URL and doc_url != DEBUG_DOC_URL:
                continue

            if len(spec_num) > 9 and spec_num[:4] in {"NMNH", "USNM"}:

                try:
                    spec_nums = CatNums([spec_num])
                except ValueError as exc_info:
                    # Skip catalog numbers that can't be parsed using the basic
                    # parser. NOTE: These mostly appear to be type numbers.
                    logger.warning(str(exc_info), exc_info=exc_info)
                    continue

                # Try matching on context from different sources
                row = rows.iloc[0]
                sources = {
                    "snippet": " | ".join([r.snippet for _, r in rows.iterrows()]),
                    "title": row.title
                }
                matches = self.match_specimen(
                    spec_num, sources, spec_nums=spec_nums
                )

                # If only a simple alpha suffix is given, try matching without
                # the suffix. This catches a common case where the suffix
                # that appears in the literature does not appear in the
                # collections database.
                trimmed = re.sub(r"[a-zA-Z]$", "", spec_num)
                if not matches and trimmed != spec_num:
                    matches = self.match_specimen(
                        trimmed, sources, spec_nums=spec_nums
                    )

                matches = self.best_matches(matches)
                if matches:
                    try:
                        self.save_link(matches, row)
                    except ValueError:
                        pass
                else:
                    self.save_miss(row.spec_num, row.doc_url)

        self.session.commit()
        self.session.close()


    def match_from_snippets(self):
        """Matches records using specimens that occur in the same snippets"""

        documents = pd.read_sql_table("documents", con=self.session.bind, index_col="url")
        links = pd.read_sql_table("links", con=self.session.bind, index_col="id")

        links = links.join(
            documents, on="doc_url", lsuffix="links", rsuffix="docs"
        )

        specimens = pd.read_sql_table("specimens", con=self.session.bind, index_col="id")
        snippets = pd.read_sql_table("snippets", con=self.session.bind, index_col="id")
        specimens = specimens.join(snippets, on="snippet_id")

        for doc_url, rows in links.groupby(links.doc_url):

            if DEBUG_DOC_URL and doc_url != DEBUG_DOC_URL:
                continue

            matched, missed = self._get_matched_and_missed(rows)

            # Analyze matched rows to make additional matches
            if matched and missed:

                # Create lookup of matches using verbatim (not parsed) numbers
                lookup = {}
                for row in matched:
                    row.spec_num = row.spec_num.verbatim
                    lookup[row.spec_num] = row

                # Get list of snippets from the document that feature specimens
                snip_to_spec = {}
                spec_to_snip = {}
                for _, row in specimens[specimens.doc_url == doc_url].iterrows():
                    snip_to_spec.setdefault(row.snippet_id, []) \
                                .append(row.spec_num)
                    spec_to_snip.setdefault(row.spec_num, []) \
                                .append(row.snippet_id)

                # Map each snippet to a single department where possible
                snip_to_dept = {}
                for snippet_id, spec_nums in snip_to_spec.items():
                    depts = []
                    for spec_num in spec_nums:
                        try:
                            depts.append(lookup[spec_num].department)
                        except KeyError:
                            pass
                    if len(set(depts)) == 1:
                        snip_to_dept[snippet_id] = depts[0]
                    else:
                        # FIXME: Reclassify to missed if multiple depts?
                        pass

                # Check for inconsistencies in specimens from the same snippets
                # TKTK

                for row in missed:

                    # Find all snippets mentioning this specimen number
                    snippet_ids = spec_to_snip[row.spec_num.verbatim]

                    # Check that all matching snippets have the same department
                    depts = []
                    for snippet_id in snippet_ids:
                        try:
                            depts.append(snip_to_dept[snippet_id])
                        except KeyError:
                            pass
                    if len(set(depts)) != 1:
                        continue

                    # Find matched specimens from the snippets
                    related = []
                    for snippet_id in snippet_ids:
                        for spec_num in snip_to_spec[snippet_id]:
                            try:
                                related.append(lookup[spec_num])
                            except KeyError:
                                pass

                    # Serialize metadata from related and match
                    summarized = self.summarize_specimens(related)
                    related = []
                    for vals in summarized.values():
                        related.extend(vals)
                    sources = {"associated specimens": "|".join(related)}

                    matches = self.match_specimen(
                        row.spec_num,
                        sources,
                        dept=depts[0],
                        spec_nums=CatNums([row.spec_num])
                    )

                    matches = self.best_matches(matches)

                    # Related specimens give a lot of data to match,
                    # some of which (like country) is pretty generic,
                    # so bump up the threshold to prevent thin matches.
                    matches = [m for m in matches if m.score > 2]

                    print(f"{row.spec_num}: {depts[0]}")
                    print(related)
                    print(matches)
                    print("-------")

                    if matches:
                        try:
                            self.save_link(matches, row)
                        except ValueError:
                            print(f"{row.spec_num}: No match")
                else:
                    print(f"{row.spec_num}: No match")

        self.session.commit()
        self.session.close()


    def match_from_ranges(self):
        """Matches records using catalog number ranges from the same document"""

        documents = pd.read_sql_table("documents", con=self.session.bind, index_col="url")
        links = pd.read_sql_table("links", con=self.session.bind, index_col="id")

        links = links.join(
            documents, on="doc_url", lsuffix="links", rsuffix="docs"
        )

        for doc_url, rows in links.groupby(links.doc_url):

            if DEBUG_DOC_URL and doc_url != DEBUG_DOC_URL:
                continue

            matched, missed = self._get_matched_and_missed(rows)

            # Analyze matched rows to make additional matches
            if matched and missed:

                # Sort by catalog number
                matched.sort(key=lambda row: row.spec_num.prefixed_num)
                ranges = {}

                # If there are a bunch of matches all from one department,
                # use that department to match the rest as well. This is the
                # most common case but is potentially problematic for
                # sources like BHL that do not always separate issues into
                # individual articles.
                depts = [m.department for m in matched]
                if len(matched) > len(missed) and len(set(depts)) == 1:
                    nums = sorted([m.spec_num.prefixed_num for m in matched])
                    ranges[tuple(nums)] = depts[0]

                else:
                    # Calculate ranges of similar numbers from one department.
                    # The ranges will be used to (1) check for matching errors
                    # and (2) use blocks of catalog numbers to try to match
                    # the misses.
                    all_matched_nums = []
                    nums = []
                    last = None
                    for row in matched:
                        num = row.spec_num.prefixed_num
                        if (
                            row.department == last
                            and (not nums or num - nums[-1] <= MAX_DIFF)
                        ):
                            nums.append(num)
                        else:
                            ranges[tuple(nums)] = last
                            nums = [num]
                            last = row.department

                        all_matched_nums.append(num)
                    else:
                        ranges[tuple(nums)] = row.department

                    # Get the bounds of matched
                    min_matched = min(all_matched_nums)
                    max_matched = max(all_matched_nums)

                    # Remove the empty range
                    del ranges[()]

                    # Identify bad matches. Questionable matches consist of a
                    # single number bound on both sides by multiple matches
                    # on the same department.
                    keys = list(ranges.keys())
                    for i in range(len(keys)):

                        nums = keys[i]
                        dept = ranges[nums]

                        if len(nums) == 1:

                            # Get the department for the prior set of numbers
                            prev_dept = None
                            try:
                                prev_nums = list(keys[i - 1])
                            except IndexError:
                                prev_nums = []
                            finally:
                                if (
                                    len(prev_nums) > 1
                                    and nums[0] - max(prev_nums) <= MAX_DIFF
                                ):
                                    prev_dept = ranges[tuple(prev_nums)]

                            # Get the department for the next set of numbers
                            next_dept = None
                            try:
                                next_nums = list(keys[i + 1])
                            except IndexError:
                                next_nums = []
                            finally:
                                if (
                                    len(next_nums) > 1
                                    and min(next_nums) - nums[0] <= MAX_DIFF
                                ):
                                    next_dept = ranges[tuple(next_nums)]

                            # Reclassify as a miss if bounded on both sides
                            # by the same department
                            if (
                                prev_dept
                                and prev_dept == next_dept
                                and dept != prev_dept
                            ):
                                missed.append(row)
                                del ranges[nums]

                    # Retry missed rows using context from matched specimens
                    metadata = {}
                    keys = list(ranges.keys())
                    for row in missed:

                        # If catalog numbers fall outside the range of
                        # matched numbers, evaluate them based on the
                        # metadata of their nearest neighbors only. Allow
                        # match on department only if difference is small.
                        if row.spec_num.prefixed_num < min_matched:
                            nums = keys[0]
                            diff =  min_matched - row.spec_num.prefixed_num
                            dept = ranges[nums] if diff <= MAX_DIFF else None

                        elif row.spec_num.prefixed_num > max_matched:
                            nums = keys[-1]
                            diff =  row.spec_num.prefixed_num - max_matched
                            dept = ranges[nums] if diff <= MAX_DIFF else None

                        # In-range matches consider neighbor metadata as well
                        # but can also match on department only if matches
                        # on either side are from the same department.
                        else:
                            for i, nums in enumerate(keys):

                                dept = ranges[nums]

                                # Add numbers from previous group if not first.
                                if i:
                                    prev_nums = keys[i - 1]
                                    # Disregard dept if neighbors differ
                                    if ranges[nums] != dept:
                                        dept = [dept, ranges[num]]
                                    nums = prev_nums[:] + nums[:]

                                if min(nums) <= row.spec_num.prefixed_num <= max(nums):
                                    break
                            else:
                                nums = []
                                dept = None

                        if nums:

                            # Serialize metadata from related and match
                            related = [row for row in matched
                                       if row.spec_num.prefixed_num in nums]
                            summarized = self.summarize_specimens(related)
                            related = []
                            for vals in summarized.values():
                                related.extend(vals)
                            sources = {"adjacent specimens": "|".join(related)}

                            matches = self.match_specimen(
                                row.spec_num,
                                sources,
                                dept=dept,
                                spec_nums=CatNums([row.spec_num])
                            )

                            matches = self.best_matches(matches)

                            # Related specimens give a lot of data to match,
                            # some of which (like country) is pretty generic,
                            # so bump up the threshold to prevent thin matches.
                            matches = [m for m in matches if m.score > 2]

                            print(f"{row.spec_num.prefixed_num}: {dept} ({min(nums)}-{max(nums)})")
                            print(related)
                            print(matches)
                            print("-------")

                            if matches:
                                try:
                                    self.save_link(matches, row)
                                except ValueError:
                                    print(f"{row.spec_num.prefixed_num}: No match")
                        else:
                            print(f"{row.spec_num.prefixed_num}: No match")

        self.session.commit()
        self.session.close()


    def summarize_specimens(self, rows):
        """Summarizes key metadata from list of records


        Args:
            nums (list)
            matched (list): list of records
        """
        attrs = [
            "country",
            "state_province",
            "county",
            "order",
            "family",
            "genus",
            "group",
            "formation",
            "member",
            "earliest_period_or_lowest_system",
            "earliest_epoch_or_lowest_series",
            "earliest_age_or_lowest_stage",
            "latest_period_or_highest_system",
            "latest_epoch_or_highest_series",
            "latest_age_or_highest_stage",
        ]
        metadata = {}
        for row in rows:
            # Reduce the number of queries by searching for specimen
            # number instead of EZID. This leverages the cache if enabled.
            for rec in self.portal.get_specimen_by_id(row.spec_num):
                if rec["occurrenceID"] in row.ezid:
                    spec = Specimen(rec)
                    for attr in attrs:
                        val = as_list(getattr(spec, attr))
                        if val:
                            metadata.setdefault(attr, []).extend(val)
        return {k: set(v) for k, v in metadata.items()}



    def match_and_save(self, spec_num, rows, dept=None):
        """Finds best match for a specimen number in a list of rows"""

        if len(spec_num) > 9 and spec_num[:4] in {"NMNH", "USNM"}:
            row = rows.iloc[0]
            sources = {
                "snippet": "|".join([r.snippet for _, r in rows.iterrows()]),
                "title": rows.iloc[0].title
            }

            matches = self.match_specimen(spec_num, sources, dept=dept)
            matches = self.best_matches(matches)
            if matches:
                try:
                    self.save_link(matches, row)
                except ValueError:
                    pass
            else:
                self.save_miss(row.spec_num, row.doc_url)


    def save_link(self, matches, row):
        """Saves matches for a given row"""

        # Stringify specimen number if necessary
        spec_num = row.spec_num
        if isinstance(spec_num, CatNum):
            spec_num = row.spec_num.verbatim

        # Compute link_id
        link_id = hashlib.md5((row.doc_url + spec_num).encode("utf-8")).hexdigest()

        # Verify matches and use the most specific possible statement
        matches = self.best_matches(matches)
        stmt = str(sorted(matches, key=len)[-1])
        for match in matches:
            self.save_dwc(match.record)

        # Add an asterisk to signal that the department was forced
        dept = matches[0].record.collection_code
        if "collection code" in stmt:
            dept += "*"

        # Check if matching specimens list similar references
        has_similar_ref = False
        if row.authors:
            authors = People(row.authors)
            for match in matches:
                for ref in match.record.associated_references:
                    try:
                        author, year = get_author_and_year(ref)
                        print(f"({author}, {year}) => ({authors}, {row.year})")
                        if authors[0].similar_to(author) and year == row.year:
                            has_similar_ref = True
                            break
                    except (IndexError, ValueError) as e:
                        print(str(e))

        self.session.add(Link(
            id=link_id,
            verbatim="na",
            spec_num=spec_num,
            doc_url=row.doc_url,
            ezid=" | ".join(sorted([m.record.occurrence_id for m in matches])),
            department=dept,
            match_quality=stmt,
            has_similar_ref=has_similar_ref
        ))


    def save_dwc(self, specimen):
        """Saves DarwinCore metadata for a given row"""
        self.session.add(DarwinCore(
            id=specimen.occurrence_id,
            higher_classification=" | ".join(specimen.higher_classification),
            scientific_name = " | ".join(specimen.scientific_name),
            type_status=specimen.type_status,
            higher_geography=" | ".join(specimen.higher_geography),
            verbatim_locality = specimen.verbatim_locality,
        ))


    def save_miss(self, spec_num, doc_url):
        """Saves miss for a given row if match failed"""
        link_id = hashlib.md5((doc_url + spec_num).encode("utf-8")).hexdigest()
        self.session.add(Link(
            id=link_id,
            verbatim="na",
            spec_num=spec_num,
            doc_url=doc_url,
            match_quality="MISS",
        ))


    def to_csv(self, path):
        """Exports snippets and matches to a CSV"""

        documents = pd.read_sql_table("documents", con=self.session.bind, index_col="url")
        snippets = pd.read_sql_table("snippets", con=self.session.bind, index_col="id")
        specimens = pd.read_sql_table("specimens", con=self.session.bind, index_col="id")

        # Ditch the index column so the id field is accessible for querying
        dwc = pd.read_sql_table("dwc", con=self.session.bind)

        links = pd.read_sql_table("links", con=self.session.bind, index_col="id")

        # Add doc_url column to specimens
        specimens = specimens.join(snippets, on="snippet_id") \
                             .join(documents, on="doc_url")

        # Limit to USNM specimen numbers
        specimens = specimens[specimens.spec_num.str.startswith(("NMNH", "USNM"))]

        output = []
        for (spec_num, doc_url), rows in specimens.groupby([specimens.spec_num, specimens.doc_url]):
            try:
                link, ref, snippets = self._get_links(
                    rows, links, spec_num, doc_url
                )
            except ValueError:
                pass
            else:
                pages = sorted(set(snippets.values()))
                texts = [f'"{s}"' for s in snippets.keys()]

                link = link.copy()
                link["document"] = str(ref)
                link["page_urls"] = "\n".join(pages)
                link["snippets"] = "\n".join(texts)

                if link.ezid:
                    ezids = [s.strip() for s in link.ezid.split("|")]
                    for _, row in dwc[dwc.id.isin(ezids)].iterrows():
                        link_with_dwc = link.copy()
                        for key, val in row.items():
                            link_with_dwc[key] = val
                        link_with_dwc["ezid"] = link_with_dwc.id
                        output.append(link_with_dwc.to_dict())
                        if not len(output) % 100:
                            print(f"{len(output):,} links processed")
                else:
                    link.match_quality = None
                    output.append(link.to_dict())
                    if not len(output) % 100:
                        print(f"{len(output):,} links processed")

        # Ordered list of columns to output
        cols = [
            "document",
            "doc_url",
            "page_urls",
            "ezid",
            "spec_num",
            "department",
            "snippets",
            "higher_classification",
            "scientific_name",
            "type_status",
            "higher_geography",
            "verbatim_locality",
            "match_quality",
            "has_similar_ref",
        ]
        with open(path, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.writer(f, dialect="excel")
            writer.writerow(cols)
            for row in output:
                writer.writerow([row.get(col, "") for col in cols])


    def report(self, source):
        """Compiles a list of citations keyed to specimen"""
        documents = pd.read_sql_table("documents", con=self.session.bind, index_col="url")
        snippets = pd.read_sql_table("snippets", con=self.session.bind, index_col="id")
        specimens = pd.read_sql_table("specimens", con=self.session.bind, index_col="id")
        links = pd.read_sql_table("links", con=self.session.bind, index_col="id")

        # FIXME: Cannot reproduce the two-part join with links in pandas
        #   SELECT documents.url, documents.title, specimens.spec_num, links.ezid, links.department, snippets.snippet
        #   FROM specimens
        #   JOIN snippets ON specimens.snippet_id = snippets.id
        #   JOIN links ON links.spec_num = specimens.spec_num AND links.doc_url = snippets.doc_url
        #   JOIN documents ON links.doc_url = documents.url

        # Add doc_url column to specimens
        specimens = specimens.join(snippets, on="snippet_id") \
                             .join(documents, on="doc_url")

        # Items are problematic so limit to parts
        specimens = specimens[
            specimens.doc_url.str.startswith("https://biodiversitylibrary.org/part/")
            & specimens.spec_num.str.startswith(("NMNH", "USNM"))
        ]

        citations = {}
        results = {}
        for (spec_num, doc_url), rows in specimens.groupby([specimens.spec_num, specimens.doc_url]):
            try:
                link, ref, snippets = self._get_links(
                    rows, links, spec_num, doc_url
                )
                if not link.ezid:
                    raise ValueError
            except ValueError:
                pass
            else:
                # Add citation to lookup, updating it if it already
                # exists with this spec_num
                citation = Citation( " | ".join(snippets.values()), ref)
                stmt = f"{link.spec_num}: {link.match_quality}"

                try:
                    citations[str(citation)].matches.append(stmt)
                except KeyError:
                    citation.matches.append(stmt)
                    citation.emu_note_mask = (
                        'This citation mentions the following'
                        ' specimens:\n{{}}\n\nCitation found'
                        ' using {}'.format(source)
                    )
                    citations[str(citation)] = citation

                results.setdefault(link.ezid, []).append(str(citation))
                if len(results) > 100:
                    break

        return {k: [citations[v] for v in v] for k, v in results.items()}


    def _get_links(self, rows, links, spec_num, doc_url):
        """Finds links matching a given specimen number and document"""
        try:
            link = links[(links.spec_num == spec_num)
                         & (links.doc_url == doc_url)].iloc[0]
        except IndexError:
            raise ValueError("No linked record found")
        else:
            snippets = {}
            for _, row in rows.iterrows():
                # Create the reference from metadata in the first row
                if not snippets:
                    ref = Reference({
                        "kind": row.kind,
                        "authors": row.authors,
                        "year": row.year,
                        "title": row.title,
                        "publication": row.publication,
                        "volume": row.volume,
                        "number": row.number,
                        "pages": row.pages,
                        "doi": row.doi,
                        "url": row.doc_url
                    })
                snippets[row.snippet] = row.page_id
        return link, ref, snippets


    def _get_matched_and_missed(self, rows):
        """Splits list of rows into matched and missed"""
        # Split rows into matched and missed
        matched = []
        missed = []
        for _, row in rows.iterrows():
            try:
                row.spec_num = CatNum(row.spec_num)
            except ValueError:
                pass
            else:
                if row.ezid:
                    matched.append(row)
                else:
                    missed.append(row)
        return matched, missed

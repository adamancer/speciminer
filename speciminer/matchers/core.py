"""Finds catalog records matching a given specimen number and snippet"""
import logging

from nmnh_ms_tools.bots import GeoGalleryBot
from nmnh_ms_tools.records import CatNum, Specimen




logger = logging.getLogger(__name__)




class Matcher:
    portal = GeoGalleryBot()

    def __init__(self, *args, **kwargs):
        pass


    def match_specimen(self, specimen, sources=None, dept=None, **kwargs):
        """Matches a specimen number to a catalog record"""

        if not sources and not dept:
            raise ValueError("sources and dept both empty")
        if not sources:
            return {}

        # NOTE: The empty string was intended to allow department only matches.
        # I'm not sure that's such a great idea, so hashed it out for now.
        #if dept:
        #    sources[""] = ""

        # Use the portal record even if a Specimen object is provided
        if not isinstance(specimen, (str, CatNum)):
            specimen = specimen.occurrence_id
        records = self.portal.get_specimen_by_id(specimen)

        # Compare each record to each source
        matches = {}
        for rec in records:
            if isinstance(rec, Specimen):
                spec = rec
            else:
                try:
                    spec = Specimen(rec)
                except Exception as exc_info:
                    logger.error("Could not create Specimen", exc_info=exc_info)
                    continue

            match = spec.match_texts(sources, dept=dept, **kwargs)
            if match:
                matches[spec.occurrence_id] = match

        high_score = max(matches.values()) if matches else None
        return {k: v for k, v in matches.items() if v == high_score}


    def best_matches(self, matches):
        """Selects highest quality matches from a list"""
        if isinstance(matches, dict):
            matches = list(matches.values())

        # Only save matches if they are specific to a single department
        if len({m.record.collection_code for m in matches}) != 1:
            return []
        # Use the longest, most specific statement
        return sorted(matches, key=lambda m: len(str(m)))

"""Defines functions for reading/writing data to database"""
import logging

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.exc import FlushError
from sqlalchemy.orm.util import identity_key

from nmnh_ms_tools.bots import GeoGalleryBot
from nmnh_ms_tools.records import Specimen
from nmnh_ms_tools.tools.specimen_numbers.link import MatchMaker




logger = logging.getLogger(__name__)




class SessionWrapper:
    """Wraps sqlalchemy session to prevent IntegrityErrors"""

    def __init__(self, sessionmaker, limit=1):
        self._sessionmaker = sessionmaker
        self._session = None
        self._records = []
        self.limit = limit
        self.order = []


    def __len__(self):
        return len(self._records)


    @property
    def session(self):
        if self._session is None:
            self._session = self._sessionmaker()
        return self._session


    @session.setter
    def session(self, val):
        self._session = val


    def __getattr__(self, attr):
        return getattr(self.session, attr)


    def add(self, rec):
        """Adds record to current session"""
        return self.add_all([rec])


    def add_all(self, recs):
        """Adds records to current session, committing if more than limit"""
        self._records.extend(recs)
        if len(self) >= self.limit:
            self.commit()


    def commit(self):
        """Safely commits records from the current session"""
        if self._records:
            logger.info(f"Committing {len(self):,} records")
            records = []
            for group in self._order_records():
                records.extend(self._remove_duplicates(group))
            try:
                self.session.add_all(records)
                self.session.commit()
            except (FlushError, IntegrityError):
                self.session.rollback()
                try:
                    for rec in records:
                        self.session.merge(rec)
                    self.session.commit()
                except (FlushError, IntegrityError):
                    self.session.rollback()
                    for rec in records:
                        try:
                            self.session.merge(rec)
                            self.session.commit()
                        except (FlushError, IntegrityError) as exc_info:
                            self.session.rollback()
                            logger.error(f"Failed to commit {group}: {exc_info}")
            self._records = []


    def close(self):
        """Closes the current session"""
        self.session.close()
        self.session = None


    @staticmethod
    def _remove_duplicates(records):
        """Removes duplicates from a list of records to prevent flush error"""
        group = {identity_key(instance=rec)[1]: rec for rec in records}
        return list(group.values())


    def _order_records(self):
        """Orders a list of records prior to commit"""
        if self.order:
            ordered = {c.__name__: [] for c in self.order}
            for rec in self._records:
                ordered[rec.__class__.__name__].append(rec)
            return list([o for o in ordered.values() if o])
        return [self._records] if self._records else []

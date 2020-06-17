import logging


class OccurrenceEngine:
    def __init__(self, source):
        self.logger = logging.getLogger(__name__)
        self.data_source = source

    def has_occurrences(self, taxid, geometry=None, country=None):
        result = self.data_source.has_occurrences(
            taxid, geometry=geometry, country=country
        )
        return result

    def get_occurrences(self, taxid, ranks=None):
        results = self.data_source.get_occurrences(taxid, ranks=ranks)
        return results

    def get_occurrences_in_zone(self, taxid, geometry, ranks=None):
        results = self.data_source.get_occurrences(
            taxid, geometry=geometry, ranks=ranks
        )
        return results

    def occurs_in(self, taxid, geometry):
        return (
            len(self.data_source.get_occurrences(taxid, geometry=geometry, limit=1)) > 0
        )

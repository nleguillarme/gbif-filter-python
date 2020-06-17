from util.data_source import OccurrenceSource
from util.taxid import TaxId
from pygbif import occurrences
from time import sleep
import logging
import pygbif


class GbifAPI(OccurrenceSource):
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        OccurrenceSource.__init__(self)
        pygbif.caching(True)

    def get_occurrences(self, taxid, geometry=None, ranks=None, limit=300):
        occ = None
        if self.is_valid_taxid(taxid):
            self.logger.debug("Get occurrences for taxon {}".format(taxid))
            occ = self.get_all_occurrences(taxid, geometry=geometry, limit=300)
        else:
            raise ValueError("Invalid taxid ", taxid)
        if occ:
            return self.format_results(occ, ranks)
        return occ

    def get_all_occurrences(self, taxid, geometry=None, limit=300):
        # TODO : manage limit and offset
        occ = occurrences.search(
            taxonKey=taxid.get_id(), geometry=geometry, limit=limit
        )
        total = occ["count"]
        counter = len(occ["results"])
        while counter < total:
            tmp = occurrences.search(
                taxonKey=taxid.get_id(), geometry=geometry, limit=limit, offset=counter
            )
            occ["results"] += tmp["results"]
            counter += len(tmp["results"])
            self.logger.debug("{}/{}".format(counter, total))
        return occ

    def has_occurrences(self, taxid, geometry=None, country=None):
        occ = occurrences.search(
            taxonKey=taxid.get_id(), geometry=geometry, country=country, limit=1
        )
        self.logger.debug(
            "Ask for {} occurrences(s), got {}".format(1, len(occ["results"]))
        )
        return len(occ["results"]) > 0

    def is_valid_taxid(self, taxid):
        return taxid.get_prefix() == "GBIF:"

    def format_results(self, results, ranks=None):
        formatted_results = []
        count = results["count"]
        self.logger.debug(
            "Got {}/{} occurrences".format(len(results["results"]), count)
        )
        for result in results["results"]:
            taxid = TaxId("GBIF:", result["taxonKey"]).get_taxid()
            if ranks == None:
                formatted_results.append(taxid)
            elif result["taxonRank"] in ranks:
                formatted_results.append(taxid)
        self.logger.debug(
            "Return {} occurrences at {} level".format(len(formatted_results), ranks)
        )
        return formatted_results

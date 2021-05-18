import logging
import pygbif
from pygbif import occurrences, species


class GbifHelper:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        pygbif.caching(True)

    def get_valid_taxid(self, name=None, taxid=None, rank=None, kingdom=None):
        if taxid:  # TODO : Find a way to validate taxid
            return taxid, rank
        self.logger.info(
            f"Look for id of taxon {name} with rank {rank} in GBIF Backbone Taxonomy"
        )
        match = species.name_backbone(
            name=name, rank=rank, kingdom=kingdom, strict=True, verbose=False
        )
        if match["matchType"] == "EXACT":
            if match["synonym"]:
                taxid = match["acceptedUsageKey"]
            else:
                taxid = match["usageKey"]
            rank = match["rank"]
            self.logger.info(
                "Found exact match for taxon {} with id {}".format(name, taxid)
            )
            return taxid, rank
        else:
            self.logger.error("No match for taxon {} : {}".format(name, match))
            return None, rank

    def get_children(self, parent_taxid, children_rank, habitat=None):
        children = species.name_lookup(
            higherTaxonKey=parent_taxid,
            type="occurrence",
            datasetKey="d7dddbf4-2cf0-4f39-9b2a-bb099caae36c",  # Look in GBIF Backbone only
            rank=children_rank.upper(),
            habitat=habitat,
            limit=1000,
        )
        results = []
        if len(children["results"]) == 1000:
            logger.error(
                f"Number of results for {taxid} exceed the limit of 1000 records. Results may be incomplete."
            )
        for child in children["results"]:
            if child["taxonomicStatus"] == "ACCEPTED":
                results.append(child)
        return results

    def apply_spatial_filter(self, taxa_list, geometry=None, country=None):
        keep = []
        for taxon in taxa_list:
            if self.has_occurrences(taxon["key"], geometry, country):
                keep.append(taxon)
        return keep

    def get_occurrences(self, taxid, geometry=None, ranks=None, limit=300):
        occ = None
        if taxid:
            self.logger.debug("Get occurrences for taxon {}".format(taxid))
            occ = self.get_all_occurrences(taxid, geometry=geometry, limit=300)
        else:
            raise ValueError("Invalid taxid ", taxid)
        if occ:
            return self.format_results(occ, ranks)
        return occ

    # def get_all_occurrences(self, taxid, geometry=None, limit=300):
    #     # TODO : manage limit and offset
    #     occ = occurrences.search(taxonKey=taxid, geometry=geometry, limit=limit)
    #     total = occ["count"]
    #     counter = len(occ["results"])
    #     while counter < total:
    #         tmp = occurrences.search(
    #             taxonKey=taxid, geometry=geometry, limit=limit, offset=counter
    #         )
    #         occ["results"] += tmp["results"]
    #         counter += len(tmp["results"])
    #         self.logger.debug("{}/{}".format(counter, total))
    #     return occ

    def has_occurrences(self, taxid, geometry=None, country=None):
        occ = occurrences.search(
            taxonKey=taxid, geometry=geometry, country=country, limit=1
        )
        self.logger.debug(
            "Ask for {} occurrences(s), got {}".format(1, len(occ["results"]))
        )
        return len(occ["results"]) > 0

    def format_results(self, results, ranks=None):
        formatted_results = []
        count = results["count"]
        self.logger.debug(
            "Got {}/{} occurrences".format(len(results["results"]), count)
        )
        for result in results["results"]:
            taxid = result["taxonKey"]
            if ranks == None:
                formatted_results.append(taxid)
            elif result["taxonRank"] in ranks:
                formatted_results.append(taxid)
        self.logger.debug(
            "Return {} occurrences at {} level".format(len(formatted_results), ranks)
        )
        return formatted_results

import argparse
import os
import yaml
import logging
import logging.config
import json
import sys
import pandas as pd
import pygbif

from util.taxid import TaxId
from util.gbif import GbifAPI
from util.occurrence_engine import OccurrenceEngine


def setup_logging(
    default_path="./logging.json", default_level=logging.INFO, env_key="LOG_CFG"
):
    """ Setup logging configuration
    """
    path = default_path
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, "rt") as f:
            config = json.load(f)
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)


def parse_conf_file(path):
    if os.path.exists(path):
        with open(path, "r") as ymlfile:
            cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)
            return cfg
    return None


def get_taxid(name, rank, kingdom, db_prefix, logger):
    logger.info(
        f"Look for id of taxon {name} with rank {rank} in GBIF Backbone Taxonomy"
    )

    match = pygbif.species.name_backbone(
        name=name, rank=rank, kingdom=kingdom, strict=True, verbose=False
    )
    if match["matchType"] == "EXACT":
        if match["synonym"]:
            id = match["acceptedUsageKey"]
            logger.info(f"Taxon {name} is synonym. Found accepted taxid {id}")
        else:
            id = match["usageKey"]
        taxid = TaxId(db_prefix, id)
        logger.info("Found exact match for taxon {} with id {}".format(name, taxid))
        return taxid
    else:
        logger.error("No match for taxon {} : {}".format(name, match))
        return None


if __name__ == "__main__":

    setup_logging()
    logger = logging.getLogger(__name__)

    # Read command line args and configuration file
    parser = argparse.ArgumentParser(
        description="Search for occurrences of taxa in a specific country or spatial area and returns the list of taxa with known occurrences."
    )
    parser.add_argument("CONFIG", help="Configuration file (YAML)")
    parser.add_argument("INPUT", help="Input file (CSV)")
    parser.add_argument("OUTPUT", help="Output file (CSV)")
    parser.add_argument(
        "-t",
        "--tag",
        action="store_true",
        help="Add a gbif_filter_tag column to the input data frame.",
    )

    args = parser.parse_args()
    cfg = parse_conf_file(args.CONFIG)

    db_prefix = "GBIF:"

    geometry = None
    country = cfg["country"] if "country" in cfg else None
    if not country:
        geometry = cfg["geometry"] if "geometry" in cfg else None
    zone_str = f"country {country}" if country else "POLYGON"

    kingdom = cfg["taxa_kingdom"] if "taxa_kingdom" in cfg else None

    rank_column = cfg["rank_column"] if "rank_column" in cfg else None
    if not rank_column:
        taxa_rank = cfg["taxa_rank"] if "taxa_rank" in cfg else None
    taxa_column = cfg["taxa_column"]

    # Call processing engine constructors
    occ = OccurrenceEngine(source=GbifAPI())

    # Read input data
    usecols = [taxa_column, rank_column] if rank_column else [taxa_column]
    dtype = {}
    for col in usecols:
        dtype[col] = "object"
    df_taxa = pd.read_csv(args.INPUT, sep=cfg["sep"], dtype=dtype)

    id_cache = {}
    occ_cache = {}
    tags = [None] * df_taxa.shape[0]

    for index, row in df_taxa.iterrows():

        taxon_info = row[taxa_column]
        if pd.isna(taxon_info):
            continue
        taxon_rank = str(row[rank_column]).upper() if rank_column else taxa_rank

        # If the taxa column contains taxa names, try to get taxid in GBIF Backbone Taxonomy
        if cfg["taxa_field"] == "name":
            if taxon_info not in id_cache:
                taxid = get_taxid(taxon_info, taxon_rank, kingdom, db_prefix, logger)
                id_cache[taxon_info] = taxid
                if not taxid:
                    continue
            else:
                taxid = id_cache[taxon_info]

        # If the taxa column contains taxids, just build the TaxId instance
        elif cfg["taxa_field"] == "taxid":
            taxid = TaxId(db_prefix, taxon_info)

        if taxid:
            if str(taxid) not in occ_cache:
                logger.info(f"Look for occurrences of taxon {taxid} in {zone_str}")
                occ_cache[str(taxid)] = occ.has_occurrences(taxid, geometry, country)
                if not occ_cache[str(taxid)]:
                    logger.info(f"Taxon {taxid} not found in zone of interest")

            # If an occurrence has been found for the taxon of interest,
            # keep the corresponding row of the input df
            tags[index] = occ_cache[str(taxid)]

    if args.tag:
        df_taxa["gbif_filter_tag"] = tags
        logger.info("Write filtered data to {}".format(args.OUTPUT))
        df_taxa.to_csv(args.OUTPUT, sep=cfg["sep"], na_rep="NA", index=False)
    else:
        keep = [x == True for x in tags]
        filtered_df = df_taxa[keep]
        logger.info("Write filtered data to {}".format(args.OUTPUT))
        filtered_df.to_csv(args.OUTPUT, sep=cfg["sep"], na_rep="NA", index=False)

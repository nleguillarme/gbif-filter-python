import argparse
import os
import yaml
import logging
import logging.config
import json
import sys
import pandas as pd
from pygbif import species
from box import Box

# from util.taxid import TaxId
from gbif_helper import GbifHelper


def setup_logging(
    default_path="./logging.json", default_level=logging.INFO, env_key="LOG_CFG"
):
    """Setup logging configuration"""
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
            return Box(cfg)
    return None


def validate_config(cfg):
    # Area of interest (country if available, else geometry)
    cfg.country = cfg.country if "country" in cfg else None
    if not cfg.country:
        cfg.geometry = cfg.geometry if "geometry" in cfg else None
    cfg.zone_str = f"country {cfg.country}" if cfg.country else "POLYGON"

    cfg.taxa_kingdom = cfg.taxa_kingdom if "taxa_kingdom" in cfg else None

    cfg.rank_column = cfg.rank_column if "rank_column" in cfg else None
    if not cfg.rank_column:
        cfg.taxa_rank = cfg.taxa_rank if "taxa_rank" in cfg else None

    cfg.name_column = cfg.name_column if "name_column" in cfg else None
    cfg.taxid_column = cfg.taxid_column if "taxid_column" in cfg else None
    if not (cfg.name_column or cfg.taxid_column):
        raise Exception("Need at least one of name_column or taxid_column")

    cfg.resolve_to_rank = cfg.resolve_to_rank if "resolve_to_rank" in cfg else None
    if cfg.resolve_to_rank:
        cfg.resolve_to_rank = (
            cfg.resolve_to_rank.upper()
            if cfg.resolve_to_rank.upper() in ["SPECIES", "GENUS"]
            else "SPECIES"
        )

    cfg.habitat = (
        cfg.habitat
        if ("habitat" in cfg and cfg.habitat in ["TERRESTRIAL", "FRESHWATER", "MARINE"])
        else None
    )
    return cfg


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
    cfg = validate_config(cfg)
    logger.debug(cfg)

    # Create occurrence engine
    gbif = GbifHelper()

    # Read input data
    usecols = [x for x in [cfg.name_column, cfg.taxid_column, cfg.rank_column] if x]
    dtype = {col: "object" for col in usecols}
    df_taxa = pd.read_csv(args.INPUT, sep=cfg.sep, dtype=dtype)

    logger.debug(df_taxa)

    id_cache = {}
    occ_cache = {}
    tags = [None] * df_taxa.shape[0]
    resolved_names = [None] * df_taxa.shape[0]
    resolved_ids = [None] * df_taxa.shape[0]

    for index, row in df_taxa.iterrows():

        # Get valid GBIF taxid from taxon id or name
        name = row[cfg.name_column] if cfg.name_column else None
        taxid = row[cfg.taxid_column] if cfg.taxid_column else None
        name = None if pd.isna(name) else name
        taxid = None if pd.isna(taxid) else taxid
        if not (name or taxid):
            continue
        taxon_info = taxid if taxid else name
        taxon_rank = str(row[cfg.rank_column]) if cfg.rank_column else cfg.taxa_rank
        taxon_rank = taxon_rank.upper() if taxon_rank else taxon_rank

        if taxon_info not in id_cache:  # Update id cache
            taxid, rank = gbif.get_valid_taxid(
                name, taxid, taxon_rank, cfg.taxa_kingdom
            )
            id_cache[taxon_info] = (taxid, rank if rank else taxon_rank)
        else:  # Read id cache
            taxid, rank = id_cache[taxon_info]

        if taxid:
            if taxid not in occ_cache:

                logger.info(f"Look for occurrences of taxon {taxid} in {cfg.zone_str}")
                occ_cache[str(taxid)] = gbif.has_occurrences(
                    taxid, cfg.geometry, cfg.country
                )
                if not occ_cache[str(taxid)]:
                    logger.info(f"Taxon {taxid} not found in zone of interest")
                else:
                    logger.info(f"Taxon {taxid} found in zone of interest")
                    if (
                        cfg.resolve_to_rank
                        and rank
                        and rank != cfg.resolve_to_rank
                        and rank in ["FAMILY", "GENUS"]
                    ):
                        children = gbif.get_children(
                            parent_taxid=taxid,
                            children_rank=cfg.resolve_to_rank,
                            habitat=cfg.habitat,
                        )
                        logger.debug(
                            f"Child for {taxid} with rank {cfg.resolve_to_rank} and accepted name = {len(children)}"
                        )
                        children = gbif.apply_spatial_filter(
                            children, cfg.geometry, cfg.country
                        )
                        logger.debug(
                            f"Child for {taxid} with rank {cfg.resolve_to_rank} and in interest area = {len(children)}"
                        )

                        resolved_names[index] = [x["canonicalName"] for x in children]
                        resolved_ids[index] = [x["key"] for x in children]

            # If an occurrence has been found for the taxon of interest,
            # keep the corresponding row of the input df
            tags[index] = occ_cache[str(taxid)]

    # Write results to output file
    column_offset = 0
    if cfg.resolve_to_rank:
        df_taxa[
            f"gbif_filter_resolved_{cfg.resolve_to_rank.lower()}_names"
        ] = resolved_names
        df_taxa[
            f"gbif_filter_resolved_{cfg.resolve_to_rank.lower()}_ids"
        ] = resolved_ids
        column_offset = 2
    if args.tag:
        df_taxa.insert(len(df_taxa.columns) - column_offset, "gbif_filter_tag", tags)
        logger.info("Write filtered data to {}".format(args.OUTPUT))
        df_taxa.to_csv(args.OUTPUT, sep=cfg["sep"], na_rep="NA", index=False)
    else:
        keep = [x == True for x in tags]
        filtered_df = df_taxa[keep]
        logger.info("Write filtered data to {}".format(args.OUTPUT))
        filtered_df.to_csv(args.OUTPUT, sep=cfg["sep"], na_rep="NA", index=False)

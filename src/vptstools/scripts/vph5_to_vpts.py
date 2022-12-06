import csv
import glob
import json
import os
import sys
from pathlib import Path
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Any

import click
from vptstools.odimh5 import ODIMReader, InvalidSourceODIM
from vptstools.vpts import check_vp_odim, Profile

# Return codes (0 = success)
EXIT_INVALID_SOURCE_FILE = 1
EXIT_NO_SOURCE_DATA = 2
EXIT_INCONSISTENT_METADATA = 3


@click.command()
@click.argument("ODIM_hdf5_profiles")
@click.option("-o", "--output-dir-path", default="vpts_out")
def cli(odim_hdf5_profiles, output_dir_path):
    """This tool aggregate/convert a bunch of ODIM hdf5 profiles files to a
    single vpts data package"""
    # Open all ODIM files
    click.echo("Opening all the source ODIM files...", nl=False)
    odims = [ODIMReader(path) for path in glob.glob(odim_hdf5_profiles, recursive=True)]
    click.echo("Done")

    if not odims:
        click.echo(
            f"No source data file found, is the supplied "
            f"pattern ({odim_hdf5_profiles}) correct?"
        )
        sys.exit(EXIT_NO_SOURCE_DATA)

    # Individual checks for each of them
    click.echo("Individual checks on all source files...", nl=False)
    for source_odim in odims:
        try:
            check_vp_odim(source_odim)
        except InvalidSourceODIM as e:
            click.echo(f"Invalid ODIM source file: {e}")
            sys.exit(EXIT_INVALID_SOURCE_FILE)
    click.echo("Done")

    click.echo("Building and sorting profiles...", nl=False)
    # Profiles will be sorted by datetimes, and (in each) levels by height
    profiles = sorted([Profile.from_odim(odim) for odim in odims])
    click.echo("Done")

    click.echo("Checking consistency of input files...", nl=False)
    # Extract global (to all profiles) metadata, and return an error if inconsistent
    global_metadata = {}  # Shared between all profiles
    # Check all profile refer to the same radar:
    if all(
        profile.radar_identifiers == profiles[0].radar_identifiers
        for profile in profiles
    ):
        global_metadata["radar_identifiers"] = profiles[0].radar_identifiers
    else:
        click.echo("Inconsistent radar identifiers in the source odim files!")
        sys.exit(EXIT_INCONSISTENT_METADATA)
    click.echo("Done")

    click.echo("Aggregating data...", nl=False)
    # Aggregate the tables for each profile to a single one
    full_data_table = []
    for profile in profiles:
        table = profile.to_table()
        for row in table:
            full_data_table.append((row))
    click.echo("Done")

    click.echo("Saving to vpts...", nl=False)
    save_to_vpts(
        full_data_table, folder_path_output=Path(output_dir_path), source_metadata=global_metadata
    )
    click.echo("Done")


if __name__ == "__main__":
    cli(
        [
            "/Users/nicolas_noe/denmark_vp_20131229_short/dkbor_vp_*",
            "-o",
            "/Users/nicolas_noe/vpts_out",
        ]
    )
    # cli(['--help'])

# TODO: print progress during execution (+progress bar)
# TODO: CSV dialect: explicitly configure + express in datapackage.json
# (already done for field separator)
# TODO: Write a full integration test (takes a few ODIM and check the end result)
# TODO: VPTS: replace vol2bird example (+table schema) by something more up-to-date
# TODO: Put more metadata (radar, ...) in datapackage.json
# TODO: The standard allows temporal gap, but no height gap. Make sure all input
# ODIM files have the same altitudes?

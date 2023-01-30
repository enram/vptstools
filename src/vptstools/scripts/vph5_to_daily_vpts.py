import csv
import urllib
import glob
import tempfile
import shutil
import json
import os
import sys
from pathlib import Path
from dataclasses import dataclass, field
from datetime import datetime, date
from typing import List, Any

import click
import s3fs

from vptstools.odimh5 import ODIMReader, InvalidSourceODIM
from vptstools.vpts import check_vp_odim, vpts, vpts_to_csv, OdimFilePath
from vptstools.s3 import handle_manifest

# Return codes (0 = success)
EXIT_INVALID_SOURCE_FILE = 1
EXIT_NO_SOURCE_DATA = 2
EXIT_INCONSISTENT_METADATA = 3

S3_BUCKET = "aloft"
MANIFEST_URL = f"s3://{S3_BUCKET}/inventory/{S3_BUCKET}/{S3_BUCKET}-hdf5-files-inventory"
MANIFEST_HOUR_OF_DAY = "01-00"


@click.command()
@click.option("--days-to-look-back", "look_back", default=1)
def cli(look_back):
    """From h5 vp files to daily vpts files

    Check the latest added h5 files on the s3 bucket using the s3 inventory,
    convert all ODIM hdf5 profiles files for the days with updated files to
    a single vpts-csv file and upload the vpts-csv file to s3.

    """
    # Load the s3 manifest of today
    manifest_parent_key = date.today().strftime(f"%Y-%m-%dT{MANIFEST_HOUR_OF_DAY}Z")
    s3_url = f"{MANIFEST_URL}/{manifest_parent_key}/manifest.json"  # define manifest of today
    url = urllib.parse.urlparse(s3_url)

    # TODO - refactor this url-input logic
    df_cov, days_to_create_vpts = handle_manifest(url, look_back=look_back)

    # Write coverage file to bucket main level
    # TODO - extend and check if this works when inventory is checking mutliple sources (ecog, baltrad,...°
    # convert to /.../ format and save to CSV
    # df_cov.to_csv(..., index=False)

    # Run vpts daily conversion for each radar-day with modified files
    inbo_s3 = s3fs.S3FileSystem(profile="inbo-prd")
    for j, daily_vpts in enumerate(days_to_create_vpts["directory"]):

        # Enlist files of the day to rerun (all the given day)
        source, radar_code, year, month, day = daily_vpts
        odim_path = OdimFilePath(source, radar_code, "vp", year, month, day)
        odim5_files = inbo_s3.ls("aloft/" + odim_path.s3_folder_path_h5)

        # - create tempdir
        folder_path = Path(tempfile.mkdtemp())
        print(folder_path, daily_vpts)

        # - download the files
        for i, file_key in enumerate(odim5_files):
            inbo_s3.download(file_key, str(folder_path / file_key.split("/")[-1]))

        # - run vpts on all files
        df_vpts = vpts([folder_path / file_key.split("/")[-1] for file_key in odim5_files])

        # - save file
        tmp_vpts_file = odim_path.s3_file_path_daily_vpts.split("/")[-1]
        vpts_to_csv(df_vpts, folder_path / odim_path.s3_file_path_daily_vpts)

        # - copy to s3
        inbo_s3.put(str(folder_path / odim_path.s3_file_path_daily_vpts), f"aloft/{odim_path.s3_file_path_daily_vpts}")

        # - remove tempdir
        shutil.rmtree(folder_path)




    # # Open all ODIM files
    # click.echo("Opening all the source ODIM files...", nl=False)
    # odims = [ODIMReader(path) for path in glob.glob(odim_hdf5_profiles, recursive=True)]
    # click.echo("Done")
    #
    # if not odims:
    #     click.echo(
    #         f"No source data file found, is the supplied "
    #         f"pattern ({odim_hdf5_profiles}) correct?"
    #     )
    #     sys.exit(EXIT_NO_SOURCE_DATA)
    #
    # # Individual checks for each of them
    # click.echo("Individual checks on all source files...", nl=False)
    # for source_odim in odims:
    #     try:
    #         check_vp_odim(source_odim)
    #     except InvalidSourceODIM as e:
    #         click.echo(f"Invalid ODIM source file: {e}")
    #         sys.exit(EXIT_INVALID_SOURCE_FILE)
    # click.echo("Done")

    # click.echo("Building and sorting profiles...", nl=False)
    # # Profiles will be sorted by datetimes, and (in each) levels by height
    # profiles = sorted([Profile.from_odim(odim) for odim in odims])
    # click.echo("Done")

    # Ask Peter - what with consistency?

    # click.echo("Checking consistency of input files...", nl=False)
    # # Extract global (to all profiles) metadata, and return an error if inconsistent
    # global_metadata = {}  # Shared between all profiles
    # # Check all profile refer to the same radar:
    # if all(
    #     profile.radar_identifiers == profiles[0].radar_identifiers
    #     for profile in profiles
    # ):
    #     global_metadata["radar_identifiers"] = profiles[0].radar_identifiers
    # else:
    #     click.echo("Inconsistent radar identifiers in the source odim files!")
    #     sys.exit(EXIT_INCONSISTENT_METADATA)
    # click.echo("Done")

    # click.echo("Aggregating data...", nl=False)
    # # Aggregate the tables for each profile to a single one
    # df_vpts = vpts(odims, "v1")  # TODO -- versie als input van de CLI
    # click.echo("Done")
    #
    # click.echo("Saving to vpts...", nl=False)
    # vpts_to_csv(df_vpts, Path(output_dir_path) / "vpts.csv", descriptor=False)

    click.echo("Done")


if __name__ == "__main__":
    cli()
    # cli(['--help'])

# TODO: print progress during execution (+progress bar)
# TODO: CSV dialect: explicitly configure + express in datapackage.json
# (already done for field separator)
# TODO: Write a full integration test (takes a few ODIM and check the end result)
# TODO: VPTS: replace vol2bird example (+table schema) by something more up-to-date
# TODO: Put more metadata (radar, ...) in datapackage.json
# TODO: The standard allows temporal gap, but no height gap. Make sure all input
# ODIM files have the same altitudes?

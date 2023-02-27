import urllib
import tempfile
import shutil
from pathlib import Path
from datetime import date

import click
import s3fs

from vptstools.vpts import vpts, vpts_to_csv
from vptstools.s3 import handle_manifest, OdimFilePath

# Return codes (0 = success)
EXIT_INVALID_SOURCE_FILE = 1
EXIT_NO_SOURCE_DATA = 2
EXIT_INCONSISTENT_METADATA = 3

S3_BUCKET = "aloft"
MANIFEST_URL = f"s3://aloft-inventory/{S3_BUCKET}/{S3_BUCKET}-hdf5-files-inventory"
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

    df_cov, days_to_create_vpts = handle_manifest(s3_url, look_back=look_back)

    # Run vpts daily conversion for each radar-day with modified files
    inbo_s3 = s3fs.S3FileSystem()
    for j, daily_vpts in enumerate(days_to_create_vpts["directory"]):

        # Enlist files of the day to rerun (all the given day)
        source, radar_code, year, month, day = daily_vpts
        odim_path = OdimFilePath(source, radar_code, "vp", year, month, day)
        odim5_files = inbo_s3.ls(f"{S3_BUCKET}/" + odim_path.s3_folder_path_h5)

        # - create tempdir
        folder_path = Path(tempfile.mkdtemp())

        # - download the files
        for i, file_key in enumerate(odim5_files):
            inbo_s3.download(file_key, str(folder_path / file_key.split("/")[-1]))

        # - run vpts on all files
        df_vpts = vpts([folder_path / file_key.split("/")[-1] for file_key in odim5_files])

        # - save file
        tmp_vpts_file = odim_path.s3_file_path_daily_vpts.split("/")[-1]
        vpts_to_csv(df_vpts, folder_path / tmp_vpts_file)

        # - copy to s3
        inbo_s3.put(str(folder_path / odim_path.s3_file_path_daily_vpts),
                    f"{S3_BUCKET}/{odim_path.s3_file_path_daily_vpts}")

        # - remove tempdir
        shutil.rmtree(folder_path)

    click.echo("Done")


if __name__ == "__main__":
    cli()
    # cli(['--help'])

# TODO: print progress during execution (+progress bar)
# TODO: Write a full integration test (takes a few ODIM and check the end result)
# TODO: The standard allows temporal gap, but no height gap. Make sure all input
# ODIM files have the same altitudes?

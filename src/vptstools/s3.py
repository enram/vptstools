import os
import re
import json
from pathlib import Path
from operator import attrgetter
from collections import namedtuple
from dataclasses import dataclass

import boto3
import pandas as pd


@dataclass(frozen=True)
class OdimFilePath:
    """ODIM file path with translation to different s3 key paths"""
    source: str
    radar_code: str
    data_type: str
    year: str
    month: str
    day: str
    hour: str = "00"
    minute: str = "00"
    file_name: str = ""  # optional as this can be constructed from scratch as well

    @classmethod
    def from_file_name(cls, h5_file_path, source):
        """"""
        return cls(source, *cls.parse_file_name(str(h5_file_path)))

    @classmethod
    def from_inventory(cls, h5_file_path):
        """"""
        return cls(h5_file_path.split("/")[0], *cls.parse_file_name(str(h5_file_path)))

    @staticmethod
    def parse_file_name(name):
        """Parse an hdf5 file name radar_code, year, month, day, hour, minute.

        Parameters
        ----------
        name : str
            File name to be parsed. An eventual parent path and
            extension will be removed

        Returns
        -------
        radar_code, data_type, year, month, day, hour, minute

        Notes
        -----
        File format is according to the following file format::

            ccrrr_vp_yyyymmddhhmmss.h5

        with ``c`` the country code two-letter ids and ``rrr``
        the radar three-letter id, e.g. bejab_vp_20161120235500.h5.
        Path information in front of the h5 name itself are ignored.
        """

        name_regex = re.compile(
            r'.*([^_]{2})([^_]{3})_([^_]*)_(\d\d\d\d)(\d\d)(\d\d)T?'
            r'(\d\d)(\d\d)(?:Z|00)+.*\.h5')
        match = re.match(name_regex, name)
        if match:
            file_name = Path(name).name
            country, radar, data_type, year, \
                month, day, hour, minute = match.groups()
            radar_code = country + radar
            return radar_code, data_type, year, month, day, hour, minute, file_name
        else:
            raise ValueError("File name is not a valid ODIM h5 file.")

    @property
    def country(self):
        """"""
        return self.radar_code[:2]

    @property
    def radar(self):
        """"""
        return self.radar_code[2:]

    def _s3_path_setup(self, file_output):
        """Common setup of the s3 bucket logic"""
        return f"{self.source}/{file_output}/{self.radar_code}/{self.year}"

    def s3_url_h5(self, bucket="aloft"):
        return f"s3://{bucket}/{self._s3_path_setup('hdf5')}/{self.month}/{self.day}/{self.file_name}"

    @property
    def s3_folder_path_h5(self):
        return f"{self._s3_path_setup('hdf5')}/{self.month}/{self.day}"

    @property
    def s3_file_path_daily_vpts(self):
        return f"{self._s3_path_setup('daily')}/{self.radar_code}_vpts_{self.year}{self.month}{self.day}.csv"

    @property
    def s3_file_path_monthly_vpts(self):
        return f"{self._s3_path_setup('monthly')}/{self.radar_code}_vpts_{self.year}{self.month}.csv"


def extract_coverage_group_from_s3_inventory(file_path):
    """Extract file name components to define a group

    The coverage file counts the number of files available
    per group (e.g. daily files per radar). This function is passed
    to the Pandas Groupby to translate the file path to a
    countable set (e.g. source, radar-code, year month and day for
    daily files per radar).

    Parameters
    ----------
    file_path : str
        File path of the ODIM h5 file. Only the file name is taken
        into account and a folder-path is ignored.
    """
    path_info = OdimFilePath.from_inventory(file_path)
    return (path_info.source, path_info.radar_code,
            path_info.year, path_info.month, path_info.day)


def list_manifest_file_keys(bucket, manifest_path):
    """Enlist the manifest individual files

    Parameters
    ----------
    bucket : str
        s3 Bucket to enlist manifest from
    manifest_path : str
        path of the manifest file in s3 (relative to main
        level in s3 bucket)
    """
    s3 = boto3.resource('s3')
    manifest = json.load(s3.Object(bucket, manifest_path).get()['Body'])
    for obj in manifest['files']:
        yield obj


def _last_modified_from_manifest_subfile(df, look_back="2day"):
    """Filter manifest files on last modified

    Parameters
    ----------
    df : pandas.DataFrame
        s3 csv-based inventory read by pandas
    look_back : str , default '2day'
        Pandas Timedelta valid string
    """
    return df[df["modified"] > pd.Timestamp.now(tz="utc") - pd.Timedelta(look_back)]


def _radar_day_counts_from_manifest_subfile(df, group_callable=extract_coverage_group_from_s3_inventory):
    """Convert"""
    return df.set_index("file").groupby(group_callable).size()


def handle_manifest(manifest_url, bucket, look_back="2day"):
    """Extract modified days and coverage from a manifest file

    Parameters
    ----------
    manifest_url : str
        URL of the s3 inventory manifest file to use
    bucket : str
        Name of the bucket
    look_back : str, default '2day'
        Time period to check for 'modified date' to extract
        the subset of files that should trigger a rerun.

    Returns
    -------
    df_cov : pandas.DataFrame
        DataFrame with the 'directory' info (source, radar_code,
        year, month, day) and the number of files in the s3 bucket.
    days_to_create_vpts : pandas.DataFrame
        DataFrame with the 'directory' info (source, radar_code,
        year, month, day) and the number of new files within the
        look back period.

    Notes
    -----
    Check https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-inventory.html
    for more informationn on s3 bucket inventory and manifest files.
    """
    df_last_n_days = []
    df_coverage = []
    for j, obj in enumerate(list_manifest_file_keys(manifest_url.hostname, manifest_url.path.lstrip('/'))):
        print(obj["key"])
        # Read the manifest subfile
        df = pd.read_csv(f"s3://{bucket}/{obj['key']}", engine="pyarrow",
                         names=["repo", "file", "size", "modified"])

        # Count occurrences per radar-day -> coverage input
        df_coverage.append(_radar_day_counts_from_manifest_subfile(df, extract_coverage_group_from_s3_inventory))

        # Extract IDs latest N days modified files
        df_last_n_days.append(_last_modified_from_manifest_subfile(df, look_back))

    # Create coverage file DataFrame
    df_cov = pd.concat(df_coverage)
    df_cov = df_cov.reset_index().groupby("index")[0].sum().reset_index()
    df_cov = df_cov.rename(columns={"index": "directory", 0: "file_count"})

    # Create modified days DataFrame
    df_mod = pd.concat(df_last_n_days)
    days_to_create_vpts = df_mod.set_index("file").groupby(
        extract_coverage_group_from_s3_inventory).size().reset_index()
    days_to_create_vpts = days_to_create_vpts.rename(columns={"index": "directory", 0: "file_count"})

    return df_cov, days_to_create_vpts

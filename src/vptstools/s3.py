import re
import urllib
import json
from pathlib import Path
from dataclasses import dataclass

import s3fs
import pandas as pd


@dataclass(frozen=True)
class OdimFilePath:
    """ODIM file path with translation from/to different S3 key paths

    Parameters
    ----------
    source: str
        Data source, e.g. baltrad, ecog-04003,...
    radar_code: str
        country + radar code
    data_type: str
        ODIM data type, e.g. vp, pvol,...
    year: str
        year, YYYY
    month: str
        month, MM
    day: str
        day, DD
    hour: str = "00"
        hour, HH
    minute: str = "00"
        minute, MM
    file_name: str = "", optional
        File name from which the other properties were derived
    file_type: str = "", optional
        File type from which the other properties were derived, e.g. hdf5
    """

    source: str
    radar_code: str
    data_type: str
    year: str
    month: str
    day: str
    hour: str = "00"
    minute: str = "00"
    file_name: str = ""  # optional as this can be constructed from scratch as well
    file_type: str = ""

    @classmethod
    def from_file_name(cls, h5_file_path, source):
        """Initialize class from ODIM file path"""
        return cls(source, *cls.parse_file_name(str(h5_file_path)))

    @classmethod
    def from_inventory(cls, h5_file_path):
        """Initialize class from S3 inventory which contains source and file_type"""
        return cls(
            h5_file_path.split("/")[0],
            *cls.parse_file_name(str(h5_file_path)),
            h5_file_path.split("/")[1],
        )

    @classmethod
    def from_s3fs_enlisting(cls, h5_file_path):
        """Initialize class from S3 inventory which contains bucket,
        source and file_type"""
        return cls(
            h5_file_path.split("/")[1],
            *cls.parse_file_name(str(h5_file_path)),
            h5_file_path.split("/")[1],
        )

    @staticmethod
    def parse_file_name(file_name):
        """Parse an hdf5 file name radar_code, data_type, year, month, day, hour,
        minute and file_name.

        Parameters
        ----------
        file_name : str
            File name to be parsed. An eventual parent path and
            extension will be removed

        Returns
        -------
        radar_code, data_type, year, month, day, hour, minute, file_name

        Notes
        -----
        File format is according to the following file format::

            ccrrr_vp_yyyymmddhhmmss.h5

        with ``c`` the country code two-letter ids and ``rrr``
        the radar three-letter id, e.g. bejab_vp_20161120235500.h5.
        Path information in front of the h5 name itself are ignored.
        """

        name_regex = re.compile(
            r".*([a-zA-Z]{2})([a-zA-Z]{3})_([a-z]*)_(\d\d\d\d)(\d\d)(\d\d)T?"
            r"(\d\d)(\d\d)(?:Z|00)?.*\.h5"
        )
        match = re.match(name_regex, file_name)
        if match:
            file_name = Path(file_name).name
            country, radar, data_type, year, month, day, hour, minute = match.groups()
            radar_code = country + radar
            return radar_code.lower(), data_type, year, month, day, hour, minute, file_name
        else:
            raise ValueError(f"File name {file_name} is not a valid ODIM h5 file.")

    @property
    def country(self):
        """Country code"""
        return self.radar_code[:2]

    @property
    def radar(self):
        """Radar code"""
        return self.radar_code[2:]

    @property
    def daily_vpts_file_name(self):
        """Name of the corresponding daily vpts file"""
        return f"{self.radar_code}_vpts_{self.year}{self.month}{self.day}.csv"

    def s3_path_setup(self, file_output):
        """Common setup of the S3 bucket logic"""
        return f"{self.source}/{file_output}/{self.radar_code}/{self.year}"

    def s3_url_h5(self, bucket="aloft"):
        """Full S3 URL for the stored h5 file"""
        return (
            f"s3://{bucket}/{self.s3_path_setup('hdf5')}/"
            f"{self.month}/{self.day}/{self.file_name}"
        )

    @property
    def s3_folder_path_h5(self):
        """S3 key with the folder containing the h5 file"""
        return f"{self.s3_path_setup('hdf5')}/{self.month}/{self.day}"

    @property
    def s3_file_path_daily_vpts(self):
        """S3 key of the daily vpts file corresponding to the h5 file"""
        return f"{self.s3_path_setup('daily')}/{self.daily_vpts_file_name}"

    @property
    def s3_file_path_monthly_vpts(self):
        """S3 key of the monthly concatenated vpts file corresponding to the h5 file"""
        return (
            f"{self.s3_path_setup('monthly')}/"
            f"{self.radar_code}_vpts_{self.year}{self.month}.csv.gz"
        )


def list_manifest_file_keys(s3_manifest_url, storage_options=None):
    """Enlist the manifest individual files

    Parameters
    ----------
    s3_manifest_url : str
        S3 URL to manifest file
    storage_options : dict, optional
        Additional parameters passed to the read_csv to access the
        S3 manifest files, eg. custom AWS profile options
        ({"profile": "inbo-prd"})
    """
    if not storage_options:
        storage_options = {}
    s3fs_s3 = s3fs.S3FileSystem(**storage_options)
    with s3fs_s3.open(s3_manifest_url) as manifest:
        manifest_json = json.load(manifest)
        for obj in manifest_json["files"]:
            yield obj


def extract_daily_group_from_inventory(file_path):
    """Extract file name components to define a group

    The coverage file counts the number of files available
    per group (e.g. daily files per radar). This function is passed
    to the Pandas ``groupby`` to translate the file path to a
    countable set (e.g. source, radar-code, year month and day for
    daily files per radar).

    Parameters
    ----------
    file_path : str
        File path of the ODIM h5 file. Only the file name is taken
        into account and a folder-path is ignored.
    """
    path_info = OdimFilePath.from_inventory(file_path)
    return (
        path_info.source,
        path_info.file_type,
        path_info.radar_code,
        path_info.year,
        path_info.month,
        path_info.day,
    )


def _last_modified_from_inventory(df, modified_days_ago="2day"):
    """Filter manifest files on last modified

    Parameters
    ----------
    df : pandas.DataFrame
        S3 csv-based inventory read by pandas
    modified_days_ago : str , default '2day'
        Pandas Timedelta valid string
    """
    return df[
        df["modified"] > (pd.Timestamp.now(tz="utc") - pd.Timedelta(modified_days_ago))
    ]


def _radar_day_counts_from_inventory(
    df, group_callable=extract_daily_group_from_inventory
):
    """Count files according to groups as defined by callable

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame need to contain a 'file' column used to count and is passed to callable
    group_callable : callable
        Function to translate the file name into individual groups

    Returns
    -------
    df : pandas DataFrame
        DataFrame containing the counts per group
    """
    return df.set_index("file").groupby(group_callable).size()


def _handle_inventory(
    df, modified_days_ago, group_func=extract_daily_group_from_inventory
):
    """Extract modified days and coverage from a single inventory df

    Parameters
    ----------
    df : pandas.DataFrame
        Pandas DataFrame of a parsed inventory file with the columns
        "repo" (str), "file" (str), "size" (int) and "modified" (datetime)
    modified_days_ago : str
        pandas Timedelta description, e.g. 2days
    group_func : callable
        Function used to create countable groups

    Returns
    -------
    df_last_n_days : pandas.DataFrame
        pandas.DataFrame with the 'directory' info (source, radar_code,
        year, month, day) and the number of new files within the
        look back period.
    df_coverage : pandas.DataFrame
        pandas.DataFrame with the 'directory' info (source, radar_code,
        year, month, day) and the number of files in the S3 bucket for each group.

    """
    # Filter for h5 files and extract source
    df["modified"] = pd.to_datetime(
        df["modified"], format="%Y-%m-%dT%H:%M:%S.%fZ", utc=True
    )
    df["file_items"] = df["file"].str.split("/")
    df["suffix"] = df["file_items"].str.get(-1).str.split(".").str.get(-1)
    df["source"] = df["file_items"].str.get(0)
    df = df[df["suffix"] == "h5"]
    df = df.drop(columns=["file_items", "suffix"])

    # Extract IDs latest N days modified files
    df_last_n_days = _last_modified_from_inventory(df, modified_days_ago)
    # Count occurrences per radar-day -> coverage input
    df_coverage = _radar_day_counts_from_inventory(df, group_func)
    return df_coverage, df_last_n_days


def handle_manifest(manifest_url, modified_days_ago="2day", storage_options=None):
    """Extract modified days and coverage from a manifest file

    Parameters
    ----------
    manifest_url : str
        URL of the S3 inventory manifest file to use; s3://...
    modified_days_ago : str, default '2day'
        Time period to check for 'modified date' to extract
        the subset of files that should trigger a rerun.
    storage_options : dict, optional
        Additional parameters passed to the read_csv to access the
        S3 manifest files, eg. custom AWS profile options
        ({"profile": "inbo-prd"})

    Returns
    -------
    df_cov : pandas.DataFrame
        DataFrame with the 'directory' info (source, radar_code,
        year, month, day) and the number of files in the S3 bucket.
    df_days_to_create_vpts : pandas.DataFrame
        DataFrame with the 'directory' info (source, radar_code,
        year, month, day) and the number of new files within the
        look back period.

    Notes
    -----
    Check https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-inventory.html
    for more information on S3 bucket inventory and manifest files.
    """

    # TODO - add additional checks on input
    df_last_n_days = []
    df_coverage = []
    for j, obj in enumerate(list_manifest_file_keys(manifest_url, storage_options)):
        # Read the manifest referenced file
        parsed_url = urllib.parse.urlparse(manifest_url)

        with pd.read_csv(
            f"s3://{parsed_url.netloc}/{obj['key']}",
            engine="c",
            names=["repo", "file", "size", "modified"],
            storage_options=storage_options,
            chunksize=50000,
        ) as reader:
            for chunk in reader:
                # Extract counts per group and groups within defined time window
                df_co, df_last = _handle_inventory(
                    chunk,
                    modified_days_ago,
                    group_func=extract_daily_group_from_inventory,
                )
                # Extract IDs latest N days modified files
                df_last_n_days.append(df_last)
                # Count occurrences per radar-day -> coverage input
                df_coverage.append(df_co)

    # Create coverage file DataFrame
    df_cov = pd.concat(df_coverage)
    df_cov = df_cov.reset_index().groupby("index")[0].sum().reset_index()
    df_cov = df_cov.rename(columns={"index": "directory", 0: "file_count"})

    # Create modified days DataFrame
    df_mod = pd.concat(df_last_n_days)
    df_days_to_create_vpts = (
        df_mod.set_index("file")
        .groupby(extract_daily_group_from_inventory)
        .size()
        .reset_index()
    )
    df_days_to_create_vpts = df_days_to_create_vpts.rename(
        columns={
            "index": "directory",
            "file": "directory",  # mapping depends on content; both included
            0: "file_count",
        }
    )

    return df_cov, df_days_to_create_vpts

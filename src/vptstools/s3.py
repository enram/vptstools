import os
import json
from operator import attrgetter
from collections import namedtuple

import boto3
import pandas as pd

from vptstools.vpts import OdimFilePath


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


# From https://stackoverflow.com/questions/35803027/retrieving-subfolders-names-in-s3-bucket-from-boto3
S3Obj = namedtuple('S3Obj', ['key', 'mtime', 'size', 'ETag'])


def s3list(bucket, path, start=None, end=None, recursive=True, list_dirs=True,
           list_objs=True, limit=None):
    """
    Iterator that lists a bucket's objects under path, (optionally) starting with
    start and ending before end.

    If recursive is False, then list only the "depth=0" items (dirs and objects).

    If recursive is True, then list recursively all objects (no dirs).

    Args:
        bucket:
            a boto3.resource('s3').Bucket().
        path:
            a directory in the bucket.
        start:
            optional: start key, inclusive (may be a relative path under path, or
            absolute in the bucket)
        end:
            optional: stop key, exclusive (may be a relative path under path, or
            absolute in the bucket)
        recursive:
            optional, default True. If True, lists only objects. If False, lists
            only depth 0 "directories" and objects.
        list_dirs:
            optional, default True. Has no effect in recursive listing. On
            non-recursive listing, if False, then directories are omitted.
        list_objs:
            optional, default True. If False, then directories are omitted.
        limit:
            optional. If specified, then lists at most this many items.

    Returns:
        an iterator of S3Obj.

    Examples:
        # set up
        >>> s3 = boto3.resource('s3')
        ... bucket = s3.Bucket('bucket-name')

        # iterate through all S3 objects under some dir
        >>> for p in s3list(bucket, 'some/dir'):
        ...     print(p)

        # iterate through up to 20 S3 objects under some dir, starting with foo_0010
        >>> for p in s3list(bucket, 'some/dir', limit=20, start='foo_0010'):
        ...     print(p)

        # non-recursive listing under some dir:
        >>> for p in s3list(bucket, 'some/dir', recursive=False):
        ...     print(p)

        # non-recursive listing under some dir, listing only dirs:
        >>> for p in s3list(bucket, 'some/dir', recursive=False, list_objs=False):
        ...     print(p)
"""
    kwargs = dict()
    if start is not None:
        if not start.startswith(path):
            start = os.path.join(path, start)
        # note: need to use a string just smaller than start, because
        # the list_object API specifies that start is excluded (the first
        # result is *after* start).
        kwargs.update(Marker=__prev_str(start))
    if end is not None:
        if not end.startswith(path):
            end = os.path.join(path, end)
    if not recursive:
        kwargs.update(Delimiter='/')
        if not path.endswith('/'):
            path += '/'
    kwargs.update(Prefix=path)
    if limit is not None:
        kwargs.update(PaginationConfig={'MaxItems': limit})

    paginator = bucket.meta.client.get_paginator('list_objects')
    for resp in paginator.paginate(Bucket=bucket.name, **kwargs):
        q = []
        if 'CommonPrefixes' in resp and list_dirs:
            q = [S3Obj(f['Prefix'], None, None, None) for f in resp['CommonPrefixes']]
        if 'Contents' in resp and list_objs:
            q += [S3Obj(f['Key'], f['LastModified'], f['Size'], f['ETag']) for f in resp['Contents']]
        # note: even with sorted lists, it is faster to sort(a+b)
        # than heapq.merge(a, b) at least up to 10K elements in each list
        q = sorted(q, key=attrgetter('key'))
        if limit is not None:
            q = q[:limit]
            limit -= len(q)
        for p in q:
            if end is not None and p.key >= end:
                return
            yield p


def __prev_str(s):
    if len(s) == 0:
        return s
    s, c = s[:-1], ord(s[-1])
    if c > 0:
        s += chr(c - 1)
    s += ''.join(['\u7FFF' for _ in range(10)])
    return s

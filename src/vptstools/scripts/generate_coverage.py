# Script that parse the whole aloft bucket and generate a coverage.csv file
# After discussion with Peter, we decided to keep a simple file count per directory (all listed in a global
# CSV file, one row per "directory")
import csv
import os
import tempfile
from collections import defaultdict
from configparser import ConfigParser
from pathlib import PurePath

import boto3

from vptstools.scripts.constants import CONFIG_FILE
from vptstools.s3 import s3list


def main():
    config = ConfigParser()
    config.read(CONFIG_FILE)

    bucket_name = config.get("destination_bucket", "name")

    session = boto3.Session(profile_name="prod")
    s3 = session.resource('s3')
    bucket = s3.Bucket(bucket_name)

    counter = defaultdict(int)

    print("Looping over files to count")
    for i, e in enumerate(s3list(bucket, "", recursive=True, list_dirs=True, list_objs=True)):
        dir = PurePath(e.key).parent
        counter[str(dir)] += 1
        if i % 10000 == 0:
            print(".", end="")

    print("Done, will now generate coverage.csv")
    with tempfile.TemporaryDirectory() as tmpdirname:
        tmp_coverage_path = os.path.join(tmpdirname, "coverage.csv")
        with open(tmp_coverage_path, 'w') as csvfile:
            fieldnames = ['directory', 'file_count']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()

            for k, v in counter.items():
                writer.writerow({'directory': k, 'file_count': v})

        print("File generated, will now upload it to S3")
        bucket.upload_file(tmp_coverage_path, 'coverage.csv')
        print("Done")


if __name__ == "__main__":
    main()

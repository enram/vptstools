import csv
import glob
import json
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Any

import click
from odimh5.reader import ODIMReader

# Return code (0 = success)
EXIT_INVALID_SOURCE_FILE = 1
EXIT_NO_SOURCE_DATA = 2

DESCRIPTOR_FILENAME = "datapackage.json"
CSV_FILENAME = "vpts.csv"

class InvalidSourceODIM(Exception):
    pass


@dataclass
class Level:
    height: float  # Coded as a 64-bit float in HDF5 file
    variables: dict = field(default_factory=dict)

    def __lt__(self, other):  # Allows sorting by height
        return self.height < other.height


# Data class representing a single input source file (=https://github.com/adokter/vol2bird/wiki/ODIM-bird-profile-format-specification)
# =single timestamp, single radar, multiple altitudes, usual variables for each altitude: dd, ff, ...
# This object aims to stay as close as possible to the HDF5 file (no data simplification/loss at this stage)
@dataclass
class Profile:
    timestamp: datetime
    levels: List[Level] = field(default_factory=list)

    def __lt__(self, other):  # Allows sorting by timestamp
        return self.timestamp < other.timestamp

    def to_table(self):
        """Return a list of dicts representing the content of the profile, such as

        [
            { timestamp: x, height: 0.0, ff: 8.23, ... },
            { timestamp: x, height: 200.0, ff: 5.23, ...}
        ]

        The list is sorted by altitude. The timestamp is obviously identical for all entries
        """
        rows = []

        for level in self.levels:
            rows.append(
                {"timestamp": self.timestamp, "height": level.height, **level.variables}
            )

        return rows

    @classmethod
    def make_from_odim(cls, source_odim: ODIMReader):
        print(f"Reading ODIM file: {ODIMReader}")

        dataset1 = source_odim.hdf5["dataset1"]
        height_values = get_values(dataset1, quantity="HGHT")

        variables_to_load = (
            "ff",
            "dbz",
            "dens",
            "u",
            "v",
            "gap",
            "w",
            "n_dbz",
            "dd",
            "n",
            "DBZH",
            "n_dbz_all",
            "eta",
            "sd_vvp",
            "n_all",
        )

        levels = []
        for i, height in enumerate(height_values):
            levels.append(
                Level(
                    height=height,
                    variables={
                        k: get_values(dataset1, quantity=k)[i]
                        for k in variables_to_load
                    },
                )
            )

        return cls(timestamp=source_odim.root_datetime, levels=sorted(levels))


def check_source_odim(source_odim: ODIMReader) -> None:
    if source_odim.root_object_str != "VP":
        raise InvalidSourceODIM(
            f"Incorrect what.object value: expected VP, found {source_odim.root_object_str}"
        )


def get_values(dataset, quantity: str) -> List[Any]:
    """In a given dataset, find the requested quantity and return a 1d list of the values

    'nodata' and 'undetect' are interpreted according to the metadata in the 'what' group
    """
    for data_group in dataset:
        if dataset[data_group]["what"].attrs["quantity"].decode("utf8") == quantity:
            nodata_val = dataset[data_group]["what"].attrs["nodata"]
            undetect_val = dataset[data_group]["what"].attrs["undetect"]

            values = [entry[0] for entry in dataset[data_group]["data"]]
            values = ["nodata" if value == nodata_val else value for value in values]
            values = [
                "undetect" if value == undetect_val else value for value in values
            ]

            return values


def table_to_csv(full_data_table, output_csv_path):
    keys = full_data_table[0].keys()
    with open(output_csv_path, "w", newline="", encoding="utf8") as output_file:
        fc = csv.DictWriter(output_file, fieldnames=keys)
        fc.writeheader()
        fc.writerows(full_data_table)


def write_descriptor(output_dir):
    content = {
        "resources": [
            {
                "name": "VPTS data",
                "path": CSV_FILENAME,
                "schema": {
                    "fields": []
                }
            }
        ]
    }

    with open(os.path.join(output_dir, DESCRIPTOR_FILENAME), 'w') as outfile:
        json.dump(content, outfile, indent=4, sort_keys=True)




def table_to_vpts(full_data_table, output_dir):
    os.mkdir(output_dir)
    table_to_csv(full_data_table, output_csv_path=os.path.join(output_dir, CSV_FILENAME))
    write_descriptor(output_dir)


@click.command()
@click.argument("ODIM_hdf5_profiles")
@click.option("-o", "--output-dir-path", default="vpts_out")
def cli(odim_hdf5_profiles, output_dir_path):
    """This tool aggregate/convert a bunch of ODIM hdf5 profiles files to a single vpts data package"""
    # Open all ODIM files
    odims = [ODIMReader(path) for path in glob.glob(odim_hdf5_profiles, recursive=True)]

    if not odims:
        click.echo(
            f"No source data file found, is the supplied pattern ({odim_hdf5_profiles}) correct?"
        )
        sys.exit(EXIT_NO_SOURCE_DATA)

    # Individual checks for each of them
    for source_odim in odims:
        try:
            check_source_odim(source_odim)
        except InvalidSourceODIM as e:
            click.echo(f"Invalid ODIM source file: {e}")
            sys.exit(EXIT_INVALID_SOURCE_FILE)

    profiles = sorted([Profile.make_from_odim(odim) for odim in odims])
    # Profiles are now sorted by timestamp, and (in each) levels by height

    # Aggregate the tables for each profile to a single one
    full_table = []
    for profile in profiles:
        table = profile.to_table()
        for row in table:
            full_table.append((row))

    table_to_vpts(full_table, output_dir=output_dir_path)


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
# TODO: Write a full integration test (takes a few ODIM and check the end result)
# TODO: write a function that transform the Profile data (dict?) to a something ready for CSV (i.e. turn height from float to int)
# TODO: VPTS: replace vol2bird example (+table schema) by something more up-to-date

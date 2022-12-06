import csv
import json
from pathlib import Path

from vptstools.vpts import datetime_to_proper8601

DESCRIPTOR_FILENAME = "datapackage.json"
CSV_FILENAME = "vpts.csv"
CSV_ENCODING = "utf8"  # !! Don't change, only utf-8 is accepted in data packages
CSV_FIELD_DELIMITER = ","


# ! TODO - ADJUST TOWARDS NEW vpts module


def table_to_frictionless_csv(full_data_table, file_path_output_csv):
    keys = full_data_table[0].keys()

    # Last round of processing: boolean values must be converted to an equivalent
    # string, otherwise the CSV module will save them Capitalized, while the
    # frictionless specs asks for lowercase.
    for entry in full_data_table:
        for key in entry:
            if entry[key] is True:
                entry[key] = "true"
            if entry[key] is False:
                entry[key] = "false"

    with open(file_path_output_csv, "w", newline="", encoding=CSV_ENCODING) as output_file:
        fc = csv.DictWriter(output_file, fieldnames=keys, delimiter=CSV_FIELD_DELIMITER)
        fc.writeheader()
        fc.writerows(full_data_table)


def write_descriptor(folder_path_output: Path, full_data_table, source_metadata):
    content = {
        "radar": {
            "identifiers": source_metadata[
                "radar_identifiers"
            ]  # TODO: decide and docmuent what to do with that (in VPTS)
        },
        "temporal": {
            "start": datetime_to_proper8601(full_data_table[0]["datetime"]),
            "end": datetime_to_proper8601(full_data_table[-1]["datetime"]),
        },
        "resources": [
            {
                "name": "VPTS data",
                "path": CSV_FILENAME,
                "dialect": {"delimiter": CSV_FIELD_DELIMITER},
                "schema": {"fields": []},
            }
        ],
    }

    with open(folder_path_output / DESCRIPTOR_FILENAME, "w") as outfile:
        json.dump(content, outfile, indent=4, sort_keys=True)


def save_to_vpts(full_data_table, folder_path_output: Path, source_metadata: dict):
    if not folder_path_output.exists():
        folder_path_output.mkdir()
    table_to_frictionless_csv(
        full_data_table, file_path_output_csv=folder_path_output / CSV_FILENAME
    )
    write_descriptor(folder_path_output, full_data_table, source_metadata)
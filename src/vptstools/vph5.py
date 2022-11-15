import csv
import json
from datetime import datetime
from typing import List, Any
from abc import ABC
from pathlib import Path
from dataclasses import dataclass, field, asdict

import pandas as pd
import numpy as np

from vptstools.odimh5 import ODIMReader, InvalidSourceODIM

NODATA = "NA"
UNDETECT = "NaN"

DESCRIPTOR_FILENAME = "datapackage.json"
CSV_FILENAME = "vpts.csv"
CSV_ENCODING = "utf8"  # !! Don't change, only utf-8 is accepted in data packages
CSV_FIELD_DELIMITER = ","

def check_vp_odim(source_odim: ODIMReader) -> None:
    """Verify ODIM file is an hdf5 ODIM format containing 'VP' data."""
    if not {"what", "how", "where"}.issubset(source_odim.hdf5.keys()):
        raise InvalidSourceODIM(
            "No hdf5 ODIM format: File does not contain what/how/where "
            "group information."
        )
    if source_odim.root_object_str != "VP":
        raise InvalidSourceODIM(
            f"Incorrect what.object value: expected VP, "
            f"found {source_odim.root_object_str}"
        )


def datetime_to_proper8601(timestamp):
    """Convert datetime to ISO8601 standard

    Parameters
    ----------
    timestamp : datetime.datetime
        Datetime

    Notes
    -----
    See https://stackoverflow.com/questions/19654578/python-utc-datetime-\
    objects-iso-format-doesnt-include-z-zulu-or-zero-offset
    """
    return str(timestamp).replace("+00:00", "Z")


def _get_variables(dataset, variable_mapping: dict,
                   quantity: str, convert_to_bool: bool = False) -> List[Any]:
    """In a given dataset, find the requested quantity and return a 1d list
    of the values

    'nodata' and 'undetect' are interpreted according to the metadata in the
    'what' group if convert_to_bool is true, 1 will be converted to True and
    0 to False

    Notes
    -----
    In order to handle the 'nodata' and 'undetect', a list overcomes casting as is done
    when using numpy in this case (and the non exsitence of Nan for integer in numpy).
    """
    data_group = variable_mapping[quantity]

    nodata_val = dataset[data_group]["what"].attrs["nodata"]
    undetect_val = dataset[data_group]["what"].attrs["undetect"]

    values = [entry[0] for entry in dataset[data_group]["data"]]
    values = [NODATA if value == nodata_val else value for value in values]
    values = [UNDETECT if value == undetect_val else value for value in values]

    if convert_to_bool:
        values = [True if value == 1 else False for value in values]

    return values


"""
# TODO - abstract away the version (not possible based on frictionless specs alone)
# elements to abstract:
# variables(&mapping&datatypes&constraints), metadata(&mapping), order, nodata/undetect-handling, CSV-dialect
class AbstractVptsCsv(ABC):

    nodata = "NA"
    undetect = "NaN"

    @abstractmethod
    def variables():
        ...

    @abstractmethod
    def metadata():
        ...

    @abstractmethod
    def order_columns():
        ...

class VptsCsvV1(AbstractVptsCsv):
    ...
"""


@dataclass
class MetaData:
    rcs: float
    sd_vvp_threshold: float
    vcp: int
    radar_longitude: float
    radar_latitude: float
    radar_height: int
    radar_wavelength: float

    @classmethod
    def from_odim(cls, source_odim: ODIMReader):
        return cls(
            rcs=source_odim.how["rcs_bird"],
            sd_vvp_threshold=source_odim.how["sd_vvp_thresh"],
            vcp=source_odim.how["vcp"],
            radar_longitude=str(np.round(source_odim.where["lon"], 6)),
            radar_latitude=str(np.round(source_odim.where["lat"], 6)),
            radar_height=source_odim.where["height"],
            radar_wavelength=str(np.round(source_odim.how["wavelength"], 6)),
        )

@dataclass
class Level:
    height: float  # Coded as a 64-bit float in HDF5 file
    variables: dict = field(default_factory=dict)

    def __lt__(self, other):  # Allows sorting by height
        return self.height < other.height

# Data class representing a single input source file
# (=https://github.com/adokter/vol2bird/wiki/ODIM-bird-profile-format-specification)
# =single datetime, single radar, multiple altitudes,
# usual variables for each altitude: dd, ff, ...
# This object aims to stay as close as possible to the HDF5 file
# (no data simplification/loss at this stage)
@dataclass
class Profile:
    # example: {'WMO':'06477', 'NOD':'bewid', 'RAD':'BX41', 'PLC':'Wideumont'}
    identifiers: dict
    datetime: datetime
    metadata: MetaData
    levels: List[Level] = field(default_factory=list)

    def __lt__(self, other):  # Allows sorting by datetime
        return self.datetime < other.datetime

    def __str__(self):
        """"""
        return f"Bird profile: {self.datetime:%Y-%m-%d %H:%M} - {self.identifiers}"

    def __repr__(self):
        """"""
        return f"Bird profile: {self.datetime:%Y-%m-%d %H:%M} - {self.identifiers}"

    def to_dataframe(self, cast_dtype=False):
        """Access the profile data as a pandas DataFrame

        Parameters
        ----------
        cast_dtype : bool, default False
            When True, Pandas converts numeric data if possible.

        Notes
        -----
        When 'NaN' or 'NA' values are present inside a column, the object data type
        will be kept. Otherwise the difference between NaN and NA would be lost and
        this overcomes int to float conversion when Nans are available as no int NaN
        is supported by Pandas.
        """
        if cast_dtype:
            return pd.DataFrame(self.to_table())
        else:
            return pd.DataFrame(self.to_table(), dtype=object)

    def to_table(self, prepare_for_csv=True):
        """Return a list of dicts representing the content of the profile,
        such as::

            [
                { radar: "bejab", datetime: x, height: 0.0, ff: 8.23, ... },
                { radar: "bejab", datetime: x, height: 200.0, ff: 5.23, ...}
            ]

        The list is sorted by altitude. The datetime is obviously identical for all
        entries. If prepare_for_csv is True, data is transformed to fit the final
        CSV format (data types, ...)
        """
        rows = []

        for level in self.levels:
            rows.append(
                {
                    "radar": self.identifiers.get('NOD'),
                    "datetime": self.datetime,
                    "height": level.height,
                    **level.variables,
                    **self.metadata
                }
            )

        if prepare_for_csv:
            for i, row in enumerate(rows):
                rows[i]["datetime"] = datetime_to_proper8601(row["datetime"])
                rows[i]["height"] = int(row["height"])

        return rows

    @classmethod
    def from_odim(cls, source_odim: ODIMReader):
        dataset1 = source_odim.hdf5["dataset1"]
        variable_mapping = {
            value[f"/dataset1/{key}"]["what"].attrs["quantity"].decode("utf8"): key
            for key, value in dataset1.items() if key != "what"
            }
        height_values = _get_variables(dataset1, variable_mapping, quantity="HGHT")

        # Order matters according to specification
        variables_to_load = (
            {"name": "u", "odim_name": "u", "convert_to_bool": False},
            {"name": "v", "odim_name": "v", "convert_to_bool": False},
            {"name": "w", "odim_name": "w", "convert_to_bool": False},
            {"name": "ff", "odim_name": "ff", "convert_to_bool": False},
            {"name": "dd", "odim_name": "dd", "convert_to_bool": False},
            {"name": "sd_vvp", "odim_name": "sd_vvp", "convert_to_bool": False},
            {"name": "gap", "odim_name": "gap", "convert_to_bool": True},
            {"name": "eta", "odim_name": "eta", "convert_to_bool": False},
            {"name": "dens", "odim_name": "dens", "convert_to_bool": False},
            {"name": "dbz", "odim_name": "dbz", "convert_to_bool": False},
            {"name": "dbz_all", "odim_name": "DBZH", "convert_to_bool": False},
            {"name": "n", "odim_name": "n", "convert_to_bool": False},
            {"name": "n_dbz", "odim_name": "n_dbz", "convert_to_bool": False},
            {"name": "n_all", "odim_name": "n_all", "convert_to_bool": False},
            {"name": "n_dbz_all", "odim_name": "n_dbz_all", "convert_to_bool": False},
        )

        # extract metadata
        metadata = asdict(MetaData.from_odim(source_odim=source_odim))

        levels = []
        for i, height in enumerate(height_values):
            levels.append(
                Level(
                    height=height,
                    variables={
                        k["name"]: _get_variables(
                            dataset1,
                            variable_mapping,
                            quantity=k["odim_name"],
                            convert_to_bool=k["convert_to_bool"],
                        )[i]
                        for k in variables_to_load
                    },
                )
            )

        return cls(
            datetime=source_odim.root_datetime,
            identifiers=source_odim.root_source,
            metadata=metadata,
            levels=sorted(levels),
        )


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


def vpts(file_paths):
    """Convert set of h5 files to a DataFrame

    Examples
    --------
    >>> file_paths = sorted(Path("../data/raw/baltrad/").rglob("*.h5"))
    >>> vpts(file_paths)
    """
    vpts = []
    for path_h5 in file_paths:
        with ODIMReader(path_h5) as odim_vp:
            vp = Profile.from_odim(odim_vp)
        vpts.append(vp.to_dataframe())
    vpts = pd.concat(vpts)
    # TODO - check if conversion required to sort
    return vpts.sort_values(by=["radar", "datetime", "height"])

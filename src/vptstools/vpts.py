import csv
import json
from pathlib import Path
import functools
import multiprocessing
from datetime import datetime
from typing import List, Any
from abc import ABC, abstractmethod
from dataclasses import dataclass, field

import pandas as pd
import numpy as np

from vptstools.odimh5 import ODIMReader, InvalidSourceODIM

NODATA = ""
UNDETECT = "NaN"

DESCRIPTOR_FILENAME = "datapackage.json"
CSV_ENCODING = "utf8"  # !! Don't change, only utf-8 is accepted in data packages
CSV_FIELD_DELIMITER = ","

class VptsCsvVersionError(Exception):
    """Raised when non supported VPTS version is asked"""
    pass


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
    return timestamp.strftime('%Y-%m-%dT%H:%M:%SZ')


def _odim_get_variables(dataset, variable_mapping: dict, quantity: str) -> List[Any]:
    """In a given dataset, find the requested quantity and return a 1d list
    of the values

    'nodata' and 'undetect' are interpreted according to the metadata in the
    'what' group.

    Parameters
    ----------
    dataset : HDF5 group
        HDF5 group
    variable_mapping : dict
        Map the variables names to the dataset ID in the ODIM file
    quantity : str
        Variable name to extract

    Notes
    -----
    In order to handle the 'nodata' and 'undetect', a list overcomes casting as is done
    when using numpy in this case (and the non exsitence of Nan for integer in numpy).
    """
    # TODO - check with Peter what to do with the gain/offset
    data_group = variable_mapping[quantity]

    nodata_val = dataset[data_group]["what"].attrs["nodata"]
    undetect_val = dataset[data_group]["what"].attrs["undetect"]

    values = [entry[0] for entry in dataset[data_group]["data"]]
    values = [NODATA if value == nodata_val else value for value in values]
    values = [UNDETECT if value == undetect_val else value for value in values]

    return values


class AbstractVptsCsv(ABC):
    """Abstract class to define VPTS CSV conversion rules"""

    @property
    @abstractmethod
    def nodata(self) -> str:
        """'No data' representation"""
        return ""

    @property
    @abstractmethod
    def undetect(self) -> str:
        """'Undetect' representation"""
        return "NaN"

    @property
    def level_name(self) -> str:
        """Column name of the level/height data column"""
        return "height"

    @property
    def mapping(self) -> dict:
        """Variable names that require renaming from ODIM -> standard"""
        return {}

    @property
    def sort(self) -> dict:
        """Columns to define row order"""
        return {"radar" : str, "datetime": str, "height": int}

    @property
    def variables(self) -> list:
        """Variables to extract from ODIM level data"""
        return ["u", "v", "w", "ff", "dd", "sd_vvp", "gap", "eta",
                "dens", "dbz", "dbz_all", "n", "n_dbz", "n_all",
                "n_dbz_all"]

    @property
    def cast_to_bool(self) -> list:
        """Define the variables that need to be converted to TRUE/FALSE"""
        return ["gap"]

    @property
    def order(self) -> list:
        """Column order of the output table"""
        return ["radar", "datetime", self.level_name] + self.variables + \
            ["rcs", "sd_vvp_threshold", "vcp",
             "radar_longitude", "radar_latitude",
             "radar_height", "radar_wavelength"]

    def metadata(self, bird_profile) -> dict:
        """Metadata values to extract from ODIM returned as str values

        Parameters
        ----------
        bird_profile : BirdProfile
            BirdProfile to extract metadata from.

        Notes
        -----
        "datetime" is required to merge with the level data.
        """
        timestamp = datetime_to_proper8601(bird_profile.datetime)
        return dict(
            datetime=timestamp,
            radar=bird_profile.identifiers.get('NOD'),
            rcs=str(bird_profile.how["rcs_bird"]),
            sd_vvp_threshold=str(bird_profile.how["sd_vvp_thresh"]),
            vcp=str(int(bird_profile.how["vcp"])),
            radar_longitude=str(np.round(bird_profile.where["lon"], 6)),
            radar_latitude=str(np.round(bird_profile.where["lat"], 6)),
            radar_height=str(int(bird_profile.where["height"])),
            radar_wavelength=str(np.round(bird_profile.how["wavelength"], 6)),
        )


class VptsCsvV1(AbstractVptsCsv):

    @property
    def nodata(self):
        return ""

    @property
    def undetect(self):
        return "NaN"

    @property
    def mapping(self):
        """Variables that require renaming from ODIM file -> VPTS CSV standard"""
        return {
            "DBZH" : "dbz_all"
        }


def _get_vpts_version(version: str):
    """Link version ID with correct class"""
    if version == "v1":
        return VptsCsvV1()
    else:
        raise VptsCsvVersionError(f"Version {version} not supported.")


@dataclass
class Level:
    height: float  # Coded as a 64-bit float in HDF5 file
    variables: dict = field(default_factory=dict)

    def __lt__(self, other):  # Allows sorting by height
        return self.height < other.height


@dataclass(frozen=True)
class BirdProfile:
    """Represent ODIM source file

    Data class representing a single input ODIM source file, i.e.
    (https://github.com/adokter/vol2bird/wiki/ODIM-bird-profile-format-specification)
    single datetime, single radar, multiple altitudes with
    variables for each altitude: dd, ff, ...

    This object aims to stay as close as possible to the HDF5 file
    (no data simplification/loss at this stage). Use the `from_odim` method
    as a convenient instantiation.
    """
    identifiers: dict # {'WMO':'06477', 'NOD':'bewid', 'RAD':'BX41', 'PLC':'Wideumont'}
    datetime: datetime
    what: dict
    where: dict
    how: dict
    levels: List[Level] = field(default_factory=list)

    def __lt__(self, other):  # Allows sorting by datetime
        return self.datetime < other.datetime

    def __str__(self):
        """"""
        return f"Bird profile: {self.datetime:%Y-%m-%d %H:%M} - {self.identifiers}"

    def __repr__(self):
        """"""
        return f"Bird profile: {self.datetime:%Y-%m-%d %H:%M} - {self.identifiers}"

    def to_vp(self, vpts_csv):
        """Convert profile data to a CSV

        Parameters
        ----------
        vpts_csv : AbstractVptsCsv
            Ruleset with the VPTS-CSV ruleset to use

        Notes
        -----
        When 'NaN' or 'NA' values are present inside a column, the object data type
        will be kept. Otherwise the difference between NaN and NA would be lost and
        this overcomes int to float conversion when Nans are available as no int NaN
        is supported by Pandas.
        """
        df = pd.DataFrame(self.data_table, dtype=str)

        # Add metadata as specified in standard
        metadata = pd.DataFrame([vpts_csv.metadata(self)]).astype(str)
        df = df.merge(metadata, on=["radar", "datetime"])

        # Adjust to standard column name mapping
        df = df.rename(columns={"height": vpts_csv.level_name, **vpts_csv.mapping})

        # Adjust to standard undetect/nodata data mapping
        # Workaround to specify columns with those value only;
        # otherwise other columns are casted to numbers
        #df[df.columns[(df == UNDETECT).any()]] = df[df.columns[(df == UNDETECT).any()]].replace(UNDETECT, vpts_csv.undetect)
        #df[df.columns[(df == NODATA).any()]] = df[df.columns[(df == NODATA).any()]].replace(NODATA, vpts_csv.nodata)
        df = df.replace({UNDETECT: vpts_csv.undetect, NODATA: vpts_csv.nodata})

        # Select relevant variables in order of standard
        df = df[vpts_csv.order]

        # Convert to (str representation of) boolean TRUE/FALSE
        for variable in vpts_csv.cast_to_bool:
            df[variable] = df[variable].replace({"1": "TRUE", "0": "FALSE"})

        # sort the data according to sorting rule
        df = df.astype(vpts_csv.sort).sort_values(by=list(vpts_csv.sort.keys())).astype(str)
        return df

    @functools.cached_property
    def data_table(self):
        """Return a list of dicts representing the content of the profile,
        such as::

            [
                { radar: "bejab", datetime: x, height: 0.0, ff: 8.23, ... },
                { radar: "bejab", datetime: x, height: 200.0, ff: 5.23, ...}
            ]

        The list is sorted by altitude. The datetime is obviously identical for all
        entries. The representaion is independent from the VPTS CSV representation
        """
        rows = []

        for level in self.levels:
            rows.append(
                {
                    "radar": self.identifiers.get('NOD'),
                    "datetime": self.datetime,
                    "height": level.height,
                    **level.variables
                }
            )

        for i, row in enumerate(rows):
            rows[i]["datetime"] = datetime_to_proper8601(row["datetime"])
            rows[i]["height"] = int(row["height"])

        return rows

    @classmethod
    def from_odim(cls, source_odim: ODIMReader):
        """Extract BirdProfile information from ODIM with OdimReader

        Parameters
        ----------
        source_odim : ODIMReader
            ODIM file reader interface.
        """
        dataset1 = source_odim.hdf5["dataset1"]
        variable_mapping = {
            value[f"/dataset1/{key}"]["what"].attrs["quantity"].decode("utf8"): key
            for key, value in dataset1.items() if key != "what"
            }
        height_values = _odim_get_variables(dataset1, variable_mapping, quantity="HGHT")

        levels = []
        for i, height in enumerate(height_values):
            levels.append(
                Level(
                    height=height,
                    variables={
                        variable: _odim_get_variables(
                            dataset1,
                            variable_mapping,
                            quantity=variable
                        )[i]
                        for variable in variable_mapping.keys()
                    },
                )
            )

        return cls(
            datetime=source_odim.root_datetime,
            identifiers=source_odim.root_source,
            what=dict(source_odim.what),
            where=dict(source_odim.where),
            how=dict(source_odim.how),
            levels=sorted(levels)
        )


def vp(file_path, vpts_csv_version="v1"):
    """Convert ODIM h5 file to a DataFrame

    Parameters
    ----------
    file_path : Path
        File Path of ODIM h5
    vpts_csv_version : str
        Ruleset with the VPTS-CSV ruleset to use

    Examples
    --------
    >>> file_path = Path("bejab_vp_20221111T233000Z_0x9.h5")
    >>> vp(file_path)
    """
    with ODIMReader(file_path) as odim_vp:
        vp = BirdProfile.from_odim(odim_vp)
    return vp.to_vp(_get_vpts_version(vpts_csv_version))


def vpts(file_paths, vpts_csv_version="v1"):
    """Convert set of h5 files to a DataFrame all as string

    Parameters
    ----------
    file_paths : Iterable of file paths
        Iterable of ODIM h5 file paths
    vpts_csv_version : str
        Ruleset with the VPTS-CSV ruleset to use

    Examples
    --------
    >>> file_paths = sorted(Path("../data/raw/baltrad/").rglob("*.h5"))
    >>> vpts(file_paths)
    """
    with multiprocessing.Pool(processes = (multiprocessing.cpu_count() - 1)) as pool:
        data = pool.map(functools.partial(vp, vpts_csv_version=vpts_csv_version), file_paths)
    # TODO - heights; requirement to have the same heights in each file
    # TODO - ask Peter -> ignore or Error?
    # TODO - constraints -> how to handle these? ignore or error?

    # TODO - add other consistency checks -- verify with Peter
    # - profile.radar_identifiers need to be the same; requirement to have same radar
    #
    vpts = pd.concat(data)

    # Remove duplicates by taking first, see https://github.com/enram/vptstools/issues/11
    vpts = vpts.drop_duplicates(subset=["radar", "datetime", "height"])

    # Convert according to defined ruleset
    vpts_csv = _get_vpts_version(vpts_csv_version)
    vpts = vpts.astype(vpts_csv.sort).sort_values(by=list(vpts_csv.sort.keys())).astype(str)
    return vpts


def vpts_to_csv(df, file_path, descriptor=False):
    """Write vp or vpts to file

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame with vp or vpts data
    file_path : Path | str
        File path to store the VPTS CSV file
    """
    # check for str input of Path
    if not isinstance(file_path, Path):
        file_path = Path(file_path)

    # create directory if not yet existing
    file_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(file_path, sep=CSV_FIELD_DELIMITER,
              encoding=CSV_ENCODING, index=False)
    if descriptor:
        _write_descriptor(file_path)


def _write_descriptor(vpts_file_path: Path):
    """"""
    # FROM - https://github.com/enram/vptstools/issues/10
    content = {
        "profile": "tabular-data-package",
        "resources": [
            {
            "name": "vpts",
            "path": vpts_file_path.name,
            "profile": "tabular-data-resource",
            "format": "csv",
            "mediatype": "text/csv",
            "encoding": CSV_ENCODING,
            "dialect": {"delimiter": CSV_FIELD_DELIMITER},
            "schema": "https://raw.githubusercontent.com/enram/vpts-csv/main/vpts-csv-table-schema.json"
            }
        ]
    }

    ## TODO - ask Peter - what to do with the descriptor format?
    ## # --existing code  ? we can have multiple radars? or a radar for each file?
    # content = {
    #     "radar": {
    #         "identifiers": source_metadata[
    #             "radar_identifiers"
    #         ]  # TODO: decide and docmuent what to do with that (in VPTS)
    #     },
    #     "temporal": {
    #         "start": datetime_to_proper8601(full_data_table[0]["datetime"]),
    #         "end": datetime_to_proper8601(full_data_table[-1]["datetime"]),
    #     },
    #     "resources": [
    #         {
    #             "name": "VPTS data",
    #             "path": CSV_FILENAME,
    #             "dialect": {"delimiter": CSV_FIELD_DELIMITER},
    #             "schema": {"fields": []},
    #         }
    #     ],
    # }
    vpts_file_path.parent.mkdir(parents=True, exist_ok=True)

    with open(vpts_file_path.parent / DESCRIPTOR_FILENAME, "w") as outfile:
        json.dump(content, outfile, indent=4, sort_keys=True)
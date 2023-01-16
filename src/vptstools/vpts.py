import csv
import json
from pathlib import Path
import functools
import multiprocessing
from datetime import datetime
from typing import List, Any
from abc import ABC, abstractmethod
from dataclasses import dataclass

import pandas as pd
import numpy as np

from vptstools.odimh5 import ODIMReader, InvalidSourceODIM

NODATA = ""
UNDETECT = "NaN"

DESCRIPTOR_FILENAME = "datapackage.json"
CSV_ENCODING = "utf8"  # !! Don't change, only utf-8 is accepted in data packages
CSV_FIELD_DELIMITER = ","

# TODO - make logical file split of the functionalit


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


def int_to_nodata(value, nodata_values, nodata=""):
    """"""
    if value in nodata_values:
        return nodata
    else:
        return int(value)


def number_to_bool_str(values):
    """"""
    to_bool = {1: "TRUE", 0: "FALSE"}
    return [to_bool[value] for value in values]


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
    # TODO - check with Adriaan what to do with the gain/offset
    data_group = variable_mapping[quantity]

    nodata_val = dataset[data_group]["what"].attrs["nodata"]
    undetect_val = dataset[data_group]["what"].attrs["undetect"]

    values = [entry[0] for entry in dataset[data_group]["data"]]
    values = [NODATA if value == nodata_val else value for value in values]
    values = [UNDETECT if value == undetect_val else value for value in values]

    return values


class AbstractVptsCsv(ABC):
    """Abstract class to define VPTS CSV conversion rules with a certain version"""

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
    @abstractmethod
    def sort(self) -> dict:
        """Columns to define row order

        The dict need to provide the column name in
        combination with the data type to use for
        the sorting, e.g.::

            dict(radar=str, datetime=str, height=int)

        As the data is returned as strings, casting to the data
        is done before sorting, after which the casting to str
        is applied again.
        """
        return dict()

    @abstractmethod
    def mapping(self) -> dict:
        """Translation from bird-profile to vtps CSV data standard.

        Data columns can be derived from the different attributes of the
        bird profile:

            - ``identifiers``: radar identification metadata
            - ``datetime``: the timestamp
            - ``levels``: the heights or levels of the measurement
            - ``variables``: the variables in the data (e.g. dd, ff, u,...)
            - ``how``: ODIM5 metadata
            - ``where``: ODIM5 metadata
            - ``what``: ODIM5 metadata

        An example of the dict to return::

            dict(
                radar=bird_profile.identifiers["NOD"],
                height=bird_profile.levels,
                u=bird_profile.variables["u"],
                v=bird_profile.variables["v"],
                vcp=int(bird_profile.how["vcp"])
            )

        As data is extracted as such additional helper functions can
        be added as well, e.g.::

            ...
            datetime=datetime_to_proper8601(bird_profile.datetime),
            gap=number_to_bool_str(bird_profile.variables["gap"]),
            radar_latitude=np.round(bird_profile.where["lat"], 6)
            ...

        Notes
        -----
        The order of the variables matter, as this defines the column
        order.
        """
        return dict()


class VptsCsvV1(AbstractVptsCsv):

    @property
    def nodata(self) -> str:
        """'No data' representation"""
        return ""

    @property
    def undetect(self) -> str:
        """'Undetect' representation"""
        return "NaN"

    @property
    def sort(self) -> dict:
        """Columns to define row order"""
        return dict(radar=str, datetime=str, height=int)

    def mapping(self, bird_profile):
        """Translation from bird-profile to vtps CSV data standard.

        Notes
        -----
        The order of the variables matter, as this defines the column
        order.
        """
        return dict(
            radar=bird_profile.identifiers["NOD"],
            datetime=datetime_to_proper8601(bird_profile.datetime),
            height=bird_profile.levels,
            u=bird_profile.variables["u"],
            v=bird_profile.variables["v"],
            w=bird_profile.variables["w"],
            ff=bird_profile.variables["ff"],
            dd=bird_profile.variables["dd"],
            sd_vvp=bird_profile.variables["sd_vvp"],
            gap=number_to_bool_str(bird_profile.variables["gap"]),
            eta=bird_profile.variables["eta"],
            dens=bird_profile.variables["dens"],
            dbz=bird_profile.variables["dbz"],
            dbz_all=bird_profile.variables["DBZH"],
            n=bird_profile.variables["n"],
            n_dbz=bird_profile.variables["n_dbz"],
            n_all=bird_profile.variables["n_all"],
            n_dbz_all=bird_profile.variables["n_dbz_all"],
            rcs=bird_profile.how["rcs_bird"],
            sd_vvp_threshold=bird_profile.how["sd_vvp_thresh"],
            vcp=int_to_nodata(bird_profile.how["vcp"], ["NULL", 0], self.nodata),
            radar_longitude=np.round(bird_profile.where["lon"], 6),
            radar_latitude=np.round(bird_profile.where["lat"], 6),
            radar_height=int(bird_profile.where["height"]),
            radar_wavelength=np.round(bird_profile.how["wavelength"], 6)
        )


def _get_vpts_version(version: str):
    """Link version ID with correct class"""
    if version == "v1":
        return VptsCsvV1()
    else:
        raise VptsCsvVersionError(f"Version {version} not supported.")


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
    levels: List[int]
    variables: dict

    def __lt__(self, other):  # Allows sorting by datetime
        return self.datetime < other.datetime

    def __str__(self):
        """"""
        return f"Bird profile: {self.datetime:%Y-%m-%d %H:%M} - {self.identifiers}"

    def __repr__(self):
        """"""
        return f"Bird profile: {self.datetime:%Y-%m-%d %H:%M} - {self.identifiers}"

    def to_vp(self, vpts_csv_version):
        """Convert profile data to a CSV

        Parameters
        ----------
        vpts_csv_version : AbstractVptsCsv
            Ruleset with the VPTS-CSV ruleset to use

        Notes
        -----
        When 'NaN' or 'NA' values are present inside a column, the object data type
        will be kept. Otherwise the difference between NaN and NA would be lost and
        this overcomes int to float conversion when Nans are available as no int NaN
        is supported by Pandas.
        """
        df = pd.DataFrame(vpts_csv_version.mapping(self), dtype=str)

        # Adjust to standard undetect/nodata data mapping
        # Workaround to specify columns with those value only;
        # otherwise other columns are casted to numbers
        #df[df.columns[(df == UNDETECT).any()]] = df[df.columns[(df == UNDETECT).any()]].replace(UNDETECT, vpts_csv.undetect)
        #df[df.columns[(df == NODATA).any()]] = df[df.columns[(df == NODATA).any()]].replace(NODATA, vpts_csv.nodata)
        df = df.replace({UNDETECT: vpts_csv_version.undetect, NODATA: vpts_csv_version.nodata})

        # sort the data according to sorting rule
        df = df.astype(vpts_csv_version.sort).sort_values(by=list(vpts_csv_version.sort.keys())).astype(str)

        return df

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

        variable_mapping.pop("HGHT")
        variables = dict()
        for variable in variable_mapping.keys():
            variables[variable] = _odim_get_variables(
                dataset1,
                variable_mapping,
                quantity=variable
                )

        return cls(
            datetime=source_odim.root_datetime,
            identifiers=source_odim.root_source,
            what=dict(source_odim.what),
            where=dict(source_odim.where),
            how=dict(source_odim.how),
            levels=[int(height) for height in height_values],
            variables=variables
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
    descriptor : bool
        Add additional frictionless metadata description
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
    vpts_file_path.parent.mkdir(parents=True, exist_ok=True)

    with open(vpts_file_path.parent / DESCRIPTOR_FILENAME, "w") as outfile:
        json.dump(content, outfile, indent=4, sort_keys=True)
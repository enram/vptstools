import re
import json
from pathlib import Path
import functools
import multiprocessing
from datetime import datetime
from typing import List, Any
from abc import ABC, abstractmethod
from dataclasses import dataclass
import tempfile

import pandas as pd
import numpy as np
from frictionless import validate

from vptstools.odimh5 import ODIMReader, InvalidSourceODIM

NODATA = ""
UNDETECT = "NaN"

DESCRIPTOR_FILENAME = "vpts.resource.json"
CSV_ENCODING = "utf8"  # !! Don't change, only utf-8 is accepted in frictionless resources
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


def check_source_file(source_file, regex):
    """Raise Exception when the source_file str is not according to the regex

    Parameters
    ----------
    source_file : str
        URL or path to the source file from which the data were derived.
    regex : str
        Regular expression to test the source_file against
    """
    sf_regex = re.compile(regex)
    if re.match(sf_regex, source_file):
        return source_file
    else:
        raise ValueError(
            f"Incorrect file description for the source_file."
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
    """Convert to integer or nodata value if enlisted

    Parameters
    ----------
    value : str | int | float
        Single data value
    nodata_values : list
        List of values in which case the data point need to be converted to ``nodata``
    nodata : str | float, default ""
        Data value to use when incoming value is one of the ``nodata_values``

    Returns
    -------
    str | int

    Examples
    --------
    >>> int_to_nodata('0', [0, 'NULL'], nodata="")
    ''
    >>> int_to_nodata('12', [0, 'NULL'], nodata="")
    12
    >>> int_to_nodata('NULL', [0, 'NULL'], nodata="")
    ''
    ""
    """
    if value in nodata_values:
        return nodata
    else:
        return int(value)


def number_to_bool_str(values):
    """Convert list of boolean values to str versions

    Parameters
    ----------
    values : list of bool
        List of Boolean values

    Returns
    -------
    list of str [TRUE, FALSE,...]
    """
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

    gain = dataset[data_group]["what"].attrs["gain"]
    offset = dataset[data_group]["what"].attrs["offset"]

    nodata_val = dataset[data_group]["what"].attrs["nodata"]
    undetect_val = dataset[data_group]["what"].attrs["undetect"]

    # Apply offset/gain while preserving the original variable datatype
    variable_dtype = dataset[data_group]["data"].dtype
    values = (dataset[data_group]["data"] * gain + offset).astype(variable_dtype).flatten().tolist()
    # use regular list here to have mixed dtypes for the data versus nodata/undetect
    values = [NODATA if value == nodata_val else value for value in values]
    values = [UNDETECT if value == undetect_val else value for value in values]
    return values


# TODO - change naming and put in logic module
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


class AbstractVptsCsv(ABC):
    """Abstract class to define VPTS CSV conversion rules with a certain version"""

    source_file_regex = ".*"

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

    source_file_regex = r"^(?=^[^.\/~])(^((?!\.{2}).)*$).*$"

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
        return dict(radar=str, datetime=str, height=int, source_file=str)

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
            radar_latitude=np.round(bird_profile.where["lat"], 6),
            radar_longitude=np.round(bird_profile.where["lon"], 6),
            radar_height=int(bird_profile.where["height"]),
            radar_wavelength=np.round(bird_profile.how["wavelength"], 6),
            source_file=check_source_file(bird_profile.source_file, self.source_file_regex)
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
    identifiers: dict  # {'WMO':'06477', 'NOD':'bewid', 'RAD':'BX41', 'PLC':'Wideumont'}
    datetime: datetime
    what: dict
    where: dict
    how: dict
    levels: List[int]
    variables: dict
    source_file: str = ""

    def __post_init__(self):
        if not isinstance(self.source_file, str):
            raise TypeError("Source_file need to be a str representation of a file path.")

    def __lt__(self, other):  # Allows sorting by datetime
        return self.datetime < other.datetime

    def __str__(self):
        return f"Bird profile: {self.datetime:%Y-%m-%d %H:%M} - {self.identifiers}"

    def __repr__(self):
        return f"Bird profile: {self.datetime:%Y-%m-%d %H:%M} - {self.identifiers}"

    def to_vp(self, vpts_csv_version):
        """Convert profile data to a CSV

        Parameters
        ----------
        vpts_csv_version : AbstractVptsCsv
            Ruleset with the VPTS-CSV ruleset to use, e.g. v1

        Notes
        -----
        When 'NaN' or 'NA' values are present inside a column, the object data type
        will be kept. Otherwise the difference between NaN and NA would be lost and
        this overcomes int to float conversion when Nans are available as no int NaN
        is supported by Pandas.
        """
        df = pd.DataFrame(vpts_csv_version.mapping(self), dtype=str)

        df = df.replace({UNDETECT: vpts_csv_version.undetect, NODATA: vpts_csv_version.nodata})

        # sort the data according to sorting rule
        df = df.astype(vpts_csv_version.sort).sort_values(by=list(vpts_csv_version.sort.keys())).astype(str)

        return df

    @classmethod
    def from_odim(cls, source_odim: ODIMReader, source_file=None):
        """Extract BirdProfile information from ODIM with OdimReader

        Parameters
        ----------
        source_odim : ODIMReader
            ODIM file reader interface.
        source_file : str, optional
            URL or path to the source file from which the data were derived.
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

        # Resolve hdf5 file full path if no source_file is provided by the user
        if not source_file:
            source_file = Path(source_odim.hdf5.filename).name

        return cls(
            datetime=source_odim.root_datetime,
            identifiers=source_odim.root_source,
            what=dict(source_odim.what),
            where=dict(source_odim.where),
            how=dict(source_odim.how),
            levels=[int(height) for height in height_values],
            variables=variables,
            source_file=str(source_file)
        )


def vp(file_path, vpts_csv_version="v1", source_file=""):
    """Convert ODIM h5 file to a DataFrame

    Parameters
    ----------
    file_path : Path
        File Path of ODIM h5
    vpts_csv_version : str, default ""
        Ruleset with the VPTS-CSV ruleset to use, e.g. v1
    source_file : str | callable
        URL or path to the source file from which the data were derived or
        a callable that converts the file_path to the source_file

    Examples
    --------
    >>> file_path = Path("bejab_vp_20221111T233000Z_0x9.h5")
    >>> vp(file_path)
    >>> vp(file_path, source_file="s3://aloft/baltrad/hdf5/2022/11/11/bejab_vp_20221111T233000Z_0x9.h5")

    Use file name itself as source_file representation in vp file using a custom callable function

    >>> vp(file_path, source_file=lambda x: Path(x).name)
    """
    # Convert file_path into source_file using callable
    if callable(source_file):
        source_file = source_file(file_path)

    with ODIMReader(file_path) as odim_vp:
        vp = BirdProfile.from_odim(odim_vp, source_file)
    return vp.to_vp(_get_vpts_version(vpts_csv_version))


def _convert_to_source(file_path):
    """Return the file name itself from a file path"""
    return Path(file_path).name


def vpts(file_paths, vpts_csv_version="v1", source_file=None):
    """Convert set of h5 files to a DataFrame all as string

    Parameters
    ----------
    file_paths : Iterable of file paths
        Iterable of ODIM h5 file paths
    vpts_csv_version : str
        Ruleset with the VPTS-CSV ruleset to use, e.g. v1
    source_file : callable, optional
        A callable that converts the file_path to the source_file. When None,
        the file name itself (without parent folder reference) is used.

    Notes
    -----
    Due tot the multiprocessing support, the source_file as a callable can not be a anonymous lambda function.

    Examples
    --------
    >>> file_paths = sorted(Path("../data/raw/baltrad/").rglob("*.h5"))
    >>> vpts(file_paths)

    Use file name itself as source_file representation in vp file using a custom callable function

    >>> def path_to_source(file_path):
    ...     return Path(file_path).name
    >>> vpts(file_paths, source_file=path_to_source)
    """
    # Use the file nam itself as source_file when no custom callable is provided
    if not source_file:
        source_file = _convert_to_source

    with multiprocessing.Pool(processes=(multiprocessing.cpu_count() - 1)) as pool:
        data = pool.map(functools.partial(vp, vpts_csv_version=vpts_csv_version,
                                          source_file=source_file), file_paths)

    vpts_ = pd.concat(data)

    # Convert according to defined rule set
    vpts_csv = _get_vpts_version(vpts_csv_version)
    vpts_ = vpts_.astype(vpts_csv.sort).sort_values(by=list(vpts_csv.sort.keys())).astype(str)
    return vpts_


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
        _write_resource_descriptor(file_path)


def validate_vpts(df, version="v1"):
    """Validate vpts DataFrame against the frictionless data schema and return report

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame as created by the vp or vpts functions
    version : str, v1 | v2 | ...


    Returns
    -------
    dict
        Frictionless validation report
    """
    tmp_path = Path(tempfile.mkdtemp())
    vpts_to_csv(df, tmp_path / "vpts.csv", descriptor=True)
    report = validate(tmp_path / DESCRIPTOR_FILENAME)
    return report


def _write_resource_descriptor(vpts_file_path: Path):
    """Write a frictionless resource descriptor file

    Parameters
    ----------
    vpts_file_path : pathlib.Path
        File path of the resource (vpts file) written to disk
    """
    content = {
        "name": "vpts",
        "path": vpts_file_path.name,
        "format": "csv",
        "mediatype": "text/csv",
        "encoding": CSV_ENCODING,
        "dialect": {"delimiter": CSV_FIELD_DELIMITER},
        "schema": "https://raw.githubusercontent.com/enram/vpts-csv/main/vpts-csv-table-schema.json"
    }
    vpts_file_path.parent.mkdir(parents=True, exist_ok=True)

    with open(vpts_file_path.parent / DESCRIPTOR_FILENAME, "w") as outfile:
        json.dump(content, outfile, indent=4, sort_keys=True)


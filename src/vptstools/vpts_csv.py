import re
from abc import ABC, abstractmethod

import numpy as np


class VptsCsvVersionError(Exception):
    """Raised when non supported VPTS CSV version is asked"""

    pass


"""
Collection of utility functions to support mapping conversions
"""


def datetime_to_proper8601(timestamp):
    """Convert datetime to ISO8601 standard

    Parameters
    ----------
    timestamp : datetime.datetime
        datetime to represent to ISO8601 standard.

    Notes
    -----
    See https://stackoverflow.com/questions/19654578/python-utc-datetime-\
    objects-iso-format-doesnt-include-z-zulu-or-zero-offset

    Examples
    --------
    >>> from datetime import datetime
    >>> datetime_to_proper8601(datetime(2021, 1, 1, 4, 0))
    '2021-01-01T04:00:00Z'
    """
    return timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")


def int_to_nodata(value, nodata_values, nodata=""):
    """Convert str to either integer or the corresponding nodata value if enlisted

    Parameters
    ----------
    value : str
        Single data value
    nodata_values : list of str
        List of values in which case the data point need to be converted to ``nodata``
    nodata : str | float, default ""
        Data value to use when incoming value is one of the ``nodata_values``

    Returns
    -------
    str | int

    Examples
    --------
    >>> int_to_nodata("0", ["0", 'NULL'], nodata="")
    ''
    >>> int_to_nodata("12", ["0", 'NULL'], nodata="")
    12
    >>> int_to_nodata('NULL', ["0", 'NULL'], nodata="")
    ''
    ""
    """
    if not isinstance(value, str):
        raise TypeError("Conversion with no-data check only supports str values.")
    if np.any([not isinstance(item, str) for item in nodata_values]):
        raise TypeError("Make sure to define the nodata_values as str.")
    if value in nodata_values:
        return nodata
    else:
        return int(value)


def number_to_bool_str(values):
    """Convert list of boolean values to str versions with capital letters

    Parameters
    ----------
    values : list of bool
        List of Boolean values

    Returns
    -------
    list of str [TRUE, FALSE,...]

    Examples
    --------
    >>> number_to_bool_str([True, False, False])
    ['TRUE', 'FALSE', 'FALSE']
    """
    to_bool = {1: "TRUE", 0: "FALSE"}
    return [to_bool[value] for value in values]


def check_source_file(source_file, regex):
    """Raise Exception when the source_file str is not according to the regex

    Parameters
    ----------
    source_file : str
        URL or path to the source file from which the data were derived.
    regex : str
        Regular expression to test the source_file against

    Returns
    -------
    source_file : str

    Raises
    ------
    ValueError : source_file not according to regex

    Examples
    --------
    >>> check_source_file("s3://alof/baltrad/2023/01/01/"
    ...                   "bejab_vp_20230101T000500Z_0x9.h5",
    ...                   r".*h5")
    's3://alof/baltrad/2023/01/01/bejab_vp_20230101T000500Z_0x9.h5'
    """
    sf_regex = re.compile(regex)
    if re.match(sf_regex, source_file):
        return source_file
    else:
        raise ValueError(
            "Incorrect file description for the source_file. Make sure "
            "the file path is not starting with '../' or '/'"
        )


"""
VPTS CSV version abstract version and individual version mapping implementations

To create a new version of the VPTS CSV implementation, create a new class `VptsCsvVX`
inherited from the `AbstractVptsCsv` class and provide the `abstractmethod`. See the
`mapping` method  for the conversion  functionality. Make sure to add the mapping
to the `_get_vpts_version` function
"""


def get_vpts_version(version: str):
    """Link version ID (v1, v2,..) with correct AbstractVptsCsv child class

    Parameters
    ----------
    version : str
        e.g. v1.0, v2.0,...

    Returns
    -------
    VptsCsvVx : child class of the AbstractVptsCsv

    Raises
    ------
    VptsCsvVersionError : Version of the VPTS CSV is not supported by an implementation
    """
    if version == "v1.0":
        return VptsCsvV1()
    else:
        raise VptsCsvVersionError(f"Version {version} not supported.")


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

            dict(radar=str, datetime=str, height=int, source_file=str)

        As the data is returned as strings, casting to the data
        is done before sorting, after which the casting to str
        is applied again.
        """
        return dict()

    @abstractmethod
    def mapping(self, bird_profile) -> dict:
        """Translation from ODIM bird profile to VPTS CSV data format.

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
        """Translation from ODIM bird profile to VPTS CSV data format.

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
            vcp=int_to_nodata(str(bird_profile.how["vcp"]), ["NULL", "0"], self.nodata),
            radar_latitude=np.round(bird_profile.where["lat"], 6),
            radar_longitude=np.round(bird_profile.where["lon"], 6),
            radar_height=int(bird_profile.where["height"]),
            radar_wavelength=np.round(bird_profile.how["wavelength"], 6),
            source_file=check_source_file(
                bird_profile.source_file, self.source_file_regex
            ),
        )

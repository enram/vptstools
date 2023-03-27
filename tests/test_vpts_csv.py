import pytest
import numpy as np

from vptstools.odimh5 import ODIMReader
from vptstools.vpts import BirdProfile
from vptstools.vpts_csv import (
    VptsCsvV1,
    VptsCsvVersionError,
    get_vpts_version,
    check_source_file,
    int_to_nodata,
    number_to_bool_str,
)


"""
IMPORTANT!

When creating a new version of the vpts specification class (VptsCsvVx), add this as additional vpts version
to the pytest parameterize decorators of these test classes to apply these tests to the new version as well. All
tests should be VptsCsv independent.
"""


@pytest.mark.parametrize("vpts_version", ["v1.0"])
class TestVptsVersionClass:
    """Test VPTS specification mapping classes"""

    def test_nodata_undetect_str(self, vpts_version):
        """vpts returns nodata/undetect as str representation"""
        vpts_spec = get_vpts_version(vpts_version)
        assert isinstance(vpts_spec.nodata, str)
        assert isinstance(vpts_spec.undetect, str)

    def test_sort_columns(self, vpts_version, path_with_vp):
        """vpts returns a non-empty dictionary to define the mapping wit names available in the mapping"""
        vpts_spec = get_vpts_version(vpts_version)
        assert isinstance(vpts_spec.sort, dict)
        # non-empty dict
        assert bool(vpts_spec.sort)
        # values define if it needs to defined as str, int or float
        assert set(vpts_spec.sort.values()).issubset([int, float, str])
        # sort keys should be part of the defined mapping as well
        with ODIMReader(next(path_with_vp.rglob("*.h5"))) as odim_vp:
            vp = BirdProfile.from_odim(odim_vp)
        mapping = vpts_spec.mapping(vp)
        assert set(vpts_spec.sort.keys()).issubset(mapping.keys())

    def test_mapping_dict(self, vpts_version, path_with_vp):
        """vpts returns a dictionary to translate specific variables"""
        vpts_spec = get_vpts_version(vpts_version)
        with ODIMReader(next(path_with_vp.rglob("*.h5"))) as odim_vp:
            vp = BirdProfile.from_odim(odim_vp)
        mapping = vpts_spec.mapping(vp)
        assert isinstance(mapping, dict)
        # dict values should not all be scalars but contain list.array as values as well (pd conversion support)
        assert np.array(
            [isinstance(value, (list, np.ndarray)) for value in mapping.values()]
        ).any()


class TestVptsVersionMapper:
    def test_version_mapper(self):
        """User defined version is mapped to correct class"""
        assert isinstance(get_vpts_version("v1.0"), VptsCsvV1)

    def test_version_non_existent(self):
        """Raise error when none-supported version is requested"""
        with pytest.raises(VptsCsvVersionError):
            get_vpts_version("v8555")


class TestVptsCsvV1SupportFun:
    def test_int_to_nodata(self):
        """Incoming str values are converted to nodata or converted to int"""
        assert int_to_nodata("0", ["0", "NULL"], nodata="") == ""
        assert int_to_nodata("NULL", ["0", "NULL"], nodata="") == ""
        assert int_to_nodata("12", ["0", "NULL"], nodata="") == 12

    def test_int_to_nodata_wrong_type(self):
        """int_to_nodata does not support non-str incoming values"""
        # check the incominb values
        with pytest.raises(TypeError):
            int_to_nodata(0, ["0", "NULL"], nodata="")
        # check the enisted nodata values
        with pytest.raises(TypeError):
            int_to_nodata("24", [0, "NULL"], nodata="")

    def test_number_to_bool_str(self):
        """Boolean values are mapped to TRUE/FALSE"""
        assert number_to_bool_str([True, False, False]) == ["TRUE", "FALSE", "FALSE"]

    def test_check_source_file(self):
        """Paths from directory, name only and a (windows) drive are valid"""
        assert check_source_file(
            "s3://alof/baltrad/2023/01/01/bejab_vp_20230101T000500Z_0x9.h5",
            VptsCsvV1.source_file_regex,
        )
        assert check_source_file(
            "bejab_vp_20230101T000500Z_0x9.h5", VptsCsvV1.source_file_regex
        )
        assert check_source_file(
            "C://bejab_vp_20230101T000500Z_0x9.h5", VptsCsvV1.source_file_regex
        )

    def test_check_source_file_wrong_file(self):
        """Path of V1 can not be relative or absolute-linux style"""
        with pytest.raises(ValueError):
            check_source_file("/dummy/path", VptsCsvV1.source_file_regex)
        with pytest.raises(ValueError):
            check_source_file("../dummy/relative/path", VptsCsvV1.source_file_regex)
        with pytest.raises(ValueError):
            check_source_file("./dummy/relative/path", VptsCsvV1.source_file_regex)

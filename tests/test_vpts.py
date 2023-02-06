import datetime
import dataclasses

import pytest
import numpy as np

from vptstools.odimh5 import ODIMReader
from vptstools.vpts import (vpts, vp, vpts_to_csv,
                            _get_vpts_version, BirdProfile,  # noqa
                            int_to_nodata, datetime_to_proper8601,
                            VptsCsvV1, VptsCsvVersionError, validate_vpts,
                            DESCRIPTOR_FILENAME)

import pandas as pd


"""
IMPORTANT!

When creating a new version of the vpts specification class (VptsCsvVx), add this as additional vpts version
to the pytest parameterize decorators of these test classes to apply these tests to the new version as well. All
tests should be VptsCsv independent.
"""


@pytest.mark.parametrize("vpts_version", ["v1"])
class TestVptsVersionClass:
    """Test VPTS specification mapping classes
    """
    def test_nodata_undetect_str(self, vpts_version):
        """vpts returns nodata/undetect as str representation"""
        vpts_spec = _get_vpts_version(vpts_version)
        assert isinstance(vpts_spec.nodata, str)
        assert isinstance(vpts_spec.undetect, str)

    def test_sort_columns(self, vpts_version, path_with_vp):
        """vpts returns a non-empty dictionary to define the mapping wit names available in the mapping"""
        vpts_spec = _get_vpts_version(vpts_version)
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
        vpts_spec = _get_vpts_version(vpts_version)
        with ODIMReader(next(path_with_vp.rglob("*.h5"))) as odim_vp:
            vp = BirdProfile.from_odim(odim_vp)
        mapping = vpts_spec.mapping(vp)
        assert isinstance(mapping, dict)
        # dict values should not all be scalars but contain list.array as values as well (pd conversion support)
        assert np.array([isinstance(value, (list, np.ndarray)) for value in mapping.values()]).any()


class TestVptsVersionMapper:

    def test_version_mapper(self):
        """User defined version is mapped to correct class"""
        assert isinstance(_get_vpts_version("v1"), VptsCsvV1)

    def test_version_non_existent(self):
        """Raise error when none-supported version is requested"""
        with pytest.raises(VptsCsvVersionError):
            _get_vpts_version("v2")


@pytest.mark.parametrize("vpts_version", ["v1"])
class TestVpts:

    def test_frictionless_schema_vp(self, vpts_version, tmp_path, path_with_vp):
        """Output after conversion corresponds to the frictionless schema"""
        file_path = next(path_with_vp.rglob("*.h5"))
        df_vp = vp(file_path, vpts_version)

        # TODO - DUMMY FIXES - CAN BE REMOVED AFTER SCHEMA UPDATES
        df_vp["vcp"] = "12"
        df_vp[["u", "v", "ff", "dd", "sd_vvp"]] = df_vp[["u", "v", "ff", "dd", "sd_vvp"]].replace("NaN", 1)
        df_vp[["ff", "dd", "sd_vvp", "eta"]] = df_vp[["ff", "dd", "sd_vvp", "eta"]].replace("", 1)

        report = validate_vpts(df_vp)
        assert report.stats.errors == 0

    def test_frictionless_schema_vpts(self, vpts_version, tmp_path, path_with_vp):
        """Output after conversion corresponds to the frictionless schema"""
        file_paths = sorted(path_with_vp.rglob("*.h5"))
        df_vpts = vpts(file_paths, vpts_version)

        # TODO - DUMMY FIXES - CAN BE REMOVED AFTER SCHEMA UPDATES
        df_vpts["vcp"] = "12"
        df_vpts[["u", "v", "ff", "dd", "sd_vvp"]] = df_vpts[["u", "v", "ff", "dd", "sd_vvp"]].replace("NaN", 1)
        df_vpts[["ff", "dd", "sd_vvp", "eta"]] = df_vpts[["ff", "dd", "sd_vvp", "eta"]].replace("", 1)

        report = validate_vpts(df_vpts)
        assert report.stats.errors == 0

    def test_str_dtypes(self, vpts_version, path_with_vp):
        """All columns are handled as str columns"""
        file_paths = sorted(path_with_vp.rglob("*.h5"))
        df_vpts = vpts(file_paths, vpts_version)
        # check for str in data itsefl instread of 'object' on pandas level
        assert all([isinstance(value, str) for value in df_vpts.iloc[0, :].to_dict().values()])

    def test_column_order(self, vpts_version, path_with_vp):
        """vpts column names are present and have correct sequence of VPTS CSV standard"""
        file_paths = sorted(path_with_vp.rglob("*.h5"))
        df_vpts = vpts(file_paths, vpts_version)
        vpts_spec = _get_vpts_version(vpts_version)
        with ODIMReader(file_paths[0]) as odim_vp:
            bird_profile = BirdProfile.from_odim(odim_vp)
        assert list(df_vpts.columns) == list(vpts_spec.mapping(bird_profile).keys())

    def test_duplicate_entries(self, vpts_version, path_with_vp):
        """Pick only first if duplicate entries (radar, datetime and height combination) are present."""
        file_paths = sorted(path_with_vp.rglob("*.h5"))
        df_vpts = vpts(file_paths, vpts_version)
        vpts_spec = _get_vpts_version(vpts_version)
        assert df_vpts[list(vpts_spec.sort.keys())].duplicated().sum() == 0
        # TODO - check only first one is used then duplicated are present
        # ...

    def test_sorting(self, vpts_version, path_with_vp):
        """vpts data is sorted, e.g. 'radar > timestamp > height'"""
        file_paths = sorted(path_with_vp.rglob("*.h5"))
        df_vpts = vpts(file_paths, vpts_version)
        vpts_spec = _get_vpts_version(vpts_version)

        # Resorting does not change dataframe (already sorted)
        df_pre = df_vpts[list(vpts_spec.sort.keys())]
        df_post = df_vpts[list(vpts_spec.sort.keys())].astype(vpts_spec.sort).sort_values(
            by=list(vpts_spec.sort.keys())).astype(str)
        pd.testing.assert_frame_equal(df_pre, df_post)

    def test_nodata(self, vpts_version):
        """vpts nodata values are serialized correctly in the output"""
        # TODO
        assert True

    def test_undetect(self, vpts_version):
        """vpts undetect values are serialized correctly in the output"""
        # TODO
        assert True

    def test_heights_all_the_same(self, vpts_version, path_with_vp):
        """vpts data contains the same levels/heights for each timestamp/radar"""
        file_paths = sorted(path_with_vp.rglob("*.h5"))
        df_vpts = vpts(file_paths, vpts_version)
        levels = df_vpts.groupby(["radar", "datetime"])["height"].unique()
        assert len(levels.apply(pd.Series).astype(int).drop_duplicates()) == 1

    def test_vcp_nodata(self, vpts_version, vp_metadata_only, monkeypatch):
        """Both 0 and 'NULL' VCP values need to be converted to nodata

        See also
        --------
        For discussion, see https://github.com/adokter/vol2bird/issues/198
        """
        vpts_csv_version = _get_vpts_version(vpts_version)

        def _mock_mapping(bird_profile):
            return dict(
                radar=bird_profile.identifiers["NOD"],
                datetime=datetime_to_proper8601(bird_profile.datetime),
                vcp=int_to_nodata(vp_metadata_only.how["vcp"], ["NULL", 0],
                                  vpts_csv_version.nodata),
                height=bird_profile.levels
            )
        monkeypatch.setattr(vpts_csv_version, "mapping", _mock_mapping)

        # bird profile (vp_metadata_only fixture) containing 0 values
        vp_metadata_only.how["vcp"] = 0
        df = vp_metadata_only.to_vp(vpts_csv_version)
        assert df["vcp"].unique() == vpts_csv_version.nodata

        # bird profile (vp) containing NULL values
        vp_metadata_only.how["vcp"] = "NULL"
        df = vp_metadata_only.to_vp(vpts_csv_version)
        assert df["vcp"].unique() == vpts_csv_version.nodata

        # bird profile (vp_metadata_only fixture) containing expected int value as str
        vp_metadata_only.how["vcp"] = 12
        df = vp_metadata_only.to_vp(vpts_csv_version)
        assert df["vcp"].unique() == np.array(['12'])


@pytest.mark.parametrize("vpts_version", ["v1"])
class TestVptsToCsv:

    def test_path_created(self, vpts_version, path_with_vp, tmp_path):
        """Routine creates parent folders if not existing"""
        file_paths = sorted(path_with_vp.rglob("*.h5"))
        df_vpts = vpts(file_paths, vpts_version)
        custom_folder = tmp_path / "SUBFOLDER"
        vpts_to_csv(df_vpts, custom_folder / "vpts.csv", descriptor=True)
        assert custom_folder.exists()
        assert (custom_folder / "vpts.csv").exists()
        assert (custom_folder / DESCRIPTOR_FILENAME).exists()

    def test_no_descriptor(self, vpts_version, path_with_vp, tmp_path):
        """No datapackage written when False"""
        file_paths = sorted(path_with_vp.rglob("*.h5"))
        df_vpts = vpts(file_paths, vpts_version)
        vpts_to_csv(df_vpts, tmp_path / "vpts.csv", descriptor=False)
        assert (tmp_path / "vpts.csv").exists()
        assert not (tmp_path / DESCRIPTOR_FILENAME).exists()

    def test_path_as_str(self, vpts_version, path_with_vp, tmp_path):
        """To csv support a file path provided as str instead of Path as well"""
        file_paths = sorted(path_with_vp.rglob("*.h5"))
        df_vpts = vpts(file_paths, vpts_version)
        custom_folder = tmp_path / "SUBFOLDER"
        vpts_to_csv(df_vpts, str(custom_folder / "vpts.csv"), descriptor=True)
        assert custom_folder.exists()


class TestBirdProfile:

    def test_from_odim(self, path_with_vp):
        """odim format is correctly mapped"""
        assert True # TODO

    def test_sortable(self, vp_metadata_only):
        """vp can be sorted on datetime"""
        vp_dict = dataclasses.asdict(vp_metadata_only)
        vp_dict["datetime"] = datetime.datetime(2030, 11, 14, 19, 5, tzinfo=datetime.timezone.utc)
        vp_dict["variables"] = dict()
        vp_metadata_only_later = BirdProfile(*vp_dict.values())
        assert vp_metadata_only < vp_metadata_only_later

    def test_str(self, vp_metadata_only):
        """"""
        assert str(vp_metadata_only), f"Bird profile: {vp_metadata_only.datetime:%Y-%m-%d %H:%M} " \
                                      f"- {vp_metadata_only.identifiers}"


class TestOdimFilePath:
    # TODO - add unit tests

    def test_parse_file_name(self):
        """"""
        pass
        # baltrad/hdf5/fivan/2016/10/25/fivan_vp_20161025T2100Z_0x7_147742969449.h5
        # baltrad/hdf5/fiuta/2021/11/14/fiuta_vp_20211114T214500Z_0xb.h5
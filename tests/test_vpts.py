import datetime
import dataclasses
from pathlib import Path

import pytest
import numpy as np

from vptstools.odimh5 import ODIMReader
from vptstools.vpts import (vpts, vp, vpts_to_csv,
                            _get_vpts_version, BirdProfile,  # noqa
                            int_to_nodata, datetime_to_proper8601,
                            VptsCsvV1, VptsCsvVersionError, validate_vpts,
                            DESCRIPTOR_FILENAME, OdimFilePath)

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


def _convert_to_source_dummy(file_path):
    """Return the file name itself from a file path"""
    return "DUMMY VALUE"


def _convert_to_source_s3(file_path):
    """Return the file name itself from a file path"""
    return OdimFilePath.from_file_name(file_path, source="baltrad").s3_url_h5("aloft")


@pytest.mark.parametrize("vpts_version", ["v1"])
class TestVpts:

    def test_frictionless_schema_vp(self, vpts_version, tmp_path, path_with_vp):
        """Output after conversion corresponds to the frictionless schema"""
        file_path = next(path_with_vp.rglob("*.h5"))
        df_vp = vp(file_path, vpts_version)

        report = validate_vpts(df_vp)
        assert report.stats.errors == 0

    def test_frictionless_schema_vpts(self, vpts_version, tmp_path, path_with_vp):
        """Output after conversion corresponds to the frictionless schema"""
        file_paths = sorted(path_with_vp.rglob("*.h5"))
        df_vpts = vpts(file_paths, vpts_version)

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
        """Keep duplicate entries (radar, datetime and height combination) are present."""
        file_paths = sorted(path_with_vp.rglob("*.h5"))
        df_vpts = vpts(file_paths, vpts_version)
        vpts_spec = _get_vpts_version(vpts_version)
        # remove source_file for duplicate test
        df_ = df_vpts[list(vpts_spec.sort.keys())].drop(columns="source_file")
        assert df_.duplicated().sum() == 75

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
                height=bird_profile.levels,
                source_file=bird_profile.source_file
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

    def test_vpts_no_source_file(self, vpts_version, path_with_vp):
        """The file name itself is used when no source_file reference is provided"""
        file_paths = sorted(path_with_vp.rglob("*.h5"))
        df_vpts = vpts(file_paths, vpts_version)
        assert set(df_vpts["source_file"].unique()) == set(file_path.name for file_path in file_paths)
        assert df_vpts.reset_index(drop=True)["source_file"][0] == "bejab_vp_20221111T233000Z_0x9.h5"

    def test_vpts_no_source_file(self, vpts_version, path_with_vp):
        """The source°file reference can be overwritten by a custom callable using the file_path as input"""
        file_paths = sorted(path_with_vp.rglob("*.h5"))
        # Use a function returning a dummy value for each
        df_vpts = vpts(file_paths, vpts_version, _convert_to_source_dummy)
        assert (df_vpts["source_file"] == "DUMMY VALUE").all()

        # Use a conversion to s3 function
        df_vpts = vpts(file_paths, vpts_version, _convert_to_source_s3)
        assert df_vpts["source_file"].str.startswith("s3://aloft/baltrad").all()

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

    def test_source_file(self, vp_metadata_only):
        """BirdProfile can be created with a source_file reference.

        No checks on the format are done (checks are only linked to a certain vpts-csv version when converting to vp).
        """
        vp_dict = dataclasses.asdict(vp_metadata_only)
        source_file = "s3://noaa-nexrad-level2/2016/09/01/KBGM/KBGM20160901_000212_V06.h5"
        vp_dict["source_file"] = source_file
        vp_with_source_file = BirdProfile(*vp_dict.values())
        assert vp_with_source_file.source_file == source_file
        assert isinstance(vp_with_source_file.source_file, str)

        source_file = "any/path/can/be/added"
        vp_dict["source_file"] = source_file
        vp_with_source_file = BirdProfile(*vp_dict.values())
        assert vp_with_source_file.source_file == source_file
        assert isinstance(vp_with_source_file.source_file, str)

        source_file = Path("./test.h5")
        vp_dict["source_file"] = str(source_file)
        vp_with_source_file = BirdProfile(*vp_dict.values())
        assert vp_with_source_file.source_file == str(source_file)
        assert isinstance(vp_with_source_file.source_file, str)

    def test_source_file_no_str(self, vp_metadata_only):
        """A TypeError is raised when the input source_file is not a str representation
        """
        vp_dict = dataclasses.asdict(vp_metadata_only)
        with pytest.raises(TypeError):
            vp_dict["source_file"] = Path("./test.h5")
            BirdProfile(*vp_dict.values())

    def test_source_file_from_odim(self, path_with_vp):
        """BirdProfile from ODIM with a user-defined source_file uses the custom path (without check)
        """
        file_paths = sorted(path_with_vp.rglob("*.h5"))
        source_file = "s3://custom_path/file.h5"
        with ODIMReader(file_paths[0]) as odim_vp:
            bird_profile = BirdProfile.from_odim(odim_vp, source_file)
        assert bird_profile.source_file == source_file
        assert isinstance(bird_profile.source_file, str)

    def test_source_file_none(self, vp_metadata_only):
        """BirdProfile can be created without a source_file resulting in an empty string"""
        assert vp_metadata_only.source_file == ""

    def test_source_file_none_from_odim(self, path_with_vp):
        """BirdProfile from ODIM without providing a source_file uses the file name
        of the ODIM path as source_file
        """
        file_paths = sorted(path_with_vp.rglob("*.h5"))
        current_path = file_paths[0]
        with ODIMReader(current_path) as odim_vp:
            bird_profile = BirdProfile.from_odim(odim_vp)
        assert bird_profile.source_file == str(current_path.name)


class TestOdimFilePath:
    # TODO - add unit tests

    def test_parse_file_name(self):
        """"""
        pass
        # baltrad/hdf5/fivan/2016/10/25/fivan_vp_20161025T2100Z_0x7_147742969449.h5
        # baltrad/hdf5/fiuta/2021/11/14/fiuta_vp_20211114T214500Z_0xb.h5
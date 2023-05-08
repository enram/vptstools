import datetime
import dataclasses
from pathlib import Path

import pytest
import numpy as np

from vptstools.odimh5 import ODIMReader, InvalidSourceODIM
from vptstools.vpts import (
    vpts,
    vp,
    vpts_to_csv,
    BirdProfile,
    validate_vpts,
    DESCRIPTOR_FILENAME,
    _convert_to_source,
)  # noqa
from vptstools.vpts_csv import (
    int_to_nodata,
    datetime_to_proper8601,
    get_vpts_version,
    AbstractVptsCsv,
)
from vptstools.s3 import OdimFilePath

import pandas as pd


"""
IMPORTANT!

When creating a new version of the vpts specification class (VptsCsvVx), add this as additional vpts version
to the pytest parameterize decorators of these test classes to apply these tests to the new version as well. All
tests should be VptsCsv independent.
"""


def _convert_to_source_dummy(file_path):
    """Return the file name itself from a file path"""
    return "DUMMY VALUE"


def _convert_to_source_s3(file_path):
    """Return the file name itself from a file path"""
    return OdimFilePath.from_file_name(file_path, source="baltrad").s3_url_h5("aloft")


@pytest.mark.parametrize("vpts_version", ["v1.0"])
class TestVpts:
    def test_frictionless_schema_vp(self, vpts_version, tmp_path, path_with_vp):
        """Output after conversion corresponds to the frictionless schema"""
        file_path = next(path_with_vp.rglob("*.h5"))
        df_vp = vp(file_path, vpts_version)

        report = validate_vpts(df_vp)
        assert report.stats["errors"] == 0

    def test_frictionless_schema_vpts(self, vpts_version, tmp_path, path_with_vp):
        """Output after conversion corresponds to the frictionless schema"""
        file_paths = sorted(path_with_vp.rglob("*.h5"))
        df_vpts = vpts(file_paths, vpts_version)

        report = validate_vpts(df_vpts)
        assert report.stats["errors"] == 0

    def test_str_dtypes(self, vpts_version, path_with_vp):
        """All columns are handled as str columns"""
        file_paths = sorted(path_with_vp.rglob("*.h5"))
        df_vpts = vpts(file_paths, vpts_version)
        # check for str in data itself is read of 'object' on Pandas level
        assert all(
            [isinstance(value, str) for value in df_vpts.iloc[0, :].to_dict().values()]
        )

    def test_column_order(self, vpts_version, path_with_vp):
        """vpts column names are present and have correct sequence of VPTS CSV standard"""
        file_paths = sorted(path_with_vp.rglob("*.h5"))
        df_vpts = vpts(file_paths, vpts_version)
        vpts_spec = get_vpts_version(vpts_version)
        with ODIMReader(file_paths[0]) as odim_vp:
            bird_profile = BirdProfile.from_odim(odim_vp)
        assert list(df_vpts.columns) == list(vpts_spec.mapping(bird_profile).keys())

    def test_duplicate_entries(self, vpts_version, path_with_vp):
        """Keep duplicate entries (radar, datetime and height combination) are present."""
        file_paths = sorted(path_with_vp.rglob("*.h5"))
        df_vpts = vpts(file_paths, vpts_version)
        vpts_spec = get_vpts_version(vpts_version)
        # remove source_file for duplicate test
        df_ = df_vpts[list(vpts_spec.sort.keys())].drop(columns="source_file")
        assert df_.duplicated().sum() == 75

    def test_sorting(self, vpts_version, path_with_vp):
        """vpts data is sorted, e.g. 'radar > timestamp > height'"""
        file_paths = sorted(path_with_vp.rglob("*.h5"))
        df_vpts = vpts(file_paths, vpts_version)
        vpts_spec = get_vpts_version(vpts_version)

        # Resorting does not change dataframe (already sorted)
        df_pre = df_vpts[list(vpts_spec.sort.keys())]
        df_post = (
            df_vpts[list(vpts_spec.sort.keys())]
            .astype(vpts_spec.sort)
            .sort_values(by=list(vpts_spec.sort.keys()))
            .astype(str)
        )
        pd.testing.assert_frame_equal(df_pre, df_post)

    def test_nodata(self, vpts_version, path_with_vp):
        """vpts nodata values are serialized correctly in the output"""
        file_path = sorted(path_with_vp.rglob("bewid*.h5"))[0]
        vpts_spec = get_vpts_version(vpts_version)
        with ODIMReader(file_path) as odim_vp:
            bird_profile = BirdProfile.from_odim(odim_vp, file_path.name)
            # raw data as saved in the hdf5 file
            dd_raw = np.array(
                odim_vp.hdf5["dataset1"]["data2"]["data"]
            ).flatten()  # data2 -> dd
            dd_nodata = odim_vp.hdf5["dataset1"]["data2"]["what"].attrs["nodata"]
        # values after conversion
        dd_vpts = bird_profile.to_vp(vpts_spec)["dd"]
        # nodata marked in vpts version need to be the same as nodata marked in raw hdf5 data
        assert ((dd_vpts == vpts_spec.nodata) == (dd_raw == dd_nodata)).all()

    def test_undetect(self, vpts_version, path_with_vp):
        """vpts undetect values are serialized correctly in the output"""
        file_path = sorted(path_with_vp.rglob("bejab*.h5"))[0]
        vpts_spec = get_vpts_version(vpts_version)
        with ODIMReader(file_path) as odim_vp:
            bird_profile = BirdProfile.from_odim(odim_vp, file_path.name)
            # raw data as saved in the hdf5 file
            ff_raw = np.array(
                odim_vp.hdf5["dataset1"]["data1"]["data"]
            ).flatten()  # data1 -> ff
            ff_undetect = odim_vp.hdf5["dataset1"]["data1"]["what"].attrs["undetect"]
        # values after conversion
        ff_vpts = bird_profile.to_vp(vpts_spec)["ff"]
        # undetect marked in vpts version need to be the same as undetect marked in raw hdf5 data
        assert ((ff_vpts == vpts_spec.undetect) == (ff_raw == ff_undetect)).all()

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
        vpts_csv_version = get_vpts_version(vpts_version)

        def _mock_mapping(bird_profile):
            return dict(
                radar=bird_profile.identifiers["NOD"],
                datetime=datetime_to_proper8601(bird_profile.datetime),
                vcp=int_to_nodata(
                    str(vp_metadata_only.how["vcp"]),
                    ["NULL", "0"],
                    vpts_csv_version.nodata,
                ),
                height=bird_profile.levels,
                source_file=bird_profile.source_file,
            )

        monkeypatch.setattr(vpts_csv_version, "mapping", _mock_mapping)

        # bird profile (vp_metadata_only fixture) containing 0 values (OdimReader reads as int)
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
        assert df["vcp"].unique() == np.array(["12"])

    def test_vp_no_source_file(self, vpts_version, path_with_vp):
        """The file name itself is used when no source_file reference is provided"""
        file_path = sorted(path_with_vp.rglob("*.h5"))[0]
        df_vp = vp(file_path, vpts_version)
        assert set(df_vp["source_file"].unique()) == set([file_path.name])
        assert (
            df_vp.reset_index(drop=True)["source_file"][0]
            == "bejab_vp_20221111T233000Z_0x9.h5"
        )

    def test_vp_custom_callable_file(self, vpts_version, path_with_vp):
        """The source file reference can be overwritten by a custom callable using the
        file_path as input"""
        file_path = sorted(path_with_vp.rglob("*.h5"))[0]
        # Use a function returning a dummy value for each
        df_vp = vp(file_path, vpts_version, _convert_to_source_dummy)
        assert (df_vp["source_file"] == "DUMMY VALUE").all()

        # Use a conversion to S3 function
        df_vp = vp(file_path, vpts_version, _convert_to_source_s3)
        assert df_vp["source_file"].str.startswith("s3://aloft/baltrad").all()

    def test_vpts_no_source_file(self, vpts_version, path_with_vp):
        """The file name itself is used when no source_file reference is provided"""
        file_paths = sorted(path_with_vp.rglob("*.h5"))
        df_vpts = vpts(file_paths, vpts_version)
        assert set(df_vpts["source_file"].unique()) == set(
            file_path.name for file_path in file_paths
        )
        assert (
            df_vpts.reset_index(drop=True)["source_file"][0]
            == "bejab_vp_20221111T233000Z_0x9.h5"
        )

    def test_vpts_convert_to_source(self, vpts_version):
        """Default helper function for filepath to file name returns name of path"""
        fname = _convert_to_source(Path("./odimh5/bewid_pvol_20170214T0000Z_0x1.h5"))
        assert fname == "bewid_pvol_20170214T0000Z_0x1.h5"

    def test_vpts_custom_callable_file(self, vpts_version, path_with_vp):
        """The source file reference can be overwritten by a custom callable using the
        file_path as input"""
        file_paths = sorted(path_with_vp.rglob("*.h5"))
        # Use a function returning a dummy value for each
        df_vpts = vpts(file_paths, vpts_version, _convert_to_source_dummy)
        assert (df_vpts["source_file"] == "DUMMY VALUE").all()

        # Use a conversion to S3 function
        df_vpts = vpts(file_paths, vpts_version, _convert_to_source_s3)
        assert df_vpts["source_file"].str.startswith("s3://aloft/baltrad").all()

    def test_vp_invalid_file(self, vpts_version, path_with_wrong_h5):  # noqa
        """Invalid h5 vp file raises InvalidSourceODIM exceptin"""
        with pytest.raises(InvalidSourceODIM):
            vp(path_with_wrong_h5 / "dummy.h5")

    def test_abstractmethoddefaults(self, vpts_version):
        """Test properties of abstract base class level"""
        AbstractVptsCsv.__abstractmethods__ = set()

        class Dummy(AbstractVptsCsv):
            """Dummn subclass"""

        vpts_dummy = Dummy()
        assert vpts_dummy.nodata == ""
        assert vpts_dummy.undetect == "NaN"
        assert vpts_dummy.sort == dict()
        assert vpts_dummy.mapping(dict()) == dict()


@pytest.mark.parametrize("vpts_version", ["v1.0"])
class TestVptsToCsv:
    def test_path_created(self, vpts_version, path_with_vp, tmp_path):
        """Routine creates parent folders if not existing"""
        file_paths = sorted(path_with_vp.rglob("*.h5"))
        df_vpts = vpts(file_paths, vpts_version)
        custom_folder = tmp_path / "SUBFOLDER"
        vpts_to_csv(df_vpts, custom_folder / "vpts.csv")
        assert custom_folder.exists()
        assert (custom_folder / "vpts.csv").exists()
        assert not (custom_folder / DESCRIPTOR_FILENAME).exists()

    def test_no_descriptor(self, vpts_version, path_with_vp, tmp_path):
        """No datapackage written when False"""
        file_paths = sorted(path_with_vp.rglob("*.h5"))
        df_vpts = vpts(file_paths, vpts_version)
        vpts_to_csv(df_vpts, tmp_path / "vpts.csv")
        assert (tmp_path / "vpts.csv").exists()
        assert not (tmp_path / DESCRIPTOR_FILENAME).exists()

    def test_path_as_str(self, vpts_version, path_with_vp, tmp_path):
        """To csv support a file path provided as str instead of Path as well"""
        file_paths = sorted(path_with_vp.rglob("*.h5"))
        df_vpts = vpts(file_paths, vpts_version)
        custom_folder = tmp_path / "SUBFOLDER"
        vpts_to_csv(df_vpts, str(custom_folder / "vpts.csv"))
        assert custom_folder.exists()


class TestBirdProfile:
    def test_from_odim(self, path_with_vp):
        """odim format is correctly mapped"""
        assert True  # TODO

    def test_sortable(self, vp_metadata_only):
        """vp can be sorted on datetime"""
        vp_dict = dataclasses.asdict(vp_metadata_only)
        vp_dict["datetime"] = datetime.datetime(
            2030, 11, 14, 19, 5, tzinfo=datetime.timezone.utc
        )
        vp_dict["variables"] = dict()
        vp_metadata_only_later = BirdProfile(*vp_dict.values())
        assert vp_metadata_only < vp_metadata_only_later

    def test_str(self, vp_metadata_only):
        """"""
        assert str(vp_metadata_only), (
            f"Bird profile: {vp_metadata_only.datetime:%Y-%m-%d %H:%M} "
            f"- {vp_metadata_only.identifiers}"
        )

    def test_source_file(self, vp_metadata_only):
        """BirdProfile can be created with a source_file reference.

        No checks on the format are done (checks are only linked to a certain vpts-csv version when converting to vp).
        """
        vp_dict = dataclasses.asdict(vp_metadata_only)
        source_file = (
            "s3://noaa-nexrad-level2/2016/09/01/KBGM/KBGM20160901_000212_V06.h5"
        )
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
        """A TypeError is raised when the input source_file is not a str representation"""
        vp_dict = dataclasses.asdict(vp_metadata_only)
        with pytest.raises(TypeError):
            vp_dict["source_file"] = Path("./test.h5")
            BirdProfile(*vp_dict.values())

    def test_source_file_from_odim(self, path_with_vp):
        """BirdProfile from ODIM with a user-defined source_file uses the custom path (without check)"""
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

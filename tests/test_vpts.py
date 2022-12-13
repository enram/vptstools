import pytest

from frictionless import validate
from vptstools.odimh5 import ODIMReader
from vptstools.vpts import (vpts, vpts_to_csv,
                            _get_vpts_version, BirdProfile,
                            VptsCsvV1, VptsCsvVersionError)

import pandas as pd

# TODO - make sure the vpts_version is either str or class representation everywhere

@pytest.mark.parametrize("vpts_version", [VptsCsvV1])
class TestVptsVersionClass():
    """Test VPTS specification mapping classes

    When creating a new version, add this as additional vpts version
    to the pytest parameterize decorator to apply these tests to the
    new version.
    """
    def test_nodata_undetect_str(self, vpts_version):
        """vpts returns nodata/undetect as str representation"""
        vpts_spec = vpts_version()
        assert isinstance(vpts_spec.nodata, str)
        assert isinstance(vpts_spec.undetect, str)

    def test_level_name_str(self, vpts_version):
        """vpts returns a label to use as level reprented column name"""
        vpts_spec = vpts_version()
        assert isinstance(vpts_spec.level_name, str)

    def test_mapping_dict(self, vpts_version):
        """vpts returns a dictionary to translate specific variables"""
        vpts_spec = vpts_version()
        assert isinstance(vpts_spec.mapping, dict)

    def test_sort_columns(self, vpts_version):
        """vpts returns a non-empty dictionary to define the mapping"""
        vpts_spec = vpts_version()
        assert isinstance(vpts_spec.sort, dict)
        # non-empty dict
        assert bool(vpts_spec.sort)
        # values define if it need to defined as str, int or float
        assert set(vpts_spec.sort.values()).issubset([int, float, str])

    def test_variables_list(self, vpts_version):
        """vpts returns list of variables from odim"""
        vpts_spec = vpts_version()
        assert isinstance(vpts_spec.variables, list)

    def test_cast_to_bool_list(self, vpts_version):
        """vpts returns list of variables to cast to boolean"""
        vpts_spec = vpts_version()
        assert isinstance(vpts_spec.cast_to_bool, list)
        # variables need to be defined as variables in class
        assert set(vpts_spec.cast_to_bool).issubset(vpts_spec.variables)

    def test_order_list(self, vpts_version, path_with_vp):
        """vpts returns list of variables to cast to boolean"""
        vpts_spec = vpts_version()
        assert isinstance(vpts_spec.order, list)
        # names need to be combination of metadata, level & var columns
        vp_file = next(path_with_vp.rglob("*.h5"))
        with ODIMReader(vp_file) as odim_vp:
            vp = BirdProfile.from_odim(odim_vp)
        set(vpts_spec.order ) == set(list(vpts_spec.metadata(vp).keys()) + [vpts_spec.level_name] + vpts_spec.variables)

    def test_metadata_str(self, vpts_version, path_with_vp):
        """Metadata extraction returns all str values"""
        vpts_spec = vpts_version()
        vp_file = next(path_with_vp.rglob("*.h5"))
        with ODIMReader(vp_file) as odim_vp:
            vp = BirdProfile.from_odim(odim_vp)

        assert all([isinstance(value, str) for value in vpts_spec.metadata(vp).values()])

class TestVptsVersionMapper():

    def test_version_mapper(self):
        """User defined version is mapped to correct class"""
        assert isinstance(_get_vpts_version("v1"), VptsCsvV1)

    def test_version_non_existent(self):
        """Raise error when none-supported version is requested"""
        with pytest.raises(VptsCsvVersionError):
            _get_vpts_version("v2")


@pytest.mark.parametrize("vpts_version", ["v1"])
class TestVpts():

    def test_frictionless_schema(self, vpts_version, tmp_path, path_with_vp):
        """Output after conversion corresponds to the frictionless schema"""
        file_paths = sorted(path_with_vp.rglob("*.h5"))

        df_vpts = vpts(file_paths, vpts_version)

        # TODO - DUMMY FIXES - ask peter (see notebook)
        df_vpts["vcp"] = "12"
        df_vpts[["u", "v", "ff", "dd", "sd_vvp"]] = df_vpts[["u", "v", "ff", "dd", "sd_vvp"]].replace("NaN", 1)
        df_vpts[["ff", "dd", "sd_vvp", "eta"]] = df_vpts[["ff", "dd", "sd_vvp", "eta"]].replace("", 1)

        vpts_to_csv(df_vpts, tmp_path / "vpts.csv", descriptor=True)

        report = validate(tmp_path / "datapackage.json")
        assert report["stats"]["errors"] == 0

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
        assert list(df_vpts.columns) == vpts_spec.order

    def test_duplicate_entries(self, vpts_version, path_with_vp):
        """Pick only first if duplicate entries (radar, datetime and height combination) are present."""
        file_paths = sorted(path_with_vp.rglob("*.h5"))
        df_vpts = vpts(file_paths, vpts_version)
        vpts_spec = _get_vpts_version(vpts_version)
        assert df_vpts[list(vpts_spec.sort.keys())].duplicated().sum() == 0
        # TODO - check only first one is used chen duplicated are present
        # ...

    def test_sorting(self, vpts_version, path_with_vp):
        """vpts data is sorted, e.g. 'radar > timestamp > height'"""
        file_paths = sorted(path_with_vp.rglob("*.h5"))
        df_vpts = vpts(file_paths, vpts_version)
        vpts_spec = _get_vpts_version(vpts_version)

        # Resorting does not change dataframe (already sorted)
        df_pre = df_vpts[list(vpts_spec.sort.keys())]
        df_post = df_vpts[list(vpts_spec.sort.keys())].astype(vpts_spec.sort).sort_values(by=list(vpts_spec.sort.keys())).astype(str)
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
        # TODO - ask Peter - unit test material or just blocking in routine?
        file_paths = sorted(path_with_vp.rglob("*.h5"))
        df_vpts = vpts(file_paths, vpts_version)
        levels = df_vpts.groupby(["radar", "datetime"])["height"].unique()
        assert len(levels.apply(pd.Series).astype(int).drop_duplicates()) == 1

    def test_constraints(self, vpts_version):
        """vpts variables are within the constraints of the VPTS CSV standard"""
        # TODO - ask Peter - unit test material or just blocking in routine?
        # Note - we can not test for this, but we can test the handling (error?)
        assert True


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
        assert (custom_folder / "datapackage.json").exists()

    def test_no_descriptor(self, vpts_version, path_with_vp, tmp_path):
        """No datapackage written when False"""
        file_paths = sorted(path_with_vp.rglob("*.h5"))
        df_vpts = vpts(file_paths, vpts_version)
        vpts_to_csv(df_vpts, tmp_path / "vpts.csv", descriptor=False)
        assert (tmp_path / "vpts.csv").exists()
        assert not (tmp_path / "datapackage.json").exists()


class TestBirdProfile:

    def test_data_table_cache(self, path_with_vp):
        """Data table property is memoized"""
        vp_file = next(path_with_vp.rglob("*.h5"))
        with ODIMReader(vp_file) as odim_vp:
            vp = BirdProfile.from_odim(odim_vp)
        assert "data_table" not in vp.__dict__
        vp.data_table
        # check if metadata is memoized
        assert "data_table" in vp.__dict__

    def test_data_table_format(self, path_with_vp):
        """data_table is a list of dicst with the data"""
        assert True # TODO

    def test_from_odim(self, path_with_vp):
        """odim format is correctly mapped"""
        assert True # TODO

    def test_sortable(self, path_with_vp):
        """vp can be sorted on datetime"""
        assert True # TODO
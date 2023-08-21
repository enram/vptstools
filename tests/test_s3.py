import os
import configparser
from unittest.mock import patch

import pytest
import pandas as pd

from vptstools.s3 import (
    OdimFilePath,
    handle_manifest,
    _handle_inventory,  # noqa
    extract_daily_group_from_inventory,
    list_manifest_file_keys,
    _last_modified_from_inventory,
)  # noqa


class TestOdimFilePath:
    @pytest.mark.parametrize(
        # radar_code, data_type, year, month, day, hour, minute, file_name
        "file_path,components",
        [
            (
                "baltrad/hdf5/fivan/2016/10/25/fivan_vp_20161025T2100Z_0x7_147742969449.h5",
                (
                    "fivan",
                    "vp",
                    "2016",
                    "10",
                    "25",
                    "21",
                    "00",
                    "fivan_vp_20161025T2100Z_0x7_147742969449.h5",
                ),
            ),
            (
                "baltrad/hdf5/fiuta/2021/11/14/fiuta_vp_20211114T214500Z_0xb.h5",
                (
                    "fiuta",
                    "vp",
                    "2021",
                    "11",
                    "14",
                    "21",
                    "45",
                    "fiuta_vp_20211114T214500Z_0xb.h5",
                ),
            ),
            (
                "baltrad/hdf5/seang/2017/01/20/seang_vp_20170120T2115Z_0xf3fc7b_148494821853.h5",
                (
                    "seang",
                    "vp",
                    "2017",
                    "01",
                    "20",
                    "21",
                    "15",
                    "seang_vp_20170120T2115Z_0xf3fc7b_148494821853.h5",
                ),
            ),
            (
                "baltrad/hdf5/searl/2016/12/31/searl_vp_20161231T2030Z_0x5_148321870475.h5",
                (
                    "searl",
                    "vp",
                    "2016",
                    "12",
                    "31",
                    "20",
                    "30",
                    "searl_vp_20161231T2030Z_0x5_148321870475.h5",
                ),
            ),
            (
                "baltrad/hdf5/plrze/2020/10/27/plrze_vp_20201027T172000Z_0x9.h5",
                (
                    "plrze",
                    "vp",
                    "2020",
                    "10",
                    "27",
                    "17",
                    "20",
                    "plrze_vp_20201027T172000Z_0x9.h5",
                ),
            ),
            (
                    "uva/hdf5/2008/02/15/NLDBL_vp_20080215T0000_NL50_v0-3-20.h5",
                    (
                            "nldbl",
                            "vp",
                            "2008",
                            "02",
                            "15",
                            "00",
                            "00",
                            "NLDBL_vp_20080215T0000_NL50_v0-3-20.h5",
                    ),
            ),
        ],
    )
    def test_parse_file_name(self, file_path, components):
        """File path components are correctly extracted from file path"""
        serialized = OdimFilePath.parse_file_name(file_path)
        assert serialized == components

    def test_parse_file_name_invalid(self):
        """File path components are correctly extracted from file path"""
        with pytest.raises(ValueError):
            OdimFilePath.parse_file_name("not a valid file name")

    @pytest.mark.parametrize(
        # radar_code, data_type, year, month, day, hour, minute, file_name
        "file_path,components",
        [
            (
                "fivan_vp_20161025T2100Z_0x7_147742969449.h5",
                (
                    "baltrad",
                    "fivan",
                    "vp",
                    "2016",
                    "10",
                    "25",
                    "21",
                    "00",
                    "fivan_vp_20161025T2100Z_0x7_147742969449.h5",
                    "",
                ),
            ),
            (
                "fiuta_vp_20211114T214500Z_0xb.h5",
                (
                    "baltrad",
                    "fiuta",
                    "vp",
                    "2021",
                    "11",
                    "14",
                    "21",
                    "45",
                    "fiuta_vp_20211114T214500Z_0xb.h5",
                    "",
                ),
            ),
            (
                "seang_vp_20170120T2115Z_0xf3fc7b_148494821853.h5",
                (
                    "baltrad",
                    "seang",
                    "vp",
                    "2017",
                    "01",
                    "20",
                    "21",
                    "15",
                    "seang_vp_20170120T2115Z_0xf3fc7b_148494821853.h5",
                    "",
                ),
            ),
            (
                "searl_vp_20161231T2030Z_0x5_148321870475.h5",
                (
                    "baltrad",
                    "searl",
                    "vp",
                    "2016",
                    "12",
                    "31",
                    "20",
                    "30",
                    "searl_vp_20161231T2030Z_0x5_148321870475.h5",
                    "",
                ),
            ),
            (
                "plrze_vp_20201027T172000Z_0x9.h5",
                (
                    "baltrad",
                    "plrze",
                    "vp",
                    "2020",
                    "10",
                    "27",
                    "17",
                    "20",
                    "plrze_vp_20201027T172000Z_0x9.h5",
                    "",
                ),
            ),
        ],
    )
    def test_parse_file_from_file_name(self, file_path, components):
        """OdimPath is correctly created based on incoming path from inventory"""
        odim_path = OdimFilePath.from_file_name(file_path, source="baltrad")
        assert odim_path == OdimFilePath(*components)

    @pytest.mark.parametrize(
        # radar_code, data_type, year, month, day, hour, minute, file_name
        "file_path,components",
        [
            (
                "baltrad/hdf5/fivan/2016/10/25/fivan_vp_20161025T2100Z_0x7_147742969449.h5",
                (
                    "baltrad",
                    "fivan",
                    "vp",
                    "2016",
                    "10",
                    "25",
                    "21",
                    "00",
                    "fivan_vp_20161025T2100Z_0x7_147742969449.h5",
                    "hdf5",
                ),
            ),
            (
                "baltrad/hdf5/fiuta/2021/11/14/fiuta_vp_20211114T214500Z_0xb.h5",
                (
                    "baltrad",
                    "fiuta",
                    "vp",
                    "2021",
                    "11",
                    "14",
                    "21",
                    "45",
                    "fiuta_vp_20211114T214500Z_0xb.h5",
                    "hdf5",
                ),
            ),
            (
                "baltrad/hdf5/seang/2017/01/20/seang_vp_20170120T2115Z_0xf3fc7b_148494821853.h5",
                (
                    "baltrad",
                    "seang",
                    "vp",
                    "2017",
                    "01",
                    "20",
                    "21",
                    "15",
                    "seang_vp_20170120T2115Z_0xf3fc7b_148494821853.h5",
                    "hdf5",
                ),
            ),
            (
                "baltrad/hdf5/searl/2016/12/31/searl_vp_20161231T2030Z_0x5_148321870475.h5",
                (
                    "baltrad",
                    "searl",
                    "vp",
                    "2016",
                    "12",
                    "31",
                    "20",
                    "30",
                    "searl_vp_20161231T2030Z_0x5_148321870475.h5",
                    "hdf5",
                ),
            ),
            (
                "baltrad/hdf5/plrze/2020/10/27/plrze_vp_20201027T172000Z_0x9.h5",
                (
                    "baltrad",
                    "plrze",
                    "vp",
                    "2020",
                    "10",
                    "27",
                    "17",
                    "20",
                    "plrze_vp_20201027T172000Z_0x9.h5",
                    "hdf5",
                ),
            ),
            (
                "ecog-04003/hdf5/bgvar/2016/10/02/bgvar_vp_20161002T2345Z.h5",
                (
                    "ecog-04003",
                    "bgvar",
                    "vp",
                    "2016",
                    "10",
                    "02",
                    "23",
                    "45",
                    "bgvar_vp_20161002T2345Z.h5",
                    "hdf5",
                ),
            ),
        ],
    )
    def test_parse_file_from_inventory(self, file_path, components):
        """OdimPath is correctly created based on incoming path from inventory"""
        odim_path = OdimFilePath.from_inventory(file_path)
        assert odim_path == OdimFilePath(*components)

        # Test the file path properties and methods
        (
            source,
            radar_code,
            file_type,
            year,
            month,
            day,
            hour,
            minute,
            file_name,
            file_type,
        ) = components
        assert (
            odim_path.daily_vpts_file_name
            == f"{radar_code}_vpts_{year}{month}{day}.csv"
        )
        assert (
            odim_path.s3_path_setup(file_type)
            == f"{source}/{file_type}/{radar_code}/{year}"
        )
        bucket = "aloft"
        assert (
            odim_path.s3_url_h5(bucket)
            == f"s3://{bucket}/{source}/{file_type}/{radar_code}/"
            f"{year}/{month}/{day}/{file_name}"
        )
        assert (
            odim_path.s3_folder_path_h5
            == f"{source}/{file_type}/{radar_code}/{year}/{month}/{day}"
        )
        assert (
            odim_path.s3_file_path_daily_vpts == f"{source}/daily/{radar_code}/{year}/"
            f"{radar_code}_vpts_{year}{month}{day}.csv"
        )
        assert (
            odim_path.s3_file_path_monthly_vpts
            == f"{source}/monthly/{radar_code}/{year}/"
            f"{radar_code}_vpts_{year}{month}.csv.gz"
        )

    @pytest.mark.parametrize(
        # radar_code, data_type, year, month, day, hour, minute, file_name
        "file_path,components",
        [
            ("fivan_vp_20161025T2100Z_0x7_147742969449.h5", ("fi", "van")),
            ("fiuta_vp_20211114T214500Z_0xb.h5", ("fi", "uta")),
            ("seang_vp_20170120T2115Z_0xf3fc7b_148494821853.h5", ("se", "ang")),
            ("searl_vp_20161231T2030Z_0x5_148321870475.h5", ("se", "arl")),
            ("plrze_vp_20201027T172000Z_0x9.h5", ("pl", "rze")),
        ],
    )
    def test_radar_code(self, file_path, components):
        """Radar and country are correctly creating the radar_code"""
        odim_path = OdimFilePath.from_file_name(file_path, source="baltrad")
        assert odim_path.country == components[0]
        assert odim_path.radar == components[1]

    @pytest.mark.parametrize(
        # radar_code, data_type, year, month, day, hour, minute, file_name
        "file_path,components",
        [
            (
                "baltrad/hdf5/fivan/2016/10/25/fivan_vp_20161025T2100Z_0x7_147742969449.h5",
                ("baltrad", "vp", "fivan", "2016", "10", "25"),
            ),
            (
                "baltrad/hdf5/fiuta/2021/11/14/fiuta_vp_20211114T214500Z_0xb.h5",
                ("baltrad", "vp", "fiuta", "2021", "11", "14"),
            ),
            (
                "baltrad/hdf5/seang/2017/01/20/seang_vp_20170120T2115Z_0xf3fc7b_148494821853.h5",
                ("baltrad", "vp", "seang", "2017", "01", "20"),
            ),
            (
                "baltrad/hdf5/searl/2016/12/31/searl_vp_20161231T2030Z_0x5_148321870475.h5",
                ("baltrad", "vp", "searl", "2016", "12", "31"),
            ),
            (
                "baltrad/hdf5/plrze/2020/10/27/plrze_vp_20201027T172000Z_0x9.h5",
                ("baltrad", "vp", "plrze", "2020", "10", "27"),
            ),
        ],
    )
    def test_extract_coverage_group_from_s3_inventory(self, file_path, components):
        """combination of source, type, radar_code and date are used
        as grouping level for coverage/daily-vpts"""
        assert extract_daily_group_from_inventory

    def test_last_modified_from_manifest_subfile(self):
        """Manifest records are correctly filtered on modified date"""
        # create a dataframe with a record for each of the last 10 days
        df_ = pd.DataFrame(
            {
                "modified": pd.date_range(
                    pd.Timestamp.now(tz="utc") - pd.Timedelta("10days"),
                    periods=10,
                    tz="utc",
                )
                + pd.Timedelta("2hours")
            }
        )
        assert _last_modified_from_inventory(df_, "2days").shape[0] == 2
        assert _last_modified_from_inventory(df_, "5days").shape[0] == 5
        assert _last_modified_from_inventory(df_, "10days").shape[0] == 10
        assert _last_modified_from_inventory(df_, "30days").shape[0] == 10


class TestHandleManifest:
    """S3 manifest/inventory is translated into coverage and
    overview of days to update daily/monthly files
    """

    # Resulting coverage file for the prepared unit test example inventory (tests/data/dummy_inventor.csv.gz)
    df_result = pd.DataFrame(
        {
            "directory": [
                ("baltrad", "hdf5", "fiuta", "2021", "04", "23"),
                ("baltrad", "hdf5", "fiuta", "2021", "04", "24"),
                ("baltrad", "hdf5", "nosta", "2023", "03", "11"),
                ("baltrad", "hdf5", "nosta", "2023", "03", "12"),
                ("ecog-04003", "hdf5", "plpoz", "2016", "09", "23"),
            ],
            "file_count": [1, 1, 4, 1, 2],
        }
    )

    def test_list_manifest_file_keys(self, s3_inventory):
        """Individual inventory items are correctly parsed from manifest file"""
        inventory_files = list(
            list_manifest_file_keys(
                "aloft-inventory/aloft/aloft-hdf5-files-inventory/2023-02-01T01-00Z/manifest.json"
            )
        )
        assert len(inventory_files) == 1
        assert (
            inventory_files[0]["key"] == "aloft/aloft-hdf5-files-inventory/data/"
            "dummy_inventory.csv.gz"
        )

    def test_list_manifest_file_keys_with_profile(self, s3_inventory, tmp_path):
        """Individual inventory items are correctly parsed from manifest file"""
        # register and define custom AWS profile
        custom_crd = tmp_path / "credentials"
        os.environ["AWS_SHARED_CREDENTIALS_FILE"] = str(custom_crd)
        with open(custom_crd, "w") as cred:
            config = configparser.ConfigParser()
            config["my-aws-profile"] = {}
            config["my-aws-profile"]["aws_access_key_id"] = "DUMMY"
            config["my-aws-profile"]["aws_secret_access_key"] = "DUMMY"
            config.write(cred)

        # run inventory with alternative profile
        inventory_files = list(
            list_manifest_file_keys(
                "aloft-inventory/aloft/aloft-hdf5-files-inventory/2023-02-01T01-00Z/manifest.json",
                storage_options={"profile": "my-aws-profile"},
            )
        )
        assert len(inventory_files) == 1
        assert (
            inventory_files[0]["key"] == "aloft/aloft-hdf5-files-inventory/data/"
            "dummy_inventory.csv.gz"
        )
        # clean up env variable
        del os.environ["AWS_SHARED_CREDENTIALS_FILE"]

    def test_handle_manifest_all(self, s3_inventory):
        """e2e test for the manifest/inventory handling functionality - all included"""
        # All inventory items within time window
        with patch(
            "pandas.Timestamp.now",
            return_value=pd.Timestamp("2023-02-01 00:00:00", tz="UTC"),
        ):
            df_cov, days_to_create_vpts = handle_manifest(
                "s3://aloft-inventory/aloft/aloft-hdf5-files-inventory/2023-02-01T01-00Z/manifest.json",
                modified_days_ago="60days",
            )  # large enough number to get all inventory 'modified' items
            # When date-modified implies full scan, df_cov and days_to_create_vpts are the same
            pd.testing.assert_frame_equal(self.df_result, df_cov)
            pd.testing.assert_frame_equal(df_cov, days_to_create_vpts)
            assert (
                set(days_to_create_vpts.columns)
                == set(["directory", "file_count"])
                == set(df_cov.columns)
            )

    def test_handle_manifest_subset(self, s3_inventory):
        """e2e test for the manifest/inventory handling functionality - subset within time window"""

        df_result = self.df_result.iloc[[1, 2, 4], :].reset_index(drop=True)

        # Subset of inventory items within time-window
        with patch(
            "pandas.Timestamp.now",
            return_value=pd.Timestamp("2023-02-01 00:00:00", tz="UTC"),
        ):
            df_cov, days_to_create_vpts = handle_manifest(
                "s3://aloft-inventory/aloft/aloft-hdf5-files-inventory/2023-02-01T01-00Z/manifest.json",
                modified_days_ago="5days",
            )  # only subset of files is within the time window of  days
            # Coverage returns the full inventory overview
            pd.testing.assert_frame_equal(self.df_result, df_cov)
            # Days to update only keeps modified files within time frame
            pd.testing.assert_frame_equal(df_result, days_to_create_vpts)
            assert (
                set(days_to_create_vpts.columns)
                == set(["directory", "file_count"])
                == set(df_cov.columns)
            )

    def test_handle_manifest_none(self, s3_inventory):
        """e2e test for the manifest/inventory handling functionality - no data within"""

        # Subset of inventory items within time-window
        with patch(
            "pandas.Timestamp.now",
            return_value=pd.Timestamp("2023-03-01 00:00:00", tz="UTC"),
        ):
            df_cov, days_to_create_vpts = handle_manifest(
                "s3://aloft-inventory/aloft/aloft-hdf5-files-inventory/2023-02-01T01-00Z/manifest.json",
                modified_days_ago="1days",
            )  # only subset of files is within the time window of  days
            # Coverage returns the full inventory overview
            pd.testing.assert_frame_equal(self.df_result, df_cov)
            # days_to_create_vpts returns empty DataFrame
            assert days_to_create_vpts.empty
            assert (
                set(days_to_create_vpts.columns)
                == set(["directory", "file_count"])
                == set(df_cov.columns)
            )

    def test_handle_inventory_alternative_suffix(self):
        """Only h5 files are included in the inventory handling, other extensions ignored"""
        df_inventory = pd.DataFrame(
            [
                {
                    "repo": "aloft",
                    "file": "baltrad/coverage.csv",
                    "size": 1,
                    "modified": pd.Timestamp("2023-01-31 00:00:00+0000", tz="UTC"),
                },
                {
                    "repo": "aloft",
                    "file": "baltrad/inventory.csv.gz",
                    "size": 1,
                    "modified": pd.Timestamp("2023-01-31 00:00:00+0000", tz="UTC"),
                },
                {
                    "repo": "aloft",
                    "file": "baltrad/manifest.json",
                    "size": 1,
                    "modified": pd.Timestamp("2023-01-31 00:00:00+0000", tz="UTC"),
                },
                {
                    "repo": "aloft",
                    "file": "baltrad/14azd6.checksum",
                    "size": 1,
                    "modified": pd.Timestamp("2023-01-31 00:00:00+0000", tz="UTC"),
                },
            ]
        )

        df_cov, days_to_create_vpts = _handle_inventory(
            df_inventory, modified_days_ago="50days"
        )
        assert df_cov.empty
        assert days_to_create_vpts.empty

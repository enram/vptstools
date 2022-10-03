import os

import pytest

from odimh5.reader import ODIMReader

CURRENT_SCRIPT_DIR = os.path.dirname(__file__)


def test_open_and_expose_hdf5():
    """ODIMReader can open a file, and then expose a hdf5 attribute"""
    odim = ODIMReader(
        os.path.join(
            CURRENT_SCRIPT_DIR, "./sample_files/bewid_pvol_20170214T0000Z_0x1.h5"
        )
    )
    assert hasattr(odim, "hdf5")


def test_with_statement():
    """ODIMReader also works with the 'with' statement"""
    with ODIMReader(
        os.path.join(
            CURRENT_SCRIPT_DIR, "./sample_files/bewid_pvol_20170214T0000Z_0x1.h5"
        )
    ) as odim:
        assert hasattr(odim, "hdf5")


def test_root_date_str():
    """The root_date_str property can be used to get the root date"""
    with ODIMReader(
        os.path.join(
            CURRENT_SCRIPT_DIR, "./sample_files/bewid_pvol_20170214T0000Z_0x1.h5"
        )
    ) as odim:
        assert odim.root_date_str == "20170214"


def test_root_datetime():
    with ODIMReader(
        os.path.join(
            CURRENT_SCRIPT_DIR, "./sample_files/bewid_pvol_20170214T0000Z_0x1.h5"
        )
    ) as odim:
        dt = odim.root_datetime

        assert dt.year == 2017
        assert dt.month == 2
        assert dt.day == 14
        assert dt.hour == 0
        assert dt.minute == 0
        assert dt.second == 16
        assert dt.microsecond == 0
        assert dt.utcoffset().total_seconds() == 0  # in UTC


def test_root_time_str():
    """The root_time_str property can be used to get the root time"""
    with ODIMReader(
        os.path.join(
            CURRENT_SCRIPT_DIR, "./sample_files/bewid_pvol_20170214T0000Z_0x1.h5"
        )
    ) as odim:
        assert odim.root_time_str == "000016"


def test_root_source_str():
    """The root_source_str property can be used to get the root source as a string"""
    with ODIMReader(
        os.path.join(
            CURRENT_SCRIPT_DIR, "./sample_files/bewid_pvol_20170214T0000Z_0x1.h5"
        )
    ) as odim:
        assert (
            odim.root_source_str
            == "WMO:06477,RAD:BX41,PLC:Wideumont,NOD:bewid,CTY:605,CMT:VolumeScanZ"
        )


def test_root_object_str():
    """The root_object_str property can be used to get the root object as a string"""
    with ODIMReader(
        os.path.join(
            CURRENT_SCRIPT_DIR, "./sample_files/bewid_pvol_20170214T0000Z_0x1.h5"
        )
    ) as odim:
        assert odim.root_object_str == "PVOL"


def test_root_source_dict():
    """The root_source property can be used to get the root source as a dict"""
    with ODIMReader(
        os.path.join(
            CURRENT_SCRIPT_DIR, "./sample_files/bewid_pvol_20170214T0000Z_0x1.h5"
        )
    ) as odim:
        assert odim.root_source == {
            "WMO": "06477",
            "RAD": "BX41",
            "PLC": "Wideumont",
            "NOD": "bewid",
            "CTY": "605",
            "CMT": "VolumeScanZ",
        }


def test_close():
    """There's a close method, HDF5 file cannot be accessed after use"""
    odim = ODIMReader(
        os.path.join(
            CURRENT_SCRIPT_DIR, "./sample_files/bewid_pvol_20170214T0000Z_0x1.h5"
        )
    )
    assert odim.hdf5.mode == "r"
    odim.close()
    with pytest.raises(ValueError):
        odim.hdf5.mode


def test_datasets():
    odim = ODIMReader(
        os.path.join(
            CURRENT_SCRIPT_DIR, "./sample_files/bewid_pvol_20170214T0000Z_0x1.h5"
        )
    )

    datasets = odim.dataset_names
    assert len(datasets) == 11
    assert "dataset1" in datasets
    assert "dataset11" in datasets
    assert not "dataset12" in datasets

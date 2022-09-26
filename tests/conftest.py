import os
from pathlib import Path

import pytest


CURRENT_DIR = Path(os.path.dirname(__file__))
SAMPlE_DATA_DIR = CURRENT_DIR / "data"


@pytest.fixture
def path_with_vp():
    """Return the folder containing minimal unit test files"""
    return SAMPlE_DATA_DIR / "vp"


@pytest.fixture
def path_with_wrong_h5():
    """Return the folder containing wrong - not ODIM -  hdf5 file"""
    return SAMPlE_DATA_DIR / "vp_no_odim_h5"


@pytest.fixture
def path_with_pvol():
    """Return the folder containing wrong ODIM hdf5 file (pvol)"""
    return SAMPlE_DATA_DIR / "vp_no_odim_h5"

from __future__ import annotations

from datetime import datetime
from typing import Dict, List

import h5py  # type: ignore
import pytz


class ODIMReader(object):
    """Simple class to read ODIM (HDF5) files

    - Can be used with the "with" statement
    """

    def __enter__(self) -> ODIMReader:
        return self

    def __init__(self, path: str):
        """Open the ODIM file

        Raises: OSError: Unable to open file
        """
        self.hdf5 = h5py.File(path, mode="r")

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _extract_root_attribute_str(self, group: str, attribute: str) -> str:
        return self.hdf5[group].attrs.get(attribute).decode("utf-8")

    @property
    def dataset_names(self) -> List[str]:
        """Get a list of all the dataset elements (names, as str)"""
        keys = list(self.hdf5)
        return [key for key in keys if "dataset" in key]

    @property
    def root_date_str(self) -> str:
        """Get the root what.date attribute as a string, format 'YYYYMMDD'"""
        return self._extract_root_attribute_str("what", "date")

    @property
    def root_time_str(self) -> str:
        """Get the root what.time attribute as a string, format 'HHMMSS' (UTC)"""
        return self._extract_root_attribute_str("what", "time")

    @property
    def root_datetime(self) -> datetime:
        """Get the root date and time as a proper aware datetime object"""
        return datetime.strptime(
            f"{self.root_date_str}{self.root_time_str}", "%Y%m%d%H%M%S"
        ).replace(tzinfo=pytz.UTC)

    @property
    def root_source_str(self) -> str:
        """Get the root what.source attribute as a string.

        Example: WMO:06477,RAD:BX41,PLC:Wideumont,NOD:bewid,CTY:605,CMT:VolumeScanZ
        """
        return self._extract_root_attribute_str("what", "source")

    @property
    def root_source(self) -> Dict[str, str]:
        """Get the root what.source attribute as a dict.

        Example: {'WMO':'06477', 'NOD':'bewid', 'RAD':'BX41', 'PLC':'Wideumont'}
        """
        string = self.root_source_str
        kv_pairs = string.split(",")
        r = {}
        for kv_pair in kv_pairs:
            k, v = kv_pair.split(":")
            r[k] = v

        return r

    @property
    def root_object_str(self) -> str:
        """Get the root what.object attribute as a string.

        Possible values according to the standard:
            - "PVOL" (Polar volume)
            - "CVOL" (Cartesian volume)
            - "SCAN" (Polar scan)
            - "RAY" (Single polar ray)
            - "AZIM" (Azimuthal object)
            - "ELEV" (Elevational object)
            - "IMAGE" (2-D cartesian image)
            - "COMP" (Cartesian composite image(s))
            - "XSEC" (2-D vertical cross section(s))
            - "VP" (1-D vertical profile)
            - "PIC" (Embedded graphical image)
        """
        return self._extract_root_attribute_str("what", "object")

    def close(self) -> None:
        self.hdf5.close()

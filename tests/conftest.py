import os
import datetime
from pathlib import Path
from typing import Callable, Any
from unittest.mock import MagicMock, patch

import pytest
import boto3
import aiobotocore.awsrequest
import aiobotocore.endpoint
import aiohttp
import aiohttp.client_reqrep
import aiohttp.typedefs
import botocore.awsrequest
import botocore.model

from moto import mock_s3, mock_sns, mock_sqs
from moto.core import DEFAULT_ACCOUNT_ID

from vptstools.vpts import BirdProfile


CURRENT_DIR = Path(os.path.dirname(__file__))
SAMPlE_DATA_DIR = CURRENT_DIR / "data"


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-1"


@pytest.fixture(autouse=True)
def mock_settings_env_vars(aws_credentials):
    """Define the default environmental variables for the unit tests"""
    env_names_to_remove = {"AWS_PROFILE"}
    modified_environ = {k: v for k, v in os.environ.items() if k not in env_names_to_remove}
    modified_environ["SNS_TOPIC"] = "arn:aws:sns:eu-west-1:123456789012:dummysnstopic"
    modified_environ["AWS_REGION"] = "eu-west-1"
    with patch.dict(os.environ, modified_environ, clear=True):
        yield


# Patch as described in https://github.com/aio-libs/aiobotocore/issues/755#issuecomment-1424945194 --------------------
class MockAWSResponse(aiobotocore.awsrequest.AioAWSResponse):
    """
    Mocked AWS Response.

    https://github.com/aio-libs/aiobotocore/issues/755
    https://gist.github.com/giles-betteromics/12e68b88e261402fbe31c2e918ea4168
    """

    def __init__(self, response: botocore.awsrequest.AWSResponse):
        self._moto_response = response
        self.status_code = response.status_code
        self.raw = MockHttpClientResponse(response)

    # adapt async methods to use moto's response
    async def _content_prop(self) -> bytes:
        return self._moto_response.content

    async def _text_prop(self) -> str:
        return self._moto_response.text


class MockHttpClientResponse(aiohttp.client_reqrep.ClientResponse):
    """
    Mocked HTP Response.

    See <MockAWSResponse> Notes
    """

    def __init__(self, response: botocore.awsrequest.AWSResponse):
        """
        Mocked Response Init.
        """

        async def read(self: MockHttpClientResponse, n: int = -1) -> bytes:
            return response.content

        self.content = MagicMock(aiohttp.StreamReader)
        self.content.read = read
        self.response = response

    @property
    def raw_headers(self) -> Any:
        """
        Return the headers encoded the way that aiobotocore expects them.
        """
        return {
            k.encode("utf-8"): str(v).encode("utf-8")
            for k, v in self.response.headers.items()
        }.items()


@pytest.fixture(scope="session", autouse=True)
def patch_aiobotocore() -> None:
    """
    Pytest Fixture Supporting S3FS Mocks.

    See <MockAWSResponse> Notes
    """

    def factory(original: Callable[[Any, Any], Any]) -> Callable[[Any, Any], Any]:
        """
        Response Conversion Factory.
        """

        def patched_convert_to_response_dict(
            http_response: botocore.awsrequest.AWSResponse,
            operation_model: botocore.model.OperationModel,
        ) -> Any:
            return original(MockAWSResponse(http_response), operation_model)

        return patched_convert_to_response_dict

    aiobotocore.endpoint.convert_to_response_dict = factory(
        aiobotocore.endpoint.convert_to_response_dict
    )


# ----------------------------------------------------------------------------------------------------------------------


@pytest.fixture
def path_with_vp():
    """Return the folder containing minimal unit test files"""
    return SAMPlE_DATA_DIR / "vp"


@pytest.fixture
def path_with_wrong_h5():
    """Return the folder containing wrong - not ODIM - hdf5 file"""
    return SAMPlE_DATA_DIR / "vp_no_odim_h5"


@pytest.fixture
def path_with_pvol():
    """Return the folder containing wrong ODIM hdf5 file (pvol)"""
    return SAMPlE_DATA_DIR / "vp_no_odim_h5"


@pytest.fixture
def file_path_pvol():
    """Return minimal unit test file of pvol ODIM5"""
    return SAMPlE_DATA_DIR / "odimh5" / "bewid_pvol_20170214T0000Z_0x1.h5"


@pytest.fixture
def vp_metadata_only():
    """"""
    return BirdProfile(
        identifiers={"WMO": "14024", "RAD": "SI41", "PLC": "Lisca", "NOD": "silis"},
        datetime=datetime.datetime(2022, 11, 14, 19, 5, tzinfo=datetime.timezone.utc),
        what={
            "date": b"20221114",
            "object": b"VP",
            "source": b"WMO:14024,RAD:SI41,PLC:Lisca,NOD:silis",
            "time": b"190500",
            "version": b"H5rad 2.4",
        },
        where={
            "height": 950.0,
            "interval": 200.0,
            "lat": 46.06776997447014,
            "levels": 25,
            "lon": 15.28489999473095,
            "maxheight": 5000.0,
            "minheight": 0.0,
        },
        how={
            "beamwidth": 1.200000029057264,
            "clutterMap": b"",
            "comment": b"",
            "dealiased": 1,
            "enddate": b"20221114",
            "endtime": b"190959",
            "filename_pvol": b"/opt/opera/projects/vol2bird_scripts/data/"
            b"out/merged/silis_pvol_20221114T190500Z_0xb.h5",
            "filename_vp": b"/opt/opera/projects/vol2bird_scripts/data/out/silis_vp_20221114T190500Z_0xb.h5",
            "maxazim": 360.0,
            "maxrange": 35.0,
            "minazim": 0.0,
            "minrange": 5.0,
            "rcs_bird": 11.0,
            "sd_vvp_thresh": 2.0,
            "software": b"BALTRAD",
            "startdate": b"20221114",
            "starttime": b"190507",
            "task": b"vol2bird",
            "task_args": b"azimMax=360.000000,azimMin=0.000000,layerThickness=200.000000,"
            b"nLayers=25,rangeMax=35000.000000,rangeMin=5000.000000,"
            b"elevMax=90.000000,elevMin=0.000000,radarWavelength=5.300000,"
            b"useClutterMap=0,clutterMap=,fitVrad=1,exportBirdProfileAsJSONVar=0,"
            b"minNyquist=5.000000,maxNyquistDealias=25.000000,"
            b"birdRadarCrossSection=11.000000,stdDevMinBird=2.000000,"
            b"cellEtaMin=11500.000000,etaMax=36000.000000,dbzType=DBZH,"
            b"requireVrad=0,dealiasVrad=1,dealiasRecycle=1,dualPol=0,singlePol=1,"
            b"rhohvThresMin=0.950000,resample=0,resampleRscale=500.000000,"
            b"resampleNbins=100,resampleNrays=360,mistNetNElevs=5,mistNetElevsOnly=1,"
            b"useMistNet=0,mistNetPath=/MistNet/mistnet_nexrad.pt,"
            b"areaCellMin=0.500000,cellClutterFractionMax=0.500000,chisqMin=0.000010,"
            b"clutterValueMin=0.100000,dbzThresMin=0.000000,fringeDist=5000.000000,"
            b"nBinsGap=8,nPointsIncludedMin=25,nNeighborsMin=5,nObsGapMin=5,"
            b"nAzimNeighborhood=3,nRangNeighborhood=3,nCountMin=4,"
            b"refracIndex=0.964000,cellStdDevMax=5.000000,absVDifMax=10.000000,"
            b"vradMin=1.000000",
            "task_version": b"0.5.0.9187",
            "vcp": 0,
            "wavelength": 5.300000190734863,
        },
        levels=[
            0,
            200,
            400,
            600,
            800,
            1000,
            1200,
            1400,
            1600,
            1800,
            2000,
            2200,
            2400,
            2600,
            2800,
            3000,
            3200,
            3400,
            3600,
            3800,
            4000,
            4200,
            4400,
            4600,
            4800,
        ],
        variables={},  # empty variables
    )


@pytest.fixture
def path_inventory():
    """Return the folder containing minimal unit test files"""
    return SAMPlE_DATA_DIR / "inventory"


@pytest.fixture(scope="function")
def s3_inventory(aws_credentials, path_inventory):
    """Mocked AWS S3 inventory bucket with a manifest json example file included

    The example inventory file contains the following hdf5 files:
    source - radar_code - date - count   -> last modified
    baltrad - fiuta  - 2021 04 23 - 1    -> 2023-01-01
    baltrad - fiuta  - 2021 04 24 - 1    -> 2023-01-28
    baltrad - nosta - 2023 03 11 - 4     -> 2023-01-31
    baltrad - nosta - 2023 03 12 - 1     -> 2023-01-01
    ecog-04003 - plpoz - 2016 09 23 - 2  -> 2023-01-28
    """
    manifest = path_inventory / "dummy_manifest.json"
    inventory = path_inventory / "dummy_inventory.csv.gz"

    with mock_s3():
        s3 = boto3.client("s3")
        # Add S3 inventory setup
        s3.create_bucket(
            Bucket="aloft-inventory",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
        )
        with open(manifest, "rb") as manifest_file:
            s3.upload_fileobj(
                manifest_file,
                "aloft-inventory",
                "aloft/aloft-hdf5-files-inventory/2023-02-01T01-00Z/manifest.json",
            )
        with open(inventory, "rb") as inventory_file:
            s3.upload_fileobj(
                inventory_file,
                "aloft-inventory",
                "aloft/aloft-hdf5-files-inventory/data/dummy_inventory.csv.gz",
            )

        # Add example data to aloft mocked S3 bucket
        s3.create_bucket(
            Bucket="aloft",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
        )
        for h5file in (path_inventory / "vp").glob("*.h5"):
            with open(h5file, "rb") as h5f:
                s3.upload_fileobj(
                    h5f, "aloft", f"baltrad/hdf5/nosta/2023/03/11/{h5file.name}"
                )
        yield s3


@pytest.fixture(scope="function")
def sns(aws_credentials):
    """"""
    with mock_sns():
        with mock_sqs():
            sns_client = boto3.client("sns")
            sns_client.create_topic(Name=os.environ["SNS_TOPIC"].split(":")[-1])

            # sqs_conn = boto3.resource("sqs")
            # sqs_conn.create_queue(QueueName="test-queue")

            sns_client.subscribe(
                TopicArn=os.environ["SNS_TOPIC"],
                Protocol="sqs",
                Endpoint=f"arn:aws:sqs:{os.environ['AWS_REGION']}:{DEFAULT_ACCOUNT_ID}:test-queue",
            )
            yield sns_client

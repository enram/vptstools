import filecmp
from unittest.mock import patch

from click.testing import CliRunner
import pandas as pd

from vptstools.bin.vph5_to_vpts import cli


def test_help():
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert (
        "Convert and aggregate h5 vp files to daily and monthly vpts-csv files"
        in result.output
    )


def test_e2e_cli(s3_inventory, path_inventory, tmp_path):
    """Run the full sequence from, inventory -> coverage -> daily file(s) -> monthly files

    The tests uses mocked S3 buckets setup with the data in tests/data/inventory. The mocked
    buckets were setup in the `s3_inventory` pytest fixture.
    """
    with patch(
        "pandas.Timestamp.now",
        return_value=pd.Timestamp("2023-02-02 00:00:00", tz="UTC"),
    ):
        # Run CLI command `vph5_to_vpts` with limited modified period check to 3 days
        runner = CliRunner()
        result = runner.invoke(cli, ["--modified-days-ago", str(3)])

        # Check individual steps of the CLI command
        assert "Create 1 daily vpts files" in result.output
        assert "Create 1 monthly vpts files" in result.output
        assert "Finished vpts update procedure" in result.output
        assert result.exception is None

        # Compare resulting coverage file with reference coverage ---------------------
        with open(tmp_path / "coverage.csv", "wb") as f:
            s3_inventory.download_fileobj("aloft", "coverage.csv", f)
        filecmp.cmp(path_inventory / "coverage.csv", tmp_path / "coverage.csv")

        # Compare resulting daily file
        with open(tmp_path / "nosta_vpts_20230311.csv", "wb") as f:
            s3_inventory.download_fileobj(
                "aloft", "baltrad/daily/nosta/2023/nosta_vpts_20230311.csv", f
            )
        filecmp.cmp(
            path_inventory / "nosta_vpts_20230311.csv",
            tmp_path / "nosta_vpts_20230311.csv",
        )

        # Compare resulting monthly file
        with open(tmp_path / "nosta_vpts_202303.csv.gz", "wb") as f:
            s3_inventory.download_fileobj(
                "aloft", "baltrad/monthly/nosta/2023/nosta_vpts_202303.csv.gz", f
            )
        filecmp.cmp(
            path_inventory / "nosta_vpts_202303.csv.gz",
            tmp_path / "nosta_vpts_202303.csv.gz",
        )


def test_e2e_cli_all(s3_inventory, path_inventory, tmp_path, sns):
    """Run the full sequence with option all to rerun all files in the bucket (zero-value).

    The tests uses mocked S3 buckets setup with the data in tests/data/inventory. The mocked
    buckets were setup in the `s3_inventory` pytest fixture.

    For this test, not all files were provided and the unit tests checks
    the calculated amount of files.
    """
    with patch(
        "pandas.Timestamp.now",
        return_value=pd.Timestamp("2023-02-02 00:00:00", tz="UTC"),
    ):
        # Run CLI command `vph5_to_vpts` with limited modified period check to 3 days
        runner = CliRunner()
        result = runner.invoke(cli, ["--modified-days-ago", str(0)])

        # Check individual steps of the CLI command
        assert "Create 5 daily vpts files" in result.output
        assert "Recreate the full set of bucket files" in result.output
        #  test fails/stops after creation of first daily file (only files provided for test)
        assert "[WARNING] - During conversion" in result.output
        assert result.exception is None
        # TODO - check if notification is sent to the SNS-TOPIC (currently only sent to mocked endpoint)

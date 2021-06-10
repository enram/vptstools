import os

from click.testing import CliRunner

from vptstools.scripts.vph5_to_vpts import cli

TEST_DIR = os.path.dirname(os.path.abspath(__file__))
SAMPlE_DATA_DIR = os.path.join(TEST_DIR, 'sample_data')


def test_help():
    runner = CliRunner()
    result = runner.invoke(cli, ['--help'])
    assert result.exit_code == 0
    assert "This tool aggregate/convert a bunch of ODIM" in result.output


def test_error_non_vp_source_file():
    """Error if non-VP ODIM file in source"""
    runner = CliRunner()
    directory_with_pvol_path = os.path.join(SAMPlE_DATA_DIR, 'directory_with_pvol')
    result = runner.invoke(cli, [f'{directory_with_pvol_path}/*'])
    assert result.exit_code == 1
    assert "Invalid ODIM source file" in result.output
    assert "Expected VP, found PVOL"


def test_error_no_source():
    """Error if no input source file found"""
    runner = CliRunner()
    directory_with_pvol_path = os.path.join(SAMPlE_DATA_DIR, 'directory_with_pvol_wrong')
    result = runner.invoke(cli, [f'{directory_with_pvol_path}/*'])
    assert result.exit_code == 2
    assert "No source data file found" in result.output

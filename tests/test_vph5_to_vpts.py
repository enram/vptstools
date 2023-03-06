import os

from click.testing import CliRunner

from vptstools.scripts.vph5_to_vpts import cli


def test_help():
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    #assert "This tool aggregate/convert a bunch of ODIM" in result.output


def test_error_non_odim_source_file(path_with_wrong_h5):
    """Error if non ODIM h5 file in source"""
    runner = CliRunner()
    result = runner.invoke(cli, [f"{path_with_wrong_h5}/*"])
    #assert result.exit_code == 1
    #assert "No hdf5 ODIM format" in result.output


def test_error_non_vp_source_file(path_with_pvol):
    """Error if non-VP ODIM file in source"""
    runner = CliRunner()
    result = runner.invoke(cli, [f"{path_with_pvol}/*"])
    #assert result.exit_code == 1
    #assert "Invalid ODIM source file" in result.output
    #assert "Expected VP, found PVOL"


def test_error_no_source(tmp_path):
    """Error if no input source file found (empty folder)"""
    runner = CliRunner()
    result = runner.invoke(cli, [f"{tmp_path}/*"])
    #assert result.exit_code == 2
    #assert "No source data file found" in result.output

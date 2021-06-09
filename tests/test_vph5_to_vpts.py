from click.testing import CliRunner

from vptstools.scripts.vph5_to_vpts import cli


def test_help():
    runner = CliRunner()
    result = runner.invoke(cli, ['--help'])
    assert result.exit_code == 0
    assert "This tool aggregate/convert a bunch of ODIM" in result.output

# TODO: test error if non-VP ODIM file in source
# TODO: test error if no input files found
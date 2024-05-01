import luigi


def test_luigi_config_file_env_var_sets_config_file():
    luigi_config = luigi.configuration.get_config()
    assert luigi_config.enabled
    assert "hrqb/luigi.cfg" in luigi_config._config_paths  # noqa: SLF001

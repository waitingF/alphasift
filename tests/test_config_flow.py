from pathlib import Path

from alphasift.config import Config


def test_flow_bars_dir_derived_from_data_dir(tmp_path: Path):
    data_dir = tmp_path / "data"
    config = Config(data_dir=data_dir, flow_bars_dir=data_dir / "flow_bars")
    assert config.flow_bars_dir == data_dir / "flow_bars"


def test_config_from_env_flow_bars_dir(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("ALPHASIFT_DATA_DIR", str(tmp_path / "custom_data"))
    config = Config.from_env()
    assert config.flow_bars_dir == tmp_path / "custom_data" / "flow_bars"

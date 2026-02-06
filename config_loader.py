from pathlib import Path
from dataclass_binder import Binder
from models import TestRigConfig

def load_test_rig_config(
    path: str | Path = "config.toml",
    /,
) -> TestRigConfig:
    """
    Load and validate the test rig configuration from a TOML file.

    Important:
        - TOML keys must use **kebab-case** (e.g. full-scale-min, alicat-shared)
          because dataclass-binder enforces this mapping to snake_case fields.

    Args:
        path: Path to the TOML configuration file (default: "config.toml")

    Returns:
        Fully validated TestRigConfig instance

    Raises:
        FileNotFoundError: Config file does not exist
        RuntimeError: Binding or validation failed
    """
    path = Path(path)

    if not path.is_file():
        raise FileNotFoundError(f"Config file not found: {path.resolve()}")

    try:
        config = Binder[TestRigConfig].parse_toml(path)
        # config.__post_init__()
        return config

    except Exception as e:
        msg = f"Failed to load config {path.name}: {type(e).__name__}: {e}"
        raise RuntimeError(msg) from e


if __name__ == "__main__":
    from loguru import logger

    try:
        cfg = load_test_rig_config()
        logger.info("Config loaded successfully")
        logger.info(f"Running in {'mock' if cfg.mock else 'standard'} mode")
        logger.info(f"Scale units        : {cfg.mass.units}")
        logger.info(
            f"Shared serial port  : "
            f"{cfg.alicat_shared.serial.port if cfg.alicat_shared.serial else 'None (per-device only)'}"
        )
        logger.info(f"Flow unit           : {cfg.flow.flow_unit}")
        logger.info(f"Flow full-scale max : {cfg.flow.full_scale_max}")
    except Exception as e:
        logger.error("Config loading failed")
        logger.exception(e)
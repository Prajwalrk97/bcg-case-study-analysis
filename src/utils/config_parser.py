import importlib.resources

import yaml
from loguru import logger

CONFIG_FILE = "config.yaml"

def read_config_file(env: str, path: str = CONFIG_FILE) -> dict:
    """Method for reading the config file.

    Args:
        path (str): Path of the config file.

    Returns:
        Config as a dict or an empty dict in case of error.
    """
    try:
        with importlib.resources.path("config", path) as config_path:
            logger.info("Reading config...")
            with open(config_path, "r") as fd:
                config = yaml.load(fd, Loader=yaml.SafeLoader)
            return config[env]

    except FileNotFoundError:
        logger.error(f"Error: The config file '{path}' was not found.")
    except yaml.YAMLError as e:
        logger.error(f"Error parsing the YAML file: {e}")
    except KeyError as e:
        logger.error(f"Invalid config environment - {e}")

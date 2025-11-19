"""I/O utilities for reading and writing data."""

import yaml
import pandas as pd
from pathlib import Path
from typing import Any, Dict
import json


def load_config(config_path: str = "config.yaml") -> Dict[str, Any]:
    """
    Load configuration from YAML file.

    Args:
        config_path: Path to config.yaml

    Returns:
        Configuration dictionary
    """
    config_file = Path(config_path)

    if not config_file.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)

    return config


def ensure_dir(path: str | Path) -> Path:
    """
    Ensure directory exists, create if not.

    Args:
        path: Directory path

    Returns:
        Path object
    """
    dir_path = Path(path)
    dir_path.mkdir(parents=True, exist_ok=True)
    return dir_path


def save_dataframe(df: pd.DataFrame, filepath: str, format: str = 'csv'):
    """
    Save DataFrame to file with automatic directory creation.

    Args:
        df: DataFrame to save
        filepath: Output file path
        format: 'csv', 'json', 'parquet', 'excel'
    """
    file_path = Path(filepath)
    ensure_dir(file_path.parent)

    if format == 'csv':
        df.to_csv(filepath, index=False)
    elif format == 'json':
        df.to_json(filepath, orient='records', date_format='iso', indent=2)
    elif format == 'parquet':
        df.to_parquet(filepath, index=False)
    elif format == 'excel':
        df.to_excel(filepath, index=False)
    else:
        raise ValueError(f"Unsupported format: {format}")


def load_dataframe(filepath: str, format: str = 'csv') -> pd.DataFrame:
    """
    Load DataFrame from file.

    Args:
        filepath: Input file path
        format: 'csv', 'json', 'parquet', 'excel'

    Returns:
        Loaded DataFrame
    """
    if not Path(filepath).exists():
        raise FileNotFoundError(f"File not found: {filepath}")

    if format == 'csv':
        return pd.read_csv(filepath)
    elif format == 'json':
        return pd.read_json(filepath)
    elif format == 'parquet':
        return pd.read_parquet(filepath)
    elif format == 'excel':
        return pd.read_excel(filepath)
    else:
        raise ValueError(f"Unsupported format: {format}")


def save_json(data: Dict | list, filepath: str, indent: int = 2):
    """
    Save data to JSON file.

    Args:
        data: Data to save
        filepath: Output file path
        indent: JSON indentation
    """
    file_path = Path(filepath)
    ensure_dir(file_path.parent)

    with open(filepath, 'w') as f:
        json.dump(data, f, indent=indent, default=str)


def load_json(filepath: str) -> Dict | list:
    """
    Load data from JSON file.

    Args:
        filepath: Input file path

    Returns:
        Loaded data
    """
    if not Path(filepath).exists():
        raise FileNotFoundError(f"File not found: {filepath}")

    with open(filepath, 'r') as f:
        return json.load(f)

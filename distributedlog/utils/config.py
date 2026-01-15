"""
Configuration management for DistributedLog.

Handles loading and merging configuration from:
- Default configuration file
- Environment-specific configuration files
- Environment variables
- Command-line arguments
"""

import os
from pathlib import Path
from typing import Any, Dict, Optional

import yaml


class Config:
    """Configuration manager for DistributedLog."""
    
    def __init__(self, config_file: Optional[str] = None):
        """
        Initialize configuration.
        
        Args:
            config_file: Path to configuration file. If None, uses default.
        """
        self._config: Dict[str, Any] = {}
        self._load_default_config()
        
        if config_file:
            self._load_config_file(config_file)
        
        self._apply_env_overrides()
    
    def _load_default_config(self) -> None:
        """Load default configuration."""
        default_config_path = Path(__file__).parent.parent.parent / "config" / "default.yaml"
        if default_config_path.exists():
            self._load_config_file(str(default_config_path))
    
    def _load_config_file(self, config_file: str) -> None:
        """
        Load configuration from YAML file.
        
        Args:
            config_file: Path to YAML configuration file
        """
        with open(config_file, "r") as f:
            file_config = yaml.safe_load(f)
            self._merge_config(file_config)
    
    def _merge_config(self, new_config: Dict[str, Any]) -> None:
        """
        Deep merge new configuration into existing configuration.
        
        Args:
            new_config: Configuration dictionary to merge
        """
        self._config = self._deep_merge(self._config, new_config)
    
    def _deep_merge(self, base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        """
        Deep merge two dictionaries.
        
        Args:
            base: Base dictionary
            override: Override dictionary
        
        Returns:
            Merged dictionary
        """
        result = base.copy()
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = value
        return result
    
    def _apply_env_overrides(self) -> None:
        """Apply environment variable overrides."""
        # Broker ID
        if broker_id := os.getenv("BROKER_ID"):
            self.set("broker.id", int(broker_id))
        
        # Broker host
        if broker_host := os.getenv("BROKER_HOST"):
            self.set("broker.host", broker_host)
        
        # Data directory
        if data_dir := os.getenv("DATA_DIR"):
            self.set("broker.data_dir", data_dir)
        
        # Log level
        if log_level := os.getenv("LOG_LEVEL"):
            self.set("logging.level", log_level)
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value using dot notation.
        
        Args:
            key: Configuration key in dot notation (e.g., "broker.port")
            default: Default value if key not found
        
        Returns:
            Configuration value
        """
        keys = key.split(".")
        value = self._config
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        return value
    
    def set(self, key: str, value: Any) -> None:
        """
        Set configuration value using dot notation.
        
        Args:
            key: Configuration key in dot notation
            value: Value to set
        """
        keys = key.split(".")
        config = self._config
        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            config = config[k]
        config[keys[-1]] = value
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Get entire configuration as dictionary.
        
        Returns:
            Configuration dictionary
        """
        return self._config.copy()


# Global configuration instance
_config: Optional[Config] = None


def get_config(config_file: Optional[str] = None) -> Config:
    """
    Get global configuration instance.
    
    Args:
        config_file: Optional configuration file path
    
    Returns:
        Configuration instance
    """
    global _config
    if _config is None:
        _config = Config(config_file)
    return _config


def reset_config() -> None:
    """Reset global configuration (mainly for testing)."""
    global _config
    _config = None

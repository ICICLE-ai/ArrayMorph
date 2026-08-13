# arraymorph/__init__.py
import os
import sys
import json
import logging

_AWS_CONFIG = {
    'STORAGE_PLATFORM': 'S3',
    'BUCKET_NAME': None,
    'AWS_ENDPOINT_URL_S3': None,
    'AWS_S3_ADDRESSING_STYLE': None,
    'AWS_ACCESS_KEY_ID': None,
    'AWS_SECRET_ACCESS_KEY': None,
    'AWS_REGION': 'us-east-2',    
    'AWS_USE_PATH_STYLE': 'false',
    'AWS_USE_TLS': 'false',
    'AWS_SIGNED_PAYLOADS': 'false',
    'AZURE_STORAGE_CONNECTION_STRING': None,
}

def _load_config_from_file():
    """
    Load AWS configuration from ~/.arraymorph/config (JSON format).
    """
    config_path = os.path.expanduser('~/.arraymorph/config')
    if not os.path.isfile(config_path):
        logging.warning(f"Config file {config_path} not found. Not loading ArrayMorph")
        return False

    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
    except json.JSONDecodeError as e:
        logging.warning(f"Config file {config_path} is not valid JSON. Not loading ArrayMorph")
        return False

    # Store the loaded config into _AWS_CONFIG (overwriting the defaults)
    for key in _AWS_CONFIG.keys():
        if key in config:
            _AWS_CONFIG[key] = config[key]
    
    return True

def _apply_user_aws_config():
    """
    Apply values from _AWS_CONFIG to os.environ,
    but ONLY if they are not already set in the shell/environment.
    This lets exported variables take priority.
    """
    for key, value in _AWS_CONFIG.items():
        if value is not None:
            os.environ.setdefault(key, value)

def _set_hdf5_environment():
    # Determine the base directory (conda environment prefix)
    conda_prefix = os.environ.get('CONDA_PREFIX')
    if conda_prefix is None:
        # Fallback: use sys.prefix if running inside a conda environment
        # (sys.prefix usually points to the environment's root)
        conda_prefix = sys.prefix

    # Build the plugin path
    plugin_path = os.path.join(conda_prefix, 'lib', 'arraymorph')

    # Set the environment variables
    os.environ['HDF5_PLUGIN_PATH'] = plugin_path
    os.environ['HDF5_VOL_CONNECTOR'] = 'arraymorph'

# Execute the function when the module is imported
if (_load_config_from_file()):
    _apply_user_aws_config()
    _set_hdf5_environment()


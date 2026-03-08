from __future__ import annotations

import os
import sys
from pathlib import Path


# ---------------------------------------------------------------------
# Existing runtime configuration helpers (kept intentionally minimal)
# ---------------------------------------------------------------------


def _get_env(name: str, default: str | None = None) -> str | None:
    """Helper to read environment variables safely."""
    return os.environ.get(name, default)


# These are intentionally NOT modified by enable().
# They remain user-controlled runtime configuration.
STORAGE_PLATFORM = _get_env("STORAGE_PLATFORM")
BUCKET_NAME = _get_env("BUCKET_NAME")
AWS_ACCESS_KEY_ID = _get_env("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = _get_env("AWS_SECRET_ACCESS_KEY")
AWS_REGION = _get_env("AWS_REGION")


# ---------------------------------------------------------------------
# ArrayMorph VOL plugin helpers
# ---------------------------------------------------------------------


def _plugin_filename() -> str:
    if sys.platform == "darwin":
        return "libarraymorph.dylib"
    if sys.platform.startswith("linux"):
        return "libarraymorph.so"

    raise RuntimeError(f"Unsupported platform: {sys.platform}")


def get_plugin_path() -> str:
    """
    Return the absolute path to the ArrayMorph VOL plugin library.
    """
    pkg_dir = Path(__file__).resolve().parent
    lib_dir = pkg_dir / "lib"

    plugin = lib_dir / _plugin_filename()

    if plugin.exists():
        return str(plugin)

    # fallback for versioned library names
    matches = list(lib_dir.glob("libarraymorph*"))
    if matches:
        return str(matches[0])

    raise FileNotFoundError(f"Could not locate ArrayMorph plugin inside {lib_dir}")


def get_plugin_dir() -> str:
    """Return the directory containing the VOL plugin."""
    return str(Path(get_plugin_path()).parent)


def enable() -> None:
    """
    Enable the ArrayMorph VOL connector for the current process.

    This only sets HDF5 plugin discovery variables.
    Storage configuration variables are left untouched.
    """
    os.environ.setdefault("HDF5_PLUGIN_PATH", get_plugin_dir())
    os.environ.setdefault("HDF5_VOL_CONNECTOR", "arraymorph")


# ---------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------

__all__ = [
    "enable",
    "get_plugin_path",
    "get_plugin_dir",
]

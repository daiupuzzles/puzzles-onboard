"""Doppler secret access for Google wrapper."""

import json
import logging
import os
import subprocess
from typing import Optional

log = logging.getLogger(__name__)

_cache: dict[tuple[str, str], dict] = {}


def get_secrets(project: str = "flowsly", config: str = "prd") -> dict:
    """Fetch all secrets from a Doppler project as a dict.

    Results are cached per (project, config) to avoid repeated subprocess
    invocations while supporting multiple Doppler projects in the same process.
    Falls back to os.environ when Doppler CLI is not available or lacks access.

    Args:
        project: Doppler project (default: "flowsly")
        config: Doppler config (default: "prd")

    Returns:
        Dict of secret_key -> secret_value
    """
    cache_key = (project, config)
    if cache_key in _cache:
        return _cache[cache_key]

    try:
        result = subprocess.run(
            ["doppler", "secrets", "download", "--project", project, "--config", config,
             "--format", "json", "--no-file"],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            log.warning("Doppler failed for %s/%s: %s — falling back to os.environ",
                        project, config, result.stderr.strip())
            _cache[cache_key] = dict(os.environ)
        else:
            _cache[cache_key] = json.loads(result.stdout)
    except FileNotFoundError:
        log.info("Doppler CLI not found, falling back to os.environ")
        _cache[cache_key] = dict(os.environ)

    return _cache[cache_key]

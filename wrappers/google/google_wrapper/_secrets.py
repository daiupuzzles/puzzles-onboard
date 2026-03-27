"""Doppler secret access for Google wrapper."""

import json
import logging
import os
import subprocess
from typing import Optional

log = logging.getLogger(__name__)

_cache: Optional[dict] = None


def get_secrets(project: str = "flowsly", config: str = "prd") -> dict:
    """Fetch all secrets from a Doppler project as a dict.

    Results are cached after first call to avoid repeated subprocess invocations.
    Falls back to os.environ when Doppler CLI is not available (e.g., Modal).

    Args:
        project: Doppler project (default: "flowsly")
        config: Doppler config (default: "prd")

    Returns:
        Dict of secret_key -> secret_value
    """
    global _cache
    if _cache is not None:
        return _cache

    try:
        result = subprocess.run(
            ["doppler", "secrets", "download", "--project", project, "--config", config,
             "--format", "json", "--no-file"],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"Doppler failed for {project}/{config}: {result.stderr.strip()}")
        _cache = json.loads(result.stdout)
    except FileNotFoundError:
        log.info("Doppler CLI not found, falling back to os.environ")
        _cache = dict(os.environ)

    return _cache

"""Doppler secret access for Supabase wrapper."""

import json
import subprocess
from typing import Optional

_cache: Optional[dict] = None


def get_secrets(project: str = "flowsly", config: str = "prd") -> dict:
    """Fetch all secrets from a Doppler project as a dict.

    Results are cached after first call to avoid repeated subprocess invocations.

    Args:
        project: Doppler project (default: "flowsly")
        config: Doppler config (default: "prd")

    Returns:
        Dict of secret_key -> secret_value

    Raises:
        RuntimeError: If Doppler command fails
    """
    global _cache
    if _cache is not None:
        return _cache

    result = subprocess.run(
        ["doppler", "secrets", "download", "--project", project, "--config", config,
         "--format", "json", "--no-file"],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Doppler failed for {project}/{config}: {result.stderr.strip()}")

    _cache = json.loads(result.stdout)
    return _cache

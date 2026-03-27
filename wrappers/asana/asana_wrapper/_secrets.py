"""Doppler secret access for Asana wrapper."""

import subprocess


def get_secret(key: str, project: str = "puzzles", config: str = "prd") -> str:
    """Fetch a single secret from Doppler.

    Args:
        key: Secret key name (e.g., "ASANA_API_KEY")
        project: Doppler project (default: "puzzles")
        config: Doppler config (default: "prd")

    Returns:
        Secret value as string

    Raises:
        RuntimeError: If Doppler command fails
    """
    result = subprocess.run(
        ["doppler", "secrets", "get", key, "--project", project, "--config", config, "--plain"],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Doppler failed for {key} ({project}/{config}): {result.stderr.strip()}")
    return result.stdout.strip()

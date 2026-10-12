
import json
import logging
import os
from pathlib import Path
from urllib.parse import urlsplit


PROJECT_ROOT = Path(__file__).resolve().parents[2]

CONFIG_PATH = PROJECT_ROOT / "config" / "app_config.json"
ENV_CONFIG_DIR = PROJECT_ROOT / "config" / "environments"

DEFAULT_LOG_FORMAT = (
    "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

SUPPORTED_ENVIRONMENTS = {"dev", "uat", "prod"}


def load_config() -> dict:
    """Load, merge, and validate shared and environment settings."""

    environment = os.getenv("APP_ENV", "dev").strip().lower()

    if environment not in SUPPORTED_ENVIRONMENTS:
        raise ValueError(
            f"Unsupported APP_ENV '{environment}'. "
            "Choose dev, uat, or prod."
        )

    # Load shared configuration.
    with CONFIG_PATH.open("r", encoding="utf-8") as file:
        config = json.load(file)

    if not isinstance(config, dict):
        raise ValueError(
            "Shared configuration must be a JSON object."
        )

    for section in ("api", "logging"):
        if not isinstance(config.get(section), dict):
            raise ValueError(
                f"Missing or invalid configuration section: {section}"
            )

    # Load environment-specific configuration.
    environment_config_path = (
        ENV_CONFIG_DIR / f"{environment}.json"
    )

    with environment_config_path.open(
        "r",
        encoding="utf-8",
    ) as file:
        environment_config = json.load(file)

    if not isinstance(environment_config, dict):
        raise ValueError(
            "Environment configuration must be a JSON object."
        )

    storage = environment_config.get("storage")

    if not isinstance(storage, dict):
        raise ValueError(
            "Missing or invalid environment storage configuration."
        )

    config["environment"] = environment
    config["storage"] = storage

    # Validate API configuration.
    api_config = config["api"]

    for key in ("base_url", "timeout_seconds", "apps"):
        if key not in api_config:
            raise ValueError(f"Missing api.{key} configuration.")

    if (
        not isinstance(api_config["base_url"], str)
        or not api_config["base_url"].strip()
    ):
        raise ValueError("api.base_url must be a non-empty string.")

    timeout = api_config["timeout_seconds"]

    if (
        isinstance(timeout, bool)
        or not isinstance(timeout, (int, float))
        or timeout <= 0
    ):
        raise ValueError(
            "api.timeout_seconds must be a positive number."
        )

    apps = api_config["apps"]

    if not isinstance(apps, list) or not apps:
        raise ValueError("api.apps must be a non-empty list.")

    for index, app in enumerate(apps):
        if not isinstance(app, dict):
            raise ValueError(f"api.apps[{index}] must be an object.")

        if "app_id" not in app or "country" not in app:
            raise ValueError(
                f"api.apps[{index}] requires app_id and country."
            )

        if (
            isinstance(app["app_id"], bool)
            or not isinstance(app["app_id"], int)
        ):
            raise ValueError(
                f"api.apps[{index}].app_id must be an integer."
            )

        if (
            not isinstance(app["country"], str)
            or not app["country"].strip()
        ):
            raise ValueError(
                f"api.apps[{index}].country must be a non-empty string."
            )

    # Validate logging configuration.
    logging_config = config["logging"]
    level_name = logging_config.get("level", "INFO")

    if (
        not isinstance(level_name, str)
        or not hasattr(logging, level_name.upper())
    ):
        raise ValueError(
            "logging.level must be a valid logging level."
        )

    # Validate storage configuration.
    backend = storage.get("backend")

    if backend == "local":
        for key in ("raw_output_dir", "processed_output_dir"):
            value = storage.get(key)

            if not isinstance(value, str) or not value.strip():
                raise ValueError(
                    f"Missing or invalid storage.{key}."
                )

    elif backend == "s3":
        for key in ("raw_output_uri", "processed_output_uri"):
            uri = storage.get(key)

            if not isinstance(uri, str) or not uri.strip():
                raise ValueError(
                    f"Missing or invalid storage.{key}."
                )

            parsed_uri = urlsplit(uri)

            if (
                parsed_uri.scheme != "s3"
                or not parsed_uri.netloc
                or parsed_uri.query
                or parsed_uri.fragment
                or "REPLACE-WITH" in uri
            ):
                raise ValueError(
                    f"Invalid or unconfigured S3 URI in storage.{key}."
                )

    else:
        raise ValueError(
            "storage.backend must be either 'local' or 's3'."
        )

    return config


def configure_logging(config: dict) -> None:
    """Initialize logging from the shared configuration."""

    logging_config = config["logging"]
    level_name = logging_config.get("level", "INFO").upper()

    logging.basicConfig(
        level=getattr(logging, level_name),
        format=logging_config.get(
            "format",
            DEFAULT_LOG_FORMAT,
        ),
    )

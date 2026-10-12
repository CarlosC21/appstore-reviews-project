
import json
import logging
from urllib.parse import urlsplit

import boto3
import requests
from botocore.exceptions import BotoCoreError, ClientError

from src.configuration.config_loader import (
    DEFAULT_LOG_FORMAT,
    PROJECT_ROOT,
    configure_logging,
    load_config,
)


logger = logging.getLogger(__name__)


# --------------------------------------------------
# Storage
# --------------------------------------------------

def save_app_metadata(
    data: dict,
    app_id: int,
    country: str,
    config: dict,
) -> str:
    """Save raw API data to local storage or Amazon S3."""

    storage = config["storage"]

    relative_path = (
        f"app_id={app_id}/"
        f"country={country}/"
        "response.json"
    )

    # Local filesystem: development environment.
    if storage["backend"] == "local":
        output_root = PROJECT_ROOT / storage["raw_output_dir"]
        output_file = output_root / relative_path

        output_file.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        with output_file.open("w", encoding="utf-8") as file:
            json.dump(
                data,
                file,
                indent=2,
                ensure_ascii=False,
            )

        return str(output_file)

    # Amazon S3: UAT or production environments.
    if storage["backend"] == "s3":
        parsed_uri = urlsplit(storage["raw_output_uri"])

        bucket_name = parsed_uri.netloc
        prefix = parsed_uri.path.strip("/")

        object_key = "/".join(
            part
            for part in (prefix, relative_path)
            if part
        )

        body = json.dumps(
            data,
            indent=2,
            ensure_ascii=False,
        ).encode("utf-8")

        # Uses the standard AWS credential provider chain.
        s3_client = boto3.client("s3")

        s3_client.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body=body,
            ContentType="application/json",
        )

        return f"s3://{bucket_name}/{object_key}"

    raise ValueError(
        f"Unsupported storage backend: {storage['backend']}"
    )


# --------------------------------------------------
# API ingestion
# --------------------------------------------------

def fetch_app_metadata(
    app_id: int,
    country: str,
    config: dict,
) -> bool:
    """Fetch app metadata, validate the response, and save the JSON."""

    api_config = config["api"]

    params = {
        "id": app_id,
        "country": country,
        "entity": "software",
    }

    logger.info(
        "Fetching metadata: app_id=%s, country=%s",
        app_id,
        country,
    )

    # 1. Request the API.
    try:
        response = requests.get(
            api_config["base_url"],
            params=params,
            timeout=api_config["timeout_seconds"],
        )

        response.raise_for_status()

    except requests.exceptions.Timeout:
        logger.exception(
            "API request timed out: app_id=%s, country=%s",
            app_id,
            country,
        )
        return False

    except requests.exceptions.HTTPError as exc:
        status_code = (
            exc.response.status_code
            if exc.response is not None
            else "unknown"
        )

        logger.error(
            "HTTP request failed: app_id=%s, country=%s, "
            "status_code=%s, error=%s",
            app_id,
            country,
            status_code,
            exc,
        )
        return False

    except requests.exceptions.RequestException as exc:
        logger.error(
            "API request failed: app_id=%s, country=%s, error=%s",
            app_id,
            country,
            exc,
        )
        return False

    logger.info(
        "API returned HTTP %s for app_id=%s, country=%s",
        response.status_code,
        app_id,
        country,
    )

    # 2. Parse and validate the response.
    try:
        data = response.json()

    except ValueError as exc:
        logger.error(
            "Invalid JSON response: app_id=%s, country=%s, error=%s",
            app_id,
            country,
            exc,
        )
        return False

    if (
        not isinstance(data, dict)
        or not isinstance(data.get("results"), list)
    ):
        logger.error(
            "Unexpected API response structure: "
            "app_id=%s, country=%s",
            app_id,
            country,
        )
        return False

    # 3. Save the raw response to the configured destination.
    try:
        saved_location = save_app_metadata(
            data=data,
            app_id=app_id,
            country=country,
            config=config,
        )

    except (OSError, ValueError, BotoCoreError, ClientError):
        logger.exception(
            "Failed to save metadata: app_id=%s, country=%s",
            app_id,
            country,
        )
        return False

    # 4. Log the outcome.
    result_count = data.get(
        "resultCount",
        len(data["results"]),
    )

    logger.info("Saved raw response to %s", saved_location)

    if result_count == 0:
        logger.warning(
            "API returned no results: app_id=%s, country=%s",
            app_id,
            country,
        )
    else:
        logger.info(
            "Retrieved %s result(s): app_id=%s, country=%s",
            result_count,
            app_id,
            country,
        )

    return True


# --------------------------------------------------
# Job entry point
# --------------------------------------------------

def main() -> int:
    """Run ingestion for all configured applications."""

    try:
        config = load_config()

    except (OSError, json.JSONDecodeError, ValueError):
        logging.basicConfig(
            level=logging.ERROR,
            format=DEFAULT_LOG_FORMAT,
        )
        logger.exception("Unable to load project configuration.")
        return 1

    configure_logging(config)

    logger.info(
        "Starting ingestion in environment: %s",
        config["environment"],
    )

    failed_apps = []

    for app in config["api"]["apps"]:
        success = fetch_app_metadata(
            app_id=app["app_id"],
            country=app["country"],
            config=config,
        )

        if not success:
            failed_apps.append(app)

    if failed_apps:
        logger.error(
            "Ingestion completed with failures: %d of %d requests failed",
            len(failed_apps),
            len(config["api"]["apps"]),
        )
        return 1

    logger.info(
        "Ingestion completed successfully: %d requests processed",
        len(config["api"]["apps"]),
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

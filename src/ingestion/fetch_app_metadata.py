import json
from pathlib import Path

import requests


BASE_URL = "https://itunes.apple.com/lookup"

APPS = [
    {
        "app_id": 310633997,
        "country": "at",
    },
    {
        "app_id": 421997825,
        "country": "au",
    },
]


def fetch_app_metadata(app_id, country):
    output_dir = (
        Path(__file__).resolve().parents[2]
        / "raw"
        / "api"
        / "app_metadata"
        / f"app_id={app_id}"
        / f"country={country}"
    )

    output_file = output_dir / "response.json"

    params = {
        "id": app_id,
        "country": country,
        "entity": "software",
    }

    print(f"Fetching app_id={app_id}, country={country}")

    response = requests.get(
        BASE_URL,
        params=params,
        timeout=30,
    )

    response.raise_for_status()

    data = response.json()

    output_dir.mkdir(parents=True, exist_ok=True)

    with output_file.open("w", encoding="utf-8") as file:
        json.dump(
            data,
            file,
            indent=2,
            ensure_ascii=False,
        )

    print(f"Saved: {output_file}")
    print(f"resultCount: {data.get('resultCount')}")
    print()


def main():
    for app in APPS:
        fetch_app_metadata(
            app_id=app["app_id"],
            country=app["country"],
        )


if __name__ == "__main__":
    main()
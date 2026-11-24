''' 
Written for Huwise functioning the same af for FallingFruit
'''

import os
import requests

DEBUG = True

# Huwise CSV export URL
DATASET_URL = (
    "https://hub.huwise.com/api/explore/v2.1/catalog/datasets/"
    "osm-france-drinking-water/exports/csv/"
    "?delimiter=%3B&lang=en&timezone=Europe%2FParis&use_labels=true"
)

# Store data under the repository's data directory, independent of the task cwd.
REPOSITORY_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
LOCAL_DATA_FOLDER = os.path.join(
    REPOSITORY_ROOT,
    "data",
    "osm_france_drinking_water",
)
DATASET_FILE = os.path.join(
    LOCAL_DATA_FOLDER,
    "osm_france_drinking_water.csv"
)

# Local ETag storage
ETAG_FILE = os.path.join(
    LOCAL_DATA_FOLDER,
    "dataset_etag.txt"
)


def get_stored_etag(etag_file):
    """Read the previously stored ETag."""
    if os.path.exists(etag_file):
        with open(etag_file, "r", encoding="utf-8") as f:
            return f.read().strip()
    return None


def save_etag(etag, etag_file):
    """Save the latest ETag."""
    with open(etag_file, "w", encoding="utf-8") as f:
        f.write(etag)


def download_dataset(dataset_url, local_file, etag_file):
    """
    Check whether the remote dataset has changed using ETag.
    Download the CSV only if a new version is available.
    """

    # Ensure the local folder exists
    os.makedirs(os.path.dirname(local_file), exist_ok=True)

    headers = {}
    stored_etag = get_stored_etag(etag_file)

    # Request only a new version if we already have a local copy
    if stored_etag and os.path.exists(local_file):
        headers["If-None-Match"] = stored_etag

        if DEBUG:
            print(f"Checking for dataset updates. ETag: {stored_etag}")
    else:
        if DEBUG:
            print("No existing dataset or ETag found. Starting initial download.")

    try:
        response = requests.get(
            dataset_url,
            headers=headers,
            stream=True,
            timeout=(10, 120)
        )

        # Dataset has not changed
        if response.status_code == 304:
            print("Dataset has not changed. No download needed.")
            return False

        # Download a new version
        response.raise_for_status()

        if DEBUG:
            print("New dataset available. Downloading CSV...")

        # Write to a temporary file first
        temporary_file = local_file + ".tmp"

        try:
            with open(temporary_file, "wb") as f:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)

            # Replace the previous file only after successful download
            os.replace(temporary_file, local_file)

        except Exception:
            if os.path.exists(temporary_file):
                os.remove(temporary_file)
            raise

        print(f"Download complete: {local_file}")

        # Save the new ETag, if supplied by the server
        new_etag = response.headers.get("ETag")

        if new_etag:
            save_etag(new_etag, etag_file)
            print(f"Saved new ETag: {new_etag}")
        else:
            print("Warning: Server did not return an ETag.")

        return True

    except requests.exceptions.RequestException as error:
        print(f"Download failed: {error}")
        raise


if DEBUG or __name__ == "__main__":
    download_dataset(DATASET_URL, DATASET_FILE, ETAG_FILE)

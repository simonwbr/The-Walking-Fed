'''
The falling fruit API is not intended for large scale data ingestion.
However, the data set is small enough to be kept locally (~50 MB) and it's available for download. If not yet present, we download the data set.
There are no changelogs published on which data has changed so we can't just update the data set via API calls.
Instead, we check periodically for updates via http 304 and only download the data set again if there has been a change.
'''

import os
import requests

DEBUG = True

# URL of falling fruits data set
LOCATION_URL = "https://fallingfruit.org/locations.csv.bz2"
TYPE_URL = "https://fallingfruit.org/types.csv.bz2"

# Local folder to store the data set and ETag
LOCAL_DATA_FOLDER = "data/falling_fruit"

# Local copy of the data set
LOCATION_DATASET = os.path.join(LOCAL_DATA_FOLDER, "locations.csv.bz2")
TYPE_DATASET = os.path.join(LOCAL_DATA_FOLDER, "types.csv.bz2")

# ETag used to store the last know version of the data set. If the ETag has changed, we know that the data set has been updated and we need to download it again.
LOCATION_ETAG = os.path.join(LOCAL_DATA_FOLDER, "location_etag.txt")
TYPE_ETAG = os.path.join(LOCAL_DATA_FOLDER, "type_etag.txt")

def download_dataset(dataset_url, local_file, etag_store):
    """
    Checks if the dataset has changed using HTTP conditional requests (ETag).
    Downloads the full compressed file only if changes are detected.
    """
    headers = {}
    stored_etag = get_stored_etag(etag_store)
    
    # If we have an existing data set file and a stored ETag, ask the server for conditional match
    if stored_etag and os.path.exists(local_file):
        headers["If-None-Match"] = stored_etag
        print(f"Checking for updates for {local_file}. Sending If-None-Match: {stored_etag}")
    else:
        print(f"No local ETag or file found for {local_file}. Initiating full download...")

    # Send a GET request with our conditional header
    response = requests.get(dataset_url, headers=headers, stream=True)

    if response.status_code == 304:
        print(f"Status 304: Data for {local_file} has not changed. Skipping download.")
        return False
        
    elif response.status_code == 200:
        print(f"Status 200: New data found or initial download started for {local_file}. Downloading...")
        
        # Write the streaming chunks to our local compressed file
        with open(local_file, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    
        print(f"Download complete: saved to '{local_file}'")
        
        # Extract and save the new ETag for the next check
        new_etag = response.headers.get("ETag")
        if new_etag:
            save_etag(new_etag, etag_store)
            print(f"Saved new ETag for {local_file}: {new_etag}")
        else:
            print(f"Warning: Server did not return an ETag header for {local_file}.")
        return True
        
    else:
        print(f"Error: Server responded with status code {response.status_code} for {local_file}")
        return False


def get_stored_etag(etag_store):
    """Get the last known ETag from the local store."""
    if os.path.exists(etag_store):
        with open(etag_store, "r") as f:
            return f.read().strip()
    return None


def save_etag(etag, etag_store):
    """Store the ETag in the local store."""
    with open(etag_store, "w") as f:
        f.write(etag)


if DEBUG or __name__ == "__main__":
    # Download the location dataset
    download_dataset(LOCATION_URL, LOCATION_DATASET, LOCATION_ETAG)
    
    # Download the type dataset
    download_dataset(TYPE_URL, TYPE_DATASET, TYPE_ETAG)
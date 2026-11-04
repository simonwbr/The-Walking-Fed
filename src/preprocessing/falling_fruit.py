'''
The ingested data sets are zipped and need to be unzipped.
This script unzips the data sets.
The data sets also contain entries we do not want to keep, e.g. dumpsters.
'''

import os
import bz2

DEBUG = True

DATA_FOLDER = "data/falling_fruit"
LOCATION_DATASET_ZIPPED = os.path.join(DATA_FOLDER, "locations.csv.bz2")
LOCATION_DATASET_UNZIPPED = os.path.join(DATA_FOLDER, "locations.csv")
TYPE_DATASET_ZIPPED = os.path.join(DATA_FOLDER, "types.csv.bz2")
TYPE_DATASET_UNZIPPED = os.path.join(DATA_FOLDER, "types.csv")


def unzip_dataset(zipped_file, unzipped_file):
    if not os.path.exists(zipped_file):
        print(f"Error: Archive not found at {zipped_file}. Run ingestion first.")
        return

    try:
        # Unzip the dataset
        with bz2.open(zipped_file, 'rt') as zipped, open(unzipped_file, 'w') as unzipped:
            for line in zipped:
                unzipped.write(line)

        print(f"Unzipped data saved to {unzipped_file}")
    except Exception as e:
        print(f"Error during unzipping: {e}")


def preview_dataset(unzipped_file):
    if not os.path.exists(unzipped_file):
        print(f"Error: Unzipped dataset not found at {unzipped_file}. Run unzip_dataset() first.")
        return

    try:
        # Preview the first few lines of the unzipped dataset
        with open(unzipped_file, 'r') as f:
            print(f"Preview of {unzipped_file}:")
            for _ in range(5):
                print(f.readline().strip())
    except Exception as e:
        print(f"Error during preview: {e}")


if DEBUG or __name__ == "__main__":
    unzip_dataset(LOCATION_DATASET_ZIPPED, LOCATION_DATASET_UNZIPPED)
    preview_dataset(LOCATION_DATASET_UNZIPPED)

    unzip_dataset(TYPE_DATASET_ZIPPED, TYPE_DATASET_UNZIPPED)
    preview_dataset(TYPE_DATASET_UNZIPPED)
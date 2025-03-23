import os
import json
import hashlib
import numpy as np
from tqdm import tqdm


def load_baseline_metadata(json_file):
    """
    Load baseline metadata from a JSON file into a numpy array.

    Args:
        json_file (str): Path to the JSON file containing baseline metadata.

    Returns:
        np.ndarray: Numpy array containing baseline metadata (hash, size, timestamp).
    """
    with open(json_file, "r") as f:
        baseline_metadata = json.load(f)

    num_files = len(baseline_metadata)
    shared_array = np.zeros((num_files, 3), dtype=np.float64)

    for i, entry in enumerate(baseline_metadata):
        shared_array[i, 0] = entry["hash"]  # Truncated hash (64-bit integer)
        shared_array[i, 1] = entry["size"]  # File size
        shared_array[i, 2] = entry["timestamp"]  # File timestamp

    return shared_array


def calculate_file_metadata(file_path, hash_algorithm="md5"):
    """
    Calculate file metadata (hash, size, timestamp) for a single file.

    Args:
        file_path (str): Path to the file.
        hash_algorithm (str): Hashing algorithm to use (default: 'md5').

    Returns:
        dict: Metadata for the file (hash, size, timestamp).
    """
    try:
        hash_func = hashlib.new(hash_algorithm)
        with open(file_path, "rb") as f:
            while chunk := f.read(8192):
                hash_func.update(chunk)
        file_hash = hash_func.hexdigest()

        # Convert hash to numeric and truncate to 64 bits
        hash_numeric = int(file_hash, 16) & ((1 << 64) - 1)

        # Get file size and last modification timestamp
        file_size = os.path.getsize(file_path)
        file_timestamp = os.path.getmtime(file_path)

        return {"hash": hash_numeric, "size": file_size, "timestamp": file_timestamp}
    except FileNotFoundError:
        return None


def process_files(directory, baseline_array):
    """
    Process files in a directory and compare their metadata with the baseline.

    Args:
        directory (str): Path to the directory to scan.
        baseline_array (np.ndarray): Numpy array containing baseline metadata.

    Returns:
        list: List of alerts for file changes.
    """
    alerts = []
    baseline_hashes = baseline_array[:, 0]
    baseline_sizes = baseline_array[:, 1]
    baseline_timestamps = baseline_array[:, 2]

    # Iterate through all files in the directory
    file_paths = [
        os.path.join(root, file)
        for root, _, files in os.walk(directory)
        for file in files
    ]

    # Use tqdm for progress tracking
    for file_path in tqdm(file_paths, desc="Processing Files"):
        current_metadata = calculate_file_metadata(file_path)

        if current_metadata is None:
            alerts.append(f"File not found: {file_path}")
            continue

        if current_metadata["hash"] not in baseline_hashes:
            alerts.append(f"New file detected: {file_path}")
        else:
            idx = np.where(baseline_hashes == current_metadata["hash"])[0][0]
            if current_metadata["size"] != baseline_sizes[idx]:
                alerts.append(f"File size changed: {file_path}")
            if current_metadata["timestamp"] != baseline_timestamps[idx]:
                alerts.append(f"File timestamp changed: {file_path}")

    return alerts


def main():
    # Configuration
    directory_to_scan = r"E:\Images"
    baseline_json = "file_metadata_truncated.json"

    # Load baseline metadata
    print("Loading baseline metadata...")
    baseline_array = load_baseline_metadata(baseline_json)
    print("Baseline metadata loaded.")

    # Process files sequentially
    print("Processing files...")
    alerts = process_files(directory_to_scan, baseline_array)

    # Print all alerts at the end
    print("\n--- Alerts ---")
    for alert in alerts:
        print(alert)

    print("File comparison complete.")


if __name__ == "__main__":
    main()

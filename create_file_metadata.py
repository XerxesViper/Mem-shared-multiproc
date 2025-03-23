import os
import json
import hashlib
from tqdm import tqdm


def calculate_file_metadata_initial(directory, output_file, hash_algorithm="md5"):
    """
    Calculate file metadata (numeric hash, size, timestamp) for all files in a directory
    and store the results in a JSON file.

    Args:
        directory (str): Path to the directory to scan.
        output_file (str): Path to the JSON file where metadata will be stored.
        hash_algorithm (str): Hashing algorithm to use (default: 'sha256').

    Returns:
        None
    """
    file_metadata = []

    # Walk through the directory and process each file
    for root, _, files in os.walk(directory):
        for file in tqdm(files):
            file_path = os.path.join(root, file)
            try:
                # Calculate file hash
                hash_func = hashlib.new(hash_algorithm)
                with open(file_path, "rb") as f:
                    while chunk := f.read(8192):  # Read file in chunks to handle large files
                        hash_func.update(chunk)
                file_hash = hash_func.hexdigest()

                # Convert hash to a numeric value
                hash_numeric = int(file_hash, 16)

                # Get file size and last modification timestamp
                file_size = os.path.getsize(file_path)
                file_timestamp = os.path.getmtime(file_path)

                # Append metadata to the list
                file_metadata.append({
                    "hash": hash_numeric,  # Numeric representation of the hash
                    "size": file_size,  # File size in bytes
                    "timestamp": file_timestamp  # Last modification time
                })
            except Exception as e:
                print(f"Error processing file {file_path}: {e}")

    # Write metadata to a JSON file
    with open(output_file, "w") as f:
        json.dump(file_metadata, f, indent=4)

    print(f"File metadata saved to {output_file}")


def calculate_file_hash(file_path, hash_algorithm="sha256"):
    """
    Calculate the hash of a file using the specified algorithm.

    Args:
        file_path (str): Path to the file.
        hash_algorithm (str): Hashing algorithm to use (default: 'sha256').

    Returns:
        str: Hexadecimal hash of the file.
    """
    hash_func = hashlib.new(hash_algorithm)
    with open(file_path, "rb") as f:
        while chunk := f.read(8192):  # Read file in chunks to handle large files
            hash_func.update(chunk)
    return hash_func.hexdigest()


def truncate_hashes_in_json(input_file, output_file, bits=64):
    """
    Open a JSON file, truncate each 128-bit hash to 64 bits, and save the updated data.

    Args:
        input_file (str): Path to the input JSON file with 128-bit hashes.
        output_file (str): Path to the output JSON file with truncated 64-bit hashes.
        bits (int): Number of bits to truncate the hash to (default: 64).

    Returns:
        None
    """
    # Load the JSON file
    with open(input_file, "r") as f:
        file_metadata = json.load(f)

    # Truncate each hash
    for entry in file_metadata:
        original_hash = entry["hash"]
        truncated_hash = original_hash & ((1 << bits) - 1)  # Keep only the lower `bits` bits
        entry["hash"] = truncated_hash  # Update the hash in the metadata

    # Save the updated metadata to a new JSON file
    with open(output_file, "w") as f:
        json.dump(file_metadata, f, indent=4)

    print(f"Truncated hashes saved to {output_file}")


# Input JSON file with 128-bit hashes
input_json = "file_metadata.json"

# Output JSON file with truncated 64-bit hashes
output_json = "file_metadata_truncated.json"

# Directory to scan
directory_to_scan = r"D:\Stuff\Images"

# Calculate metadata and save to JSON
calculate_file_metadata_initial(directory_to_scan, input_json, hash_algorithm="md5")

# Truncate hashes and save to a new file
truncate_hashes_in_json(input_json, output_json)

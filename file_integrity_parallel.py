import os
import json
import hashlib
import numpy as np
import multiprocessing as mp
from mp_shared_array import MemorySharedNumpyArray
from tqdm import tqdm


def load_baseline_metadata(json_file, shared_array):
    with open(json_file, "r") as f:
        baseline_metadata = json.load(f)

    np_array = shared_array.get_numpy_handle()
    shared_array.get_lock().acquire()  # locking the shared memory array for reading

    for i, entry in enumerate(baseline_metadata):
        np_array[i, 0] = entry["hash"]
        np_array[i, 1] = entry["size"]
        np_array[i, 2] = entry["timestamp"]

    shared_array.get_lock().release()  # release the lock after reading the data


def calculate_file_metadata(file_path, hash_algorithm="md5"):
    try:
        hash_func = hashlib.new(hash_algorithm)
        with open(file_path, "rb") as f:
            while chunk := f.read(8192):
                hash_func.update(chunk)
        file_hash = hash_func.hexdigest()
        hash_numeric = int(file_hash, 16) & ((1 << 64) - 1)
        file_size = os.path.getsize(file_path)
        file_timestamp = os.path.getmtime(file_path)
        return {"hash": hash_numeric, "size": file_size, "timestamp": file_timestamp}
    except FileNotFoundError:
        return None


def producer_process(directory, queue, stop_event, buffer_size):
    """
    Producer process to read files from the directory and load them into the queue.

    Args:
        directory (str): Path to the directory to scan.
        queue (mp.Queue): Queue to store file paths.
        stop_event (mp.Event): Event to signal when the producer is done.
        buffer_size (int): Maximum size of the queue.

    Returns:
        None
    """
    for root, _, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            queue.put(file_path)  # Add file path to the queue
            while queue.qsize() >= buffer_size:  # Wait if the queue is full
                pass
    stop_event.set()  # Signal that the producer is done


def worker_process(queue, shared_array, progress_counter, stop_event, alert_list):
    """
    Worker process to calculate file metadata and compare with baseline.

    Args:
        queue (mp.Queue): Queue containing file paths to process.
        shared_array (MemorySharedNumpyArray): Shared memory array for baseline metadata.
        progress_counter (mp.Value): Shared counter for progress tracking.
        stop_event (mp.Event): Event to signal when the producer is done.
        alert_list (mp.Manager().list): Shared list to store alerts.

    Returns:
        None
    """
    while not (queue.empty() and stop_event.is_set()):
        try:
            file_path = queue.get()
            if file_path == "STOP":
                break

            current_metadata = calculate_file_metadata(file_path)

            if current_metadata is None:
                with progress_counter.get_lock():
                    progress_counter.value += 1
                continue

            # Compare with baseline

            np_array = shared_array.get_numpy_handle()
            shared_array.get_lock().acquire()
            baseline_hashes = np_array[:, 0]
            baseline_sizes = np_array[:, 1]
            baseline_timestamps = np_array[:, 2]
            shared_array.get_lock().release()

            if current_metadata["hash"] not in baseline_hashes:
                alert_list.append(f"New file detected: {file_path}")
            else:
                idx = np.where(baseline_hashes == current_metadata["hash"])[0][0]
                if current_metadata["size"] != baseline_sizes[idx]:
                    alert_list.append(f"File size changed: {file_path}")
                if current_metadata["timestamp"] != baseline_timestamps[idx]:
                    alert_list.append(f"File timestamp changed: {file_path}")

            with progress_counter.get_lock():
                progress_counter.value += 1
        except Exception as e:
            alert_list.append(f"Error processing file {file_path}: {e}")
            with progress_counter.get_lock():
                progress_counter.value += 1


def main():
    directory_to_scan = r"E:\Images"
    baseline_json = "file_metadata_truncated.json"
    num_processes = mp.cpu_count() - 2
    buffer_size = 100

    num_files = sum(len(files) for _, _, files in os.walk(directory_to_scan))
    print(f"Total files in directory: {num_files}")

    shared_array = MemorySharedNumpyArray(
        dtype=np.float64, shape=(num_files, 3), sampling=1, lock=True
    )

    print("Loading baseline metadata...")
    load_baseline_metadata(baseline_json, shared_array)
    print("Baseline metadata loaded.")

    file_queue = mp.Queue()
    stop_event = mp.Event()
    progress_counter = mp.Value("i", 0)

    # Shared list for alerts
    manager = mp.Manager()
    alert_list = manager.list()

    # Start the producer process
    producer = mp.Process(target=producer_process, args=(directory_to_scan, file_queue, stop_event, buffer_size))
    producer.start()

    # Create a progress bar for file processing
    progress_bar = tqdm(total=num_files, desc="Processing Files", position=0)

    # Start worker processes
    processes = []
    for _ in range(num_processes):
        p = mp.Process(target=worker_process, args=(file_queue, shared_array, progress_counter, stop_event, alert_list))
        processes.append(p)
        p.start()

    # Update the progress bar in the main process
    while progress_counter.value < num_files:
        progress_bar.n = progress_counter.value
        progress_bar.refresh()

    # Wait for all processes to finish
    for p in processes:
        p.join()

    producer.join()
    progress_bar.close()

    # Print all alerts at the end
    print("\n--- Alerts ---")
    for alert in alert_list:
        print(alert)

    print("File comparison complete.")


if __name__ == "__main__":
    main()

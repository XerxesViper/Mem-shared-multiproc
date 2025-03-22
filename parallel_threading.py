import threading
from joblib import Parallel, delayed
from time import perf_counter

perf_counter_thread = perf_counter()


# Function to compute the sum of squares in a given range
def compute_sum_of_squares(start, end, results, index):
    total = sum(x * x for x in range(start, end))
    results[index] = total


# Number of threads
num_threads = 6

# Results array to store the sum of squares for each thread
results = [0] * num_threads

# Create and start threads
threads = []
for i in range(num_threads):
    start = i * 1000000
    end = (i + 1) * 1000000
    thread = threading.Thread(target=compute_sum_of_squares, args=(start, end, results, i))
    threads.append(thread)
    thread.start()

# Wait for all threads to complete
for thread in threads:
    thread.join()

# Sum up the results from all threads
total_sum_of_squares = sum(results)
print(f"Total sum of squares: {total_sum_of_squares}")

print("Total time for threading:", perf_counter() - perf_counter_thread)

perf_counter_joblib = perf_counter()


# Function to compute the sum of squares for a range of numbers
def compute_sum_of_squares(start, end):
    return sum(x * x for x in range(start, end))


# Number of jobs (equivalent to number of threads)
num_jobs = 6

# Define the ranges for each job
ranges = [(i * 1000000, (i + 1) * 1000000) for i in range(num_jobs)]

# Use joblib to parallelize the computation using threads
results = Parallel(n_jobs=num_jobs, backend='threading')(delayed(compute_sum_of_squares)(start, end) for start, end in ranges)

# Sum up the results from all jobs
total_sum_of_squares = sum(results)
print(f"Total sum of squares: {total_sum_of_squares}")

print("Total time for parallel computing:", perf_counter() - perf_counter_joblib)

from typing import List, Tuple
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
from collections import defaultdict, Counter
from time_tracker import track_execution_time

def process_lines(lines: List[str]) -> defaultdict:
    """
    Processes a chunk of lines from the JSON file, counting tweets per user per date.

    Parameters:
    - lines (List[str]): A list of strings, where each string is a JSON-encoded line from the file.

    Returns:
    - defaultdict: A dictionary with dates as keys and Counter objects as values,
      where each Counter object tracks the number of tweets per user for that date.
    """
    local_counts = defaultdict(Counter)
    for line in lines:
        record = json.loads(line)
        date = datetime.strptime(record['date'], '%Y-%m-%dT%H:%M:%S%z').date()
        username = record['user']['username']
        local_counts[date][username] += 1
    return local_counts

@track_execution_time
def q1_time(file_path: str, lines_per_chunk: int = 1000, num_workers: int = 4) -> List[Tuple[datetime.date, str]]:
    """
    Analyzes a JSON file in parallel to find the top 10 dates with the most active users.

    Parameters:
    - file_path (str): The path to the JSON file containing tweet data.
    - lines_per_chunk (int): The number of lines each thread/process will handle at once.
    - num_workers (int): The number of worker threads to use for parallel processing.

    Returns:
    - List[Tuple[datetime.date, str]]: A list of tuples, each containing a date and the username
      of the most active user on that date, sorted by the number of tweets in descending order.
      Only the top 10 dates are included.
    """
    
    chunks = []

    # Open the file and divide it into chunks based on lines_per_chunk
    with open(file_path, 'r') as f:
        chunk = []
        for line in f:
            chunk.append(line)
            if len(chunk) >= lines_per_chunk:
                chunks.append(chunk)
                chunk = []
        if chunk:
            chunks.append(chunk)

    results = defaultdict(Counter)
    
    # Use ThreadPoolExecutor to process each chunk in parallel
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        # Submit each chunk to the executor for processing
        futures = [executor.submit(process_lines, chunk) for chunk in chunks]
        for future in as_completed(futures):
            local_counts = future.result()
            # Aggregate results from all futures
            for date, counter in local_counts.items():
                results[date] += counter

    final_results = []
    # Compile final results, finding the most common user for each date
    for date, counter in results.items():
        most_common_user, count = counter.most_common(1)[0]
        final_results.append((date, most_common_user, count))

    # Sort and return the top 10 results
    top_results = sorted(final_results, key=lambda x: (-x[2], x[0]))[:10]
    return [(date, username) for date, username, _ in top_results]
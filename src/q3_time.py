from typing import List, Tuple
import json
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from time_tracker import track_execution_time

def process_chunk(chunk: List[str]) -> Counter:
    """
    Processes a chunk of tweet data to count mentions of each user.

    Parameters:
    - chunk (List[str]): A list of tweet data in JSON format, where each string is a single line.

    Returns:
    - Counter: A Counter object mapping usernames to their mention counts within the chunk.
    """
    chunk_users_counter = Counter()
    
    for line in chunk:
        record = json.loads(line)
        users = record['mentionedUsers']
        for user in users:
            username = user['username']
            chunk_users_counter[username] += 1
    
    return chunk_users_counter

@track_execution_time
def q3_time(file_path: str, num_workers: int = 4, lines_per_chunk: int = 10000) -> List[Tuple[str, int]]:
    """
    Analyzes a JSON file in parallel to find the top 10 most mentioned users.

    Parameters:
    - file_path (str): The path to the JSON file containing tweet data.
    - num_workers (int): The number of worker threads to use for parallel processing.
    - lines_per_chunk (int): The number of lines of tweet data each thread/process will handle.

    Returns:
    - List[Tuple[str, int]]: A list of tuples, each containing a username and its mention count,
      representing the top 10 most frequently mentioned users. Sorted by mention count in
      descending order.
    """
    chunks = []

    # Divide the file into chunks to be processed in parallel
    with open(file_path, 'r') as f:
        chunk = []
        for line in f:
            chunk.append(line)
            if len(chunk) >= lines_per_chunk:
                chunks.append(chunk)
                chunk = []
        if chunk:
            chunks.append(chunk)
    
    total_users_counter = Counter()
    
    # Use ThreadPoolExecutor to process chunks in parallel
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(process_chunk, chunk) for chunk in chunks]
        # Aggregate counts from each chunk as they complete
        for future in as_completed(futures):
            chunk_users_counter = future.result()
            total_users_counter.update(chunk_users_counter)
    
    # Find the top 10 most mentioned users across all chunks
    results = total_users_counter.most_common(10)
    
    return results

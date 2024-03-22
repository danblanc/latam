from typing import List, Tuple
import json
from collections import Counter
import emoji
from concurrent.futures import ThreadPoolExecutor, as_completed
from time_tracker import track_execution_time

def process_chunk(chunk: List[str]) -> Counter:
    """
    Processes a chunk of lines from the JSON file, counting emojis in each tweet.

    Parameters:
    - chunk (List[str]): A list of strings, where each string is a JSON-encoded line from the file.

    Returns:
    - Counter: A Counter object mapping each emoji to its count within the chunk.
    """
    chunk_emoji_counter = Counter()
    
    for line in chunk:
        record = json.loads(line)
        twit = record['content']
        # Extract emojis from the tweet
        emojis_list = emoji.emoji_list(twit)  
        for emoji_data in emojis_list:
            emoji_char = emoji_data['emoji']
            # Count each emoji
            chunk_emoji_counter[emoji_char] += 1  
    
    return chunk_emoji_counter

@track_execution_time
def q2_time(file_path: str, num_workers: int = 4, lines_per_chunk: int = 10000) -> List[Tuple[str, int]]:
    """
    Analyzes a JSON file containing tweets to find the top 10 most used emojis using parallel processing.

    Parameters:
    - file_path (str): The path to the JSON file containing tweet data.
    - num_workers (int): The number of worker threads to use for parallel processing.
    - lines_per_chunk (int): The number of lines each thread will process at once.

    Returns:
    - List[Tuple[str, int]]: A list of tuples, each containing an emoji (str) and its count (int),
      representing the top 10 most frequently occurring emojis in the dataset. The list is sorted
      by count in descending order.
    """
    chunks = []

    # Open the file and divide it into chunks for parallel processing
    with open(file_path, 'r') as f:
        chunk = []
        for line in f:
            chunk.append(line)
            # Chunk size check
            if len(chunk) >= lines_per_chunk:  
                chunks.append(chunk)
                chunk = []
        if chunk: 
            chunks.append(chunk)
    
    total_emoji_counter = Counter()
    
    # Use ThreadPoolExecutor to process each chunk in parallel
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        # Submit each chunk to a separate thread
        futures = [executor.submit(process_chunk, chunk) for chunk in chunks]
        for future in as_completed(futures):
            # Get results from each future
            chunk_emoji_counter = future.result() 
            # Aggregate results
            total_emoji_counter.update(chunk_emoji_counter)  
    
    # Determine the top 10 most common emojis and their counts
    results = total_emoji_counter.most_common(10)
    
    return results
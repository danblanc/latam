from typing import List, Tuple
import json
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from time_tracker import track_execution_time

def process_chunk(chunk):
    chunk_users_counter = Counter()
    
    for line in chunk:
        record = json.loads(line)
        users = record['mentionedUsers']
        if users:
            for user in users:
                username = user['username']
                chunk_users_counter[username] += 1
    
    return chunk_users_counter

@track_execution_time
def q3_time(file_path: str, num_workers: int = 4, lines_per_chunk = 10000) -> List[Tuple[str, int]]:
    chunks = []

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
    
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(process_chunk, chunk) for chunk in chunks]
        for future in as_completed(futures):
            chunk_users_counter = future.result()
            total_users_counter.update(chunk_users_counter)
    
    results = total_users_counter.most_common(10)
    
    return results
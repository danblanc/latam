from typing import List, Tuple
import json
from collections import Counter
import emoji
from concurrent.futures import ThreadPoolExecutor, as_completed
from time_tracker import track_execution_time

def process_chunk(chunk):
    chunk_emoji_counter = Counter()
    
    for line in chunk:
        record = json.loads(line)
        twit = record['content']
        emojis_list = emoji.emoji_list(twit)
        for emoji_data in emojis_list:
            emoji_char = emoji_data['emoji']
            chunk_emoji_counter[emoji_char] += 1
    
    return chunk_emoji_counter

@track_execution_time
def q2_time(file_path: str, num_workers: int = 4, lines_per_chunk = 10000) -> List[Tuple[str, int]]:
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
    
    total_emoji_counter = Counter()
    
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(process_chunk, chunk) for chunk in chunks]
        for future in as_completed(futures):
            chunk_emoji_counter = future.result()
            total_emoji_counter.update(chunk_emoji_counter)
    
    results = total_emoji_counter.most_common(10)
    
    return results
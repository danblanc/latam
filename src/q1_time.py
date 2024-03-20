from typing import List, Tuple
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
from collections import defaultdict, Counter
from time_tracker import track_execution_time


def process_lines(lines):
    local_counts = defaultdict(Counter)
    for line in lines:
        record = json.loads(line)
        date = datetime.strptime(record['date'], '%Y-%m-%dT%H:%M:%S%z').date()
        username = record['user']['username']
        local_counts[date][username] += 1
    return local_counts

@track_execution_time
def q1_time(file_path: str, lines_per_chunk = 1000, num_workers = 4) -> List[Tuple[datetime.date, str]]:
    
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

    results = defaultdict(Counter)
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(process_lines, chunk) for chunk in chunks]
        for future in as_completed(futures):
            local_counts = future.result()
            for date, counter in local_counts.items():
                results[date] += counter

    final_results = []
    for date, counter in results.items():
        most_common_user, count = counter.most_common(1)[0]
        final_results.append((date, most_common_user, count))

    top_results = sorted(final_results, key=lambda x: (-x[2], x[0]))[:10]
    return [(date, username) for date, username, _ in top_results]





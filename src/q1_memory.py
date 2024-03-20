from typing import List, Tuple
from datetime import datetime
import json
from collections import defaultdict, Counter
from time_tracker import track_execution_time

@track_execution_time
def q1_memory(file_path: str) -> List[Tuple[datetime.date, str]]:
    user_counts_per_date = defaultdict(Counter)

    with open(file_path, 'r') as f:
        for line in f:
            record = json.loads(line)
            date = datetime.strptime(record['date'], '%Y-%m-%dT%H:%M:%S%z').date()
            username = record['user']['username']
            user_counts_per_date[date][username] += 1

    results = []
    for date, counter in user_counts_per_date.items():
        most_common_user, count = counter.most_common(1)[0]
        results.append((date, most_common_user, count))

    top_results = sorted(results, key=lambda x: (-x[2], x[0]))[:10]
    return [(date, username) for date, username, _ in top_results]
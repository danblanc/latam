from typing import List, Tuple
import json
from collections import Counter
from time_tracker import track_execution_time

@track_execution_time
def q3_memory(file_path: str) -> List[Tuple[str, int]]:
    users_counter = Counter()

    with open(file_path, 'r') as f:
        for line in f:
            record = json.loads(line)
            
            users = record['mentionedUsers']
            
            if users:
                for user in users:
                    username = user['username']
                    users_counter[username] += 1

    results = users_counter.most_common(10)

    return results
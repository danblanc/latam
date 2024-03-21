from typing import List, Tuple
import json
from collections import Counter
import emoji
from time_tracker import track_execution_time

@track_execution_time
def q2_memory(file_path: str) -> List[Tuple[str, int]]:
    emoji_counter = Counter()

    with open(file_path, 'r') as f:
        for line in f:
            record = json.loads(line)
            
            twit = record['content']
            
            emojis_list = emoji.emoji_list(twit)
            
            for emoji_data in emojis_list:
                emoji_char = emoji_data['emoji']
                emoji_counter[emoji_char] += 1

    results = emoji_counter.most_common(10)

    return results
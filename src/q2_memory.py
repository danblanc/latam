from typing import List, Tuple
import json
from collections import Counter
import emoji
from time_tracker import track_execution_time

@track_execution_time
def q2_memory(file_path: str) -> List[Tuple[str, int]]:
    """
    Analyzes a JSON file containing tweets to find the top 10 most used emojis.

    Parameters:
    - file_path (str): The path to the JSON file containing tweet data. Each line in the file
      should be a JSON object representing a tweet, with a key 'content' that contains the
      text of the tweet.

    Returns:
    - List[Tuple[str, int]]: A list of tuples, each containing an emoji (str) and its count (int),
      representing the top 10 most frequently occurring emojis in the tweet dataset. The list
      is sorted by count in descending order.
    """
    
    # Initialize a counter to track emoji occurrences
    emoji_counter = Counter()

    # Open and read the JSON file line by line
    with open(file_path, 'r') as f:
        for line in f:
            # Parse the JSON data from each line
            record = json.loads(line)
            
            # Extract the tweet content
            twit = record['content']
            
            # Identify and list all emojis in the tweet content
            emojis_list = emoji.emoji_list(twit)
            
            # Increment the count for each emoji found
            for emoji_data in emojis_list:
                emoji_char = emoji_data['emoji']
                emoji_counter[emoji_char] += 1

    # Determine the top 10 most common emojis and their counts
    results = emoji_counter.most_common(10)

    return results

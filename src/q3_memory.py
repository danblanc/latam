from typing import List, Tuple
import json
from collections import Counter
from time_tracker import track_execution_time

@track_execution_time
def q3_memory(file_path: str) -> List[Tuple[str, int]]:
    """
    Analyzes a JSON file containing tweet data to find the top 10 most mentioned users.

    Parameters:
    - file_path (str): The path to the JSON file containing tweet data. Each line in the file
      should be a JSON object representing a tweet, with a key 'mentionedUsers' that contains a
      list of user objects, each having a 'username' key.

    Returns:
    - List[Tuple[str, int]]: A list of tuples, each containing a username (str) and its mention
      count (int), representing the top 10 most frequently mentioned users in the tweet dataset.
      The list is sorted by mention count in descending order.
    """
    
    # Initialize a counter to track the number of mentions for each user
    users_counter = Counter()

    # Open and read the JSON file line by line
    with open(file_path, 'r') as f:
        for line in f:
            # Parse the JSON data from each line
            record = json.loads(line)
            
            # Extract the list of mentioned users in the tweet
            users = record['mentionedUsers']
            
            # Increment the count for each mentioned user
            for user in users:
                username = user['username']
                # Ensure username is not empty
                if username:
                    users_counter[username] += 1

    # Determine the top 10 most mentioned users and their counts
    results = users_counter.most_common(10)

    return results

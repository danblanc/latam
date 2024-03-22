from typing import List, Tuple
from datetime import datetime
import json
from collections import defaultdict, Counter
from time_tracker import track_execution_time

@track_execution_time
def q1_memory(file_path: str) -> List[Tuple[datetime.date, str]]:
    """
    Analyzes a JSON file containing tweet data to find the top 10 dates with the most active users.

    Parameters:
    - file_path (str): The path to the JSON file containing tweet data.

    Returns:
    - List[Tuple[datetime.date, str]]: A list of tuples, each containing a date and the username
      of the most active user on that date, sorted by the number of tweets in descending order.
      Only the top 10 dates are included.

    The JSON file should have a structure where each line is a JSON object with at least
    'date' and 'user' keys, where 'user' is an object containing a 'username' key.
    """

    # Initialize a dictionary to count tweets per user per date
    user_counts_per_date = defaultdict(Counter)

    # Open and read the JSON file line by line
    with open(file_path, 'r') as f:
        for line in f:
            # Parse each line as a JSON object
            record = json.loads(line)
            # Extract and parse the date, keeping only the date part
            date = datetime.strptime(record['date'], '%Y-%m-%dT%H:%M:%S%z').date()
            # Extract the username
            username = record['user']['username']
            # Increment the tweet count for this user on this date
            user_counts_per_date[date][username] += 1

    # Prepare a list to hold the results
    results = []
    # For each date, find the user with the most tweets
    for date, counter in user_counts_per_date.items():
        most_common_user, count = counter.most_common(1)[0]
        results.append((date, most_common_user, count))

    # Sort the results by tweet count (descending) and date, and keep only the top 10
    top_results = sorted(results, key=lambda x: (-x[2], x[0]))[:10]
    
    # Return the top results, excluding the tweet counts
    return [(date, username) for date, username, _ in top_results]
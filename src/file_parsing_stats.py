import io
import json

from time import time


# Source file: https://github.com/json-iterator/test-data/raw/master/large-file.json
# File size: 24 MB
# Type of data: Github event log from a slice of time
# Credit: taowen (Github)


def process_json_file(file_path):
    with open(file_path, 'r') as file_obj:
        start_time = time()
        fi = io.FileIO(file_obj.fileno())
        fo = io.BufferedReader(fi)
        content = fo.read()
        read_finished_time = time()
        json_obj = json.loads(content)
        parsing_finished_time = time()
        return read_finished_time - start_time, parsing_finished_time - read_finished_time


if __name__ == "__main__":
    num_trials = 10
    file_paths = ['../resources/large-file.json', '../resources/small-file.json'] * num_trials
    read_stats = []
    parsing_stats = []

    for file_path in file_paths:
        for trial_num in range(num_trials):
            read_time, parsing_time = process_json_file('../resources/large-file.json')
            read_stats.append(read_time)
            parsing_stats.append(parsing_time)
        print(f"Average read time for {file_path} is "
              f"{round(sum(read_stats) / len(read_stats), 2)}. Max: {round(max(read_stats), 2)}, "
              f"Min: {round(min(read_stats), 2)}")
        print(f"Average processing time for {file_path} is "
              f"{round(sum(parsing_stats) / len(parsing_stats), 2)}. Max: {round(max(parsing_stats), 2)}, "
              f"Min: {round(min(parsing_stats),2)}")

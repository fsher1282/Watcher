from threading import Thread
from queue import Queue
from pymongo import MongoClient
import datetime

local_jobs_collection = MongoClient()["LocalOverlord"]['Jobs']


def lambda_function():
    print("I ran")


def worker(q):
    while True:
        item = q.get()
        if item is None:
            break
        print('We Ran')
        lambda_function()
        q.task_done()


def poll_local_status(last_checked_time, q):
    new_entries = local_jobs_collection.find({})
    for entry in new_entries:
        if 'inserted_at' in entry:
            entry_time = entry['inserted_at']
            # Check if entry_time is string and convert it to datetime
            if isinstance(entry_time, str):
                entry_time = datetime.datetime.strptime(entry_time, "%Y-%m-%d %H:%M:%S.%f")
            # Now entry_time is ensured to be a datetime object
            if entry_time > last_checked_time:
                q.put(entry)
                last_checked_time = entry_time

    return last_checked_time  # Return the updated last_checked_time



if __name__ == "__main__":
    # Read last_checked_time_str from a file
    with open("last_checked_time1.txt", "r") as f:
        last_checked_time_str = f.read().strip()

    last_checked_time = datetime.datetime.strptime(last_checked_time_str, "%Y-%m-%d %H:%M:%S.%f")

    q = Queue()
    t = Thread(target=worker, args=(q,))
    t.start()

    last_checked_time = poll_local_status(last_checked_time, q)  # Update last_checked_time

    # Write updated last_checked_time to the file
    with open("last_checked_time.txt", "w") as f:
        f.write(last_checked_time.strftime("%Y-%m-%d %H:%M:%S.%f"))

    q.join()  # Block until all tasks are done
    q.put(None)  # Stop worker
    t.join()

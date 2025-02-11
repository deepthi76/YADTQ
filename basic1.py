# basic.py
from yadtq import create_yadtq
from yadtq.api.client import TaskClient
import time

def main():
    broker, result_store = create_yadtq()
    client = TaskClient(broker, result_store)

    tasks = [
        ('add', (1, 3)),
        ('subtract', (10, 4)),
        ('multiply', (4, 6)),
        ('add', (7, 2)),
        ('subtract', (15, 8)),
        ('multiply', (5, 5))
    ]

    task_ids = []

    for task_name, args in tasks:
        task_id = client.submit(task_name, *args)
        task_ids.append(task_id)
        print(f"Submitted {task_name}{args} with ID: {task_id}")

    results = {}
    while task_ids:
        for task_id in task_ids[:]:  
            try:
                result = client.wait_for_result(task_id)
                if result:
                    print(f"Task {task_id} completed: {result}")
                    results[task_id] = result
                    task_ids.remove(task_id)
            except Exception as e:
                print(f"Error checking task {task_id}: {e}")
        
        time.sleep(1)

    print("All tasks completed.")

if __name__ == "__main__":
    main()

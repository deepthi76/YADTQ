# basic1.py
from yadtq import create_yadtq
from yadtq.api.client import TaskClient
import time

def main():
    broker, result_store = create_yadtq()
    client = TaskClient(broker, result_store)

    # Define tasks with intentional duplicates
    tasks = [
        ('add', (12, 3)),
        ('add', (12, 3)),  # Duplicate
        ('subtract', (101, 4)),
        ('multiply', (42, 6)),
        ('multiply', (42, 6)),  # Duplicate
    ]

    task_ids = []
    task_id_mapping = {}
    submission_times = {}  # Track when tasks were submitted
    
    for task_name, args in tasks:
        task_signature = f"{task_name}:{args}"
        
        if task_signature in task_id_mapping:
            print(f"\n🔄 Duplicate task detected: {task_signature}")
            task_id = task_id_mapping[task_signature]
            print(f"↪️ Reusing existing task ID: {task_id}")
            
            # Check if result already exists in Redis
            existing_result = client.get_result(task_id)
            if existing_result and existing_result['status'] == 'success':
                print(f"📝 Found cached result: {existing_result}")
        else:
            task_id = client.submit(task_name, *args)
            task_id_mapping[task_signature] = task_id
            submission_times[task_id] = time.time()
            print(f"\n📤 Submitted new task: {task_signature}")
            print(f"📎 New task ID: {task_id}")
            
        task_ids.append(task_id)

    print("\nWaiting for results...")
    results = {}
    while task_ids:
        for task_id in task_ids[:]:
            result = client.get_result(task_id)
            if result and result['status'] in ['success', 'failed']:
                execution_time = time.time() - submission_times.get(task_id, time.time())
                if execution_time < 1.0:  # If result came back very quickly, it was cached
                    print(f"\n🚀 Got cached result for task {task_id}:")
                else:
                    print(f"\n⚙️ Got new execution result for task {task_id}:")
                print(f"Result: {result}")
                results[task_id] = result
                task_ids.remove(task_id)
        time.sleep(1)

    print("\n📊 Final Results Summary:")
    for task_id, result in results.items():
        print(f"\nTask ID: {task_id}")
        print(f"Status: {result['status']}")
        print(f"Worker: {result.get('worker_id', 'N/A')}")
        print(f"Result: {result.get('result', 'N/A')}")
        print(f"Timestamp: {result.get('timestamp', 'N/A')}")

if __name__ == "__main__":
    main()
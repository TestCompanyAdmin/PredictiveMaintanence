from datetime import datetime


def print_run_metadata():
    now = datetime.utcnow()

    print("\nRUN METADATA")
    print("-" * 60)
    print(f"UTC time: {now.isoformat()}")
    print("-" * 60)
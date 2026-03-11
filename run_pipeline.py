import subprocess
import sys


SCRIPTS = [
    "src/export_raw.py",
    "src/validate_and_clean.py",
    "src/build_features.py",
]


def run_script(script_path: str) -> None:
    print(f"\n--- Running {script_path} ---")
    result = subprocess.run([sys.executable, script_path], check=True)
    if result.returncode == 0:
        print(f"Finished: {script_path}")


def main() -> None:
    for script in SCRIPTS:
        run_script(script)

    print("\nPipeline completed successfully.")
    print("Generated files:")
    print("- data/raw.parquet")
    print("- data/clean.parquet")
    print("- data/features.parquet")
    print("- reports/quality_report.json")


if __name__ == "__main__":
    main()
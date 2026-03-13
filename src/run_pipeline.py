import traceback

from src.ingestion.export_raw import main as export_raw_main
from src.validation.validate_schema import main as validate_schema_main
from src.validation.print_validation import main as print_validation_main
from src.validation.quality_flags import main as quality_flags_main
from src.cleaning.clean_data import main as clean_data_main
from src.features.build_features import main as build_features_main
from src.validation.write_quality_report import main as write_quality_report_main
from src.utils.run_metadata import print_run_metadata
from src.utils.bootstrap import ensure_project_directories


def main():
    print("=" * 60)
    print("STARTING PREDICTIVE MAINTENANCE PIPELINE")
    print("=" * 60)
    print_run_metadata()
    ensure_project_directories()

    steps = [
        ("STEP 1: export raw", export_raw_main),
        ("STEP 2: validate schema", validate_schema_main),
        ("STEP 3: print validation", print_validation_main),
        ("STEP 4: apply quality flags", quality_flags_main),
        ("STEP 5: write quality report", write_quality_report_main),
        ("STEP 6: clean data", clean_data_main),
        ("STEP 7: build features", build_features_main),
    ]

    for step_name, step_func in steps:
        print(f"\n{step_name}")
        try:
            step_func()
            print(f"{step_name} -> OK")
        except Exception:
            print("\n" + "=" * 60)
            print(f"PIPELINE FAILED IN: {step_name}")
            print("=" * 60)
            traceback.print_exc()
            raise

    print("\n" + "=" * 60)
    print("PIPELINE FINISHED SUCCESSFULLY")
    print("=" * 60)


if __name__ == "__main__":
    main()
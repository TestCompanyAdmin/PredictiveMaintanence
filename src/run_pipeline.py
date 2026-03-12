from src.ingestion.export_raw import main as export_raw_main
from src.validation.validate_schema import main as validate_schema_main
from src.validation.validate_values import main as validate_values_main
from src.validation.quality_flags import main as quality_flags_main
from src.cleaning.clean_data import main as clean_data_main
from src.features.build_features import main as build_features_main
from src.validation.write_quality_report import main as write_quality_report_main


def main():
    print("STEP 1: export raw")
    export_raw_main()

    print("\nSTEP 2: validate schema")
    validate_schema_main()

    print("\nSTEP 3: validate values")
    validate_values_main()

    print("\nSTEP 4: apply quality flags")
    quality_flags_main()

    print("\nSTEP 5: write quality report")
    write_quality_report_main()

    print("\nSTEP 6: clean data")
    clean_data_main()

    print("\nSTEP 7: build features")
    build_features_main()

    print("\nPipeline finished successfully.")


if __name__ == "__main__":
    main()
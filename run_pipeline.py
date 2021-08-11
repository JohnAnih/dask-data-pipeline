import sys
import argparse
import warnings

from processing.data_pipeline import DataPipeline

if __name__ == "__main__":
    warnings.simplefilter("ignore")
    parser = argparse.ArgumentParser(description="Implement the data pipeline")
    parser.add_argument(
        "--filename",
        "-input",
        default="./data/raw/pp-2021.csv",
        help="The CSV price paid data file",
    )
    parser.add_argument(
        "--output-folder",
        "-output",
        default="./data/processed/data",
        help="To save the data as a newline delimited JSON format",
    )
    args = parser.parse_args()

    try:
        data_pipe = DataPipeline(args.filename)

    except FileNotFoundError:
        print(
            "Oops! File not found. Ensure you saved the data in the data/raw directory"
        )
        sys.exit(1)

    print("Head of the data: ")
    print("-" * 20)
    print(" ")
    print(data_pipe.data().head(10))
    print(" ")
    print("-" * 20)

    print("About to save data as a JSON file")
    data_pipe.save_data_as_json(args.output_folder)

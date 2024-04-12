import os
import pandas as pd


def combine_output_files():
    output_folder = 'output'
    output_csv = 'output.csv'
    output_files = [file for file in os.listdir(output_folder) if file.endswith('.csv')]

    # Check if any CSV files exist in the output folder
    if not output_files:
        print("No CSV files found in the output folder.")
        return

    # Initialize an empty DataFrame to hold the combined data
    combined_df = pd.DataFrame()

    # Loop through each CSV file and append its data to the combined DataFrame
    for file in output_files:
        file_path = os.path.join(output_folder, file)
        # Read CSV file, removing leading commas from each row
        df = pd.read_csv(file_path, header=None, names=[str(i) for i in range(100)])

        # Convert all columns to strings
        df = df.astype(str)

        # Remove leading commas from each cell
        df = df.apply(lambda x: x.str.lstrip(','))  # Remove leading commas

        combined_df = combined_df.append(df, ignore_index=True)

    # Write the combined DataFrame to output.csv
    combined_df.to_csv(output_csv, index=False)
    print(f"Combined data from {len(output_files)} CSV files into {output_csv}.")


def main():
    combine_output_files()


if __name__ == "__main__":
    main()

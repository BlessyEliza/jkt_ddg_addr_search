import os
import pandas as pd

def combine_output_files():
    output_folder = 'output\p1'
    output_csv = 'output.csv'
    output_files = [file for file in os.listdir(output_folder) if file.endswith('.csv')]

    if not output_files:
        print("No CSV files found in the output folder.")
        return

    combined_df = pd.DataFrame()  # Initialize outside the loop

    for file in output_files:
        file_path = os.path.join(output_folder, file)
        df = pd.read_csv(file_path, header=None, names=[str(i) for i in range(100)])
        df = df.astype(str)
        df = df.apply(lambda x: x.str.lstrip(','))

        df.rename(columns={0: 'id'}, inplace=True)
        df = df.iloc[:, :4]
        df.columns = ['id', 'website', 'results', 'extra']

        combined_df = pd.concat([combined_df, df], ignore_index=True)  # Use concat instead of append

    combined_df.to_csv(output_csv, index=False)
    print(f"Combined data from {len(output_files)} CSV files into {output_csv}.")

def main():
    combine_output_files()

if __name__ == "__main__":
    main()

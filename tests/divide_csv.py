import pandas as pd

# Load the CSV file into a DataFrame
df = pd.read_csv('Webscrap-Tranche2_Priority1.csv')

# Calculate the number of rows per file
rows_per_file = len(df) // 5
remainder = len(df) % 5

# Loop to create and save the 5 files
start_idx = 0
for i in range(5):
    end_idx = start_idx + rows_per_file
    if i < remainder:
        end_idx += 1  # Add 1 to distribute the remainder across the files

    # Create a new DataFrame slice for this file
    df_slice = df.iloc[start_idx:end_idx]

    # Save the slice to a new CSV file
    df_slice.to_csv(f'output/p1/output_file_{i + 1}.csv', index=False)

    # Update the start index for the next file
    start_idx = end_idx

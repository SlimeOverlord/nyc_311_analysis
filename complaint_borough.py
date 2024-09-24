from argparse import ArgumentParser
import pandas as pd
import dask.dataframe as dd

def main():
    parser = ArgumentParser()
    parser.add_argument("-i", help="The input file", required=True)
    parser.add_argument("-s", help="The start date", required=True)
    parser.add_argument("-e", help="The end date", required=True)
    parser.add_argument("-o", help="The output file", required=False)
    args = parser.parse_args()

    print_complaints_per_borough(args.i, args.s, args.e, args.o)

def print_complaints_per_borough(input_file, start_date, end_date, output_file):

    # Setting the start and end dates
    start_date = pd.to_datetime(start_date, format="%d/%m/%Y")
    end_date = pd.to_datetime(end_date, format="%d/%m/%Y")

    # Reading the input file and putting it in a Pandas dataframe
    df = dd.read_csv(input_file, usecols = [1, 2, 5, 23], header=None)
    nyc_df = df.compute()

    # Transforming the datetime elements of the dataframe into actual datetime objects
    nyc_df[1] = pd.to_datetime(nyc_df[1], errors='coerce')
    nyc_df[2] = pd.to_datetime(nyc_df[2], errors='coerce')

    # Keeping only the entries that have a start date and end date between the parameter start date and end date
    date_range_nyc_df = nyc_df[(nyc_df[1] >= start_date) & (nyc_df[2] <= end_date)]

    # Renaming the columns
    date_range_nyc_df = date_range_nyc_df.rename(columns={1 : 'Start Date', 2 : 'End Date', 5 : 'Complaint Type', 23 : 'Borough'})

    # Removing the number in front of the borough names
    date_range_nyc_df['Borough'] = date_range_nyc_df['Borough'].str.extract(r'(\D+)')

    # Counting the occurrences of each complaint type per borough
    borough_complaints_count_df = date_range_nyc_df[['Complaint Type', 'Borough']].value_counts().to_frame('Count').reset_index()

    # Cleaning up the entries in the Borough column
    borough_complaints_count_df['Borough'] = borough_complaints_count_df['Borough'].str.replace(r'Unspecified\s*', '', regex=True)
    borough_complaints_count_df = borough_complaints_count_df.map(lambda x: x.strip() if isinstance(x, str) else x)
    borough_complaints_count_df = borough_complaints_count_df[borough_complaints_count_df['Borough'] != ""]

    # Setting the complaint type entries to lowercase, so that we can sort by them
    borough_complaints_count_df['Complaint Type'] = borough_complaints_count_df['Complaint Type'].str.lower()
    borough_complaints_df_sorted = borough_complaints_count_df.sort_values(by=['Complaint Type'])

    # Summing the elements of the Count column, as there are situations where we have 2 entries of the same complaint type and borough
    borough_complaints_df_final = borough_complaints_df_sorted.groupby(['Complaint Type', 'Borough'], as_index=False)['Count'].sum()

    # Outputting the file or sending it to the output file
    if output_file is not None:
        borough_complaints_df_final.to_csv(output_file, index=False)

    else:
        print("complaint type, borough, count")
        for index in borough_complaints_df_final.index:
            print(f"{borough_complaints_df_final['Complaint Type'][index]}, {borough_complaints_df_final['Borough'][index]}, {borough_complaints_df_final['Count'][index]}")
              
if __name__ == "__main__":
    main()
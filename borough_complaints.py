from argparse import ArgumentParser
import csv
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

    start_date = pd.to_datetime(start_date, format="%d/%m/%Y")
    end_date = pd.to_datetime(end_date, format="%d/%m/%Y")

    df = dd.read_csv(input_file, usecols = [1, 2, 5, 16], header=None)

    nyc_df = df.compute()

    nyc_df[1] = pd.to_datetime(nyc_df[1], errors='coerce')
    nyc_df[2] = pd.to_datetime(nyc_df[2], errors='coerce')

    date_range_nyc_df = nyc_df[(nyc_df[1] >= start_date) & (nyc_df[2] <= end_date)]

    print(date_range_nyc_df.head)    

if __name__ == "__main__":
    main()
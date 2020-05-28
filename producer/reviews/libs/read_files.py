"""
filename: read_files.py
name: read_files

description:
this is the function responsible to read files and perform some enhancements on the data.
use pandas and numpy to read data from a csv file and format to a dictionary
"""

# import libraries
import pandas as pd
from configs import config

# pandas config
pd.set_option('display.max_rows', 100000)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


class CSV:

    def __init__(self):
        self.ds_reviews = config.ds_reviews

    def csv_reader(self, gen_dt_rows):

        # reading files
        get_data = pd.read_csv(self.ds_reviews)

        # fixing column names
        get_data.columns = get_data.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')', '')

        # select column ordering
        df = get_data[['review_id', 'business_id', 'user_id', 'stars', 'useful', 'date']].head(gen_dt_rows)

        # convert to dictionary
        df_dict = df.to_dict('records')

        return df_dict

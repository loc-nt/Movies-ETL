import json
import pandas as pd
import numpy as np
import re
from sqlalchemy import create_engine
from config import db_password
import sys
import psycopg2
import time
import Challenge as ch

file_dir = "C:/Users/LocNguyen/OneDrive - stok LLC/Documents/Loc Nguyen/Online Data Analytics Bootcamp/Module 8/Movies-ETL/Resources/"

# Load JSON:
with open(f'{file_dir}/wikipedia.movies.json', mode='r') as file:
    wiki_raw = json.load(file)
# Load csv
kaggle_raw = pd.read_csv(f'{file_dir}movies_metadata.csv')
ratings_raw = pd.read_csv(f'{file_dir}ratings.csv')

# database link: "postgres://[user]:[password]@[location]:[port]/[database]"
db_string = f"postgres://postgres:{db_password}@127.0.0.1:5432/movie_data"
# Create the database engine with the following:
engine = create_engine(db_string)

ch.master_ETL(wiki_raw, kaggle_raw, ratings_raw)
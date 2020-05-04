import json
import pandas as pd
import numpy as np
import re
from sqlalchemy import create_engine
from config import db_password
import sys
# !{sys.executable} -m pip install psycopg2-binary
import time
import Challenge as ch

file_dir = "C:/Users/LocNguyen/OneDrive - stok LLC/Documents/Loc Nguyen/Online Data Analytics Bootcamp/Module 8/Movies-ETL/Resources/"

# file paths - user providing:
wiki_path = f'{file_dir}/wikipedia.movies.json'
kaggle_path = f'{file_dir}movies_metadata.csv'
ratings_path = f'{file_dir}ratings.csv'

ch.master_ETL(wiki_path, kaggle_path, ratings_path)
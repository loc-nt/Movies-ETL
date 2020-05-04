#!/usr/bin/env python
# coding: utf-8

# # Functions for Challenge Module 8:

# In[1]:


import json
import pandas as pd
import numpy as np
import re
from sqlalchemy import create_engine
from config import db_password
import sys
get_ipython().system('{sys.executable} -m pip install psycopg2-binary')
import time


# In[2]:


# create clean_movie() function:
def clean_movie(movie):
    movie = dict(movie) #create a non-destructive copy of movie
    
    # Handle the Alternative Titles
        ## Step 1: Make an empty dict to hold all of the alternative titles
    alt_titles = {}
    
        ## Step 2: Loop through a list of all alternative title keys.
    for key in ['Also known as','Arabic','Cantonese','Chinese','French',
                'Hangul','Hebrew','Hepburn','Japanese','Literally',
                'Mandarin','McCune–Reischauer','Original title','Polish',
                'Revised Romanization','Romanized','Russian',
                'Simplified','Traditional','Yiddish']:
            ### 2a: Check if the current key exists in the movie object, remove the key-value pair and add to the alternative titles dictionary.
        if key in movie:
            alt_titles[key] = movie[key]
            movie.pop(key)
            
        ## Step 3: After looping through every key, add the 'alternative titles' dict to the movie object.
    if len(alt_titles) > 0:
        movie['alt_titles'] = alt_titles
    
    
    # Change column name function - in case the record has 'Directed by', instead of 'Director':
    def change_column_name(old_name, new_name):
        if old_name in movie:
            movie[new_name] = movie.pop(old_name)
            
    change_column_name('Directed by', 'Director')        
    change_column_name('Adaptation by', 'Writer(s)')
    change_column_name('Country of origin', 'Country')
    change_column_name('Distributed by', 'Distributor')
    change_column_name('Edited by', 'Editor(s)')
    change_column_name('Length', 'Running time')
    change_column_name('Original release', 'Release date')
    change_column_name('Music by', 'Composer(s)')
    change_column_name('Produced by', 'Producer(s)')
    change_column_name('Producer', 'Producer(s)')
    change_column_name('Productioncompanies ', 'Production company(s)')
    change_column_name('Productioncompany ', 'Production company(s)')
    change_column_name('Released', 'Release Date')
    change_column_name('Release Date', 'Release date')
    change_column_name('Screen story by', 'Writer(s)')
    change_column_name('Screenplay by', 'Writer(s)')
    change_column_name('Story by', 'Writer(s)')
    change_column_name('Theme music composer', 'Composer(s)')
    change_column_name('Written by', 'Writer(s)')
    
    return movie


# In[3]:


# Create a function to convert the 2 forms into numbers for Box Office:
def parse_dollars(s):
    # if s is not a string, return NaN
    if type(s) != str:
        return np.nan

    # if input is of the form $###.# million
    if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags=re.IGNORECASE):

        # remove dollar sign and " million"
        s = re.sub('\$|\s|[a-zA-Z]','', s)

        # convert to float and multiply by a million
        value = float(s) * 10**6

        # return value
        return value

    # if input is of the form $###.# billion
    elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags=re.IGNORECASE):

        # remove dollar sign and " billion"
        s = re.sub('\$|\s|[a-zA-Z]','', s)

        # convert to float and multiply by a billion
        value = float(s) * 10**9

        # return value
        return value

    # if input is of the form $###,###,###
    elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags=re.IGNORECASE):

        # remove dollar sign and commas
        s = re.sub('\$|,','', s)

        # convert to float
        value = float(s)

        # return value
        return value

    # otherwise, return NaN
    else:
        return np.nan


# In[4]:


### 1. Create function to clean Wiki_movies:
def clean_wiki_movies(wiki_movies_raw):
    # More filtering: movies should not have episodes
    wiki_movies = [movie for movie in wiki_movies_raw
                   if ('Director' in movie or 'Directed by' in movie)
                       and 'imdb_link' in movie
                       and 'No. of episodes' not in movie]

    # make a list of dict of 'cleaned movies' with a list comprehension:
    clean_movies = [clean_movie(movie) for movie in wiki_movies]
    wiki_movies_df = pd.DataFrame(clean_movies)

    # Create new column 'imdb_id' then drop duplicates:
    wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')
    wiki_movies_df.drop_duplicates(subset='imdb_id', inplace=True)

    # Remove Mostly Null Columns:
    wiki_columns_to_keep = [column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df) * 0.9]
    wiki_movies_df = wiki_movies_df[wiki_columns_to_keep]

    # 2 forms regex for currency:
    form_one = r'\$\s*\d+\.?\d*\s*[mb]illi?on'
    form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)'

    # Cleaning Box Office column:
    box_office = wiki_movies_df['Box office'].dropna()
        # join list to convert into str:
    box_office = box_office.apply(lambda x: ' '.join(x) if type(x) == list else x)
        # Replacing any string, that starts with a dollar sign and ends with a hyphen, with a $ sign.
    box_office = box_office.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)    
        # Create a new column named 'box_office' in the wiki_movies_df to convert the Box Office into  numbers
    wiki_movies_df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
        # Drop old column:
    wiki_movies_df.drop('Box office', axis=1, inplace=True)

    # Cleaning Budget column:
    budget = wiki_movies_df['Budget'].dropna()
        # join list to convert into str:
    budget = budget.map(lambda x: ' '.join(x) if type(x) == list else x)
        # Then remove any values between a dollar sign and a hyphen (for budgets given in ranges):
    budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
        # Replace the citation references ("[3]", "[4]"...) with " "
    budget = budget.str.replace(r'\[\d+\]\s*', '')
        # Create a new column named 'budget' in the wiki_movies_df to convert the 'Budget' into numbers
    wiki_movies_df['budget'] = budget.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
        # drop the original Budget column.
    wiki_movies_df.drop('Budget', axis=1, inplace=True)

    # Cleaning Release Date column:
    release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
        # 4 forms of date:
        # 1. Full month name, one- to two-digit day, four-digit year (i.e., January 1, 2000)
    date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
        # 2. Four-digit year, two-digit month, two-digit day, with any separator (i.e., 2000-01-01)
    date_form_two = r'\d{4}.[01]\d.[123]\d'
        # 3. Full month name, four-digit year (i.e., January 2000)
    date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
        # 4. Four-digit year
    date_form_four = r'\d{4}'
        # Create a new column named 'release_date' in the wiki_movies_df to convert the Release Date into numbers
    wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format=True)

    # CLeaning Running Time column:
    running_time = wiki_movies_df['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
        # catch all hours & min phrase:    
    running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')
        # convert this df into numeric (coerce: returns error (non-Number) into NaN)
    running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)
        # Create a new column named 'running_time' in the wiki_movies_df and hours to min
    wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)
        # drop the raw Running time column:
    wiki_movies_df.drop('Running time', axis=1, inplace=True)
    
    return wiki_movies_df


# In[5]:


### 2. Create function to clean Kaggle data:
def clean_kaggle_data(kaggle_metadata):
    # Keep only False 'adult', then drop the column:
    kaggle_metadata = kaggle_metadata[kaggle_metadata['adult'] == 'False'].drop('adult',axis='columns')
    # Convert data types:
        # check if the string value is true? if yes, return a boolean value TRUE, else FALSE.
    kaggle_metadata['video'] = kaggle_metadata['video'] == 'True'
        
        # Convert numeric, and 'ignore' with warning to our attention if any errors:
    for i in ['budget', 'id', 'popularity']:
        try:
            kaggle_metadata[i] = pd.to_numeric(kaggle_metadata[i], errors='raise')
        except:
            print(f"Warning: some {i} record(s) are unable to convert to number.")            
        
        # convert datetime:
    try:
        kaggle_metadata['release_date'] = pd.to_datetime(kaggle_metadata['release_date'])
    except:
        print(f"Warning: some release_date record(s) are unable to convert to datetime, so bad data might not be dropped.")
    
    return kaggle_metadata


# In[6]:


### 3. Create a function to join the 2 cleaned df into movie_df
def join_movies_df(wiki_movies_raw, kaggle_metadata):
    cleaned_wiki = clean_wiki_movies(wiki_movies_raw)
    cleaned_kaggle = clean_kaggle_data(kaggle_metadata)
    
    # Inner join 2 tables. If failed, stop the function and return the error:
    joining_key = 'imdb_id'
    try:
        movies_df = pd.merge(cleaned_wiki, cleaned_kaggle, on=joining_key, suffixes=['_wiki','_kaggle'])
    except:
        print(f"ETL run failed when joining the dataset between wiki_movies and kaggle_metadata. Please check if {joining_key} is the correct joining key!")
        movies_df = pd.merge(cleaned_wiki, cleaned_kaggle, on=joining_key, suffixes=['_wiki','_kaggle'])
        return

    # dealing with redundant columns:
        # dropping bad data:
        movies_df = movies_df.drop(movies_df[(movies_df['release_date_wiki'] > '1996-01-01') & (movies_df['release_date_kaggle'] < '1965-01-01')].index)
       
        # make a function that fills in missing data for a column pair and then drops the redundant column.
    def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
        df[kaggle_column] = df.apply(
            lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column] # all 3 columns that we are doing the merge are in numeric
            , axis=1)
        df.drop(columns=wiki_column, inplace=True)
        # fill missing data, drop redundant columns, and run:
    fill_missing_kaggle_data(movies_df, 'runtime', 'running_time')
    fill_missing_kaggle_data(movies_df, 'budget_kaggle', 'budget_wiki')
    fill_missing_kaggle_data(movies_df, 'revenue', 'box_office')

    # Keep only needed columns and reorder:
    movies_df = movies_df.loc[:, ['imdb_id','id','title_kaggle','original_title','tagline','belongs_to_collection','url','imdb_link',
                           'runtime','budget_kaggle','revenue','release_date_kaggle','popularity','vote_average','vote_count',
                           'genres','original_language','overview','spoken_languages','Country',
                           'production_companies','production_countries','Distributor',
                           'Producer(s)','Director','Starring','Cinematography','Editor(s)','Writer(s)','Composer(s)','Based on'
                          ]]

    # Rename:
    movies_df.rename({'id':'kaggle_id',
                      'title_kaggle':'title',
                      'url':'wikipedia_url',
                      'budget_kaggle':'budget',
                      'release_date_kaggle':'release_date',
                      'Country':'country',
                      'Distributor':'distributor',
                      'Producer(s)':'producers',
                      'Director':'director',
                      'Starring':'starring',
                      'Cinematography':'cinematography',
                      'Editor(s)':'editors',
                      'Writer(s)':'writers',
                      'Composer(s)':'composers',
                      'Based on':'based_on'
                     }, axis='columns', inplace=True)
    
    return movies_df


# In[7]:


### 4. Create a function to join movies_df with Ratings data:
def join_movies_with_ratings(movies_df, ratings):
    # convert datetime:
    ratings['timestamp'] = pd.to_datetime(ratings['timestamp'], unit='s')
    # pivot table:
    rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count()                     .rename({'userId':'count'}, axis=1)                     .pivot(index='movieId',columns='rating', values='count')
    # rename columns:
    rating_counts.columns = ['rating_' + str(col) for col in rating_counts.columns]
    
    # inner join tables:
    movies_with_ratings_df = pd.merge(movies_df, rating_counts, left_on='kaggle_id', right_index=True, how='left')
    # fill na for non-rating for that score:
    movies_with_ratings_df[rating_counts.columns] = movies_with_ratings_df[rating_counts.columns].fillna(0)
    
    return movies_with_ratings_df


# In[8]:


### 5. Create Master function with 3 arguments as raw data load:
def master_ETL(wiki_raw, kaggle_raw, ratings_raw):
    # get the start_time from time.time()
    start_time = time.time()
    
    # clean and create movies_df & movies_with_ratings_df:
    try:
        movies_df = join_movies_df(wiki_raw, kaggle_raw)
        movies_with_ratings_df = join_movies_with_ratings(movies_df, ratings_raw)
    except:
        print(f'ETL failed to clean and merge dataset.')
        return
    
    # save movies_df to_sql, replacing if the same table already exists:
    try:
        movies_df.to_sql(name='movies', con=engine, if_exists='replace')
        print(f'Finish importing movies_df into SQL database.')
    except:
        print(f'ETL failed to export dataframe to SQL Database.')
        return
    
    # save rating_raw table into sql using chunksize (data is too large):
        # create a variable for the number of rows imported
    rows_imported = 0
    print(f'Starting to import ratings.csv into SQL database now.')
    for data in pd.read_csv(f'{file_dir}ratings.csv', chunksize=1000000):

            # print out the range of rows that are being imported
        print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end='')
        try:
            data.to_sql(name='ratings', con=engine, if_exists='append')
        except:
            print(f'ETL failed to continue importing file ratings.csv into SQL Database after rows {rows_imported}. Stop the import!')
            return

            # increment the number of rows imported by the chunksize
        rows_imported += len(data)

            # add elapsed time to final print out and print that the rows have finished importing
        time_spent = time.time() - start_time
        print(f'Done. {time_spent} total seconds elapsed')
        
    # Print Complete:
    print(f'Done. Full ETL ran successully in {time_spent} seconds.')


# # Run ETL:

# In[9]:


# master_ETL(wiki_raw, kaggle_raw, ratings_raw)


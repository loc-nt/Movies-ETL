# Movies-ETL

## CHALLENGE:

The ETL function is using the following assumptions:
1. The environment platform has fully installed all necessary dependencies below:

> import json  
> import pandas as pd  
> import numpy as np  
> import re  
> from sqlalchemy import create_engine  
> from config import db_password  
> import sys  
> !{sys.executable} -m pip install psycopg2-binary  
> import time  

2. In the Wikipedia data, there should be no new alternative titles since the function will not be able to capture them. However, we decided to use the title from the Kaggle metadata instead, so these titles from Wikipedia might not be relevant.

3. In the process of cleaning Wikipedia data, we have renamed & merged many similar columns to have consistent data. Therefore, if new columns name are created, those data will not be collected and not included in the dataframe imported.

4. The function will not be able to capture any new currency, date, and running time formats that were not included in the tramsforming steps for the Wikipedia data. So any new format forms will be lost.

5. Kaggle metadata is pretty structured, so we are assuming all columns within the data are convertable to the desired types. If for some reasons that the data type is not convertable, the function has a try-except block to keep the current data type as is. Therefore, we will still have data imported into the Database, but need to be careful in case the data type will not work for some future analyst codes.

6. I also assumed that the Kaggle metadata should always have the 'imdb_id' column to join with the Wikipedia data.

7. We kept most of the data available in Kaggle and dropped the same data in Wikipedia because the Kaggle data just looks more accurate and structured. Therefore, I assume that the Kaggle data is more accurate than Wikipedia.

8. Since the function will replace the current 'movies' table in the Database, I assume that the new updated data will always be equal or better quality, and no need to keep the old data anymore.

In addition, we also have a dataframe of movies with rating data, which may provide great insights when doing analysis later. I'd recommend Britta to import them into the Database for the Hackathon besides current movies and ratings table.

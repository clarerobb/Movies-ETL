# ETL movie data from Wikipedia and Kaggle

## Overview
This code automates the extract, transform, and load (ETL) process for movie data from Wikipedia and Kaggle. To do this, I gathered movie data from Wikipedia and Kaggle; cleaned, structured, and combined the datasets; and saved them into a SQL database for an upcoming hackathon. The entire ETL process is excuted in a single call `read_files()` in the 'ETL_create_database.ipynb' file.

## Data
- wikipedia-movies.json
- movies_metadata.csv
- ratings.csv

## ETL_function_test.ipynb
This code reads data from Wikipedia and Kaggle and saves it as a DataFrame. The Wikipedia data is saved as a JSON file and the Kaggle data is saved in two CSV files.

## ETL_clean_wiki_movies.ipynb
This builds on the code in 'ETL_function_test.ipynb' by extracting and cleaning the Wikipedia data. The function `clean_movie(movie)` (below) merges alternate foreign language titles into one column as well as other similar titles like "Directed by" and "Director." 

```
def clean_movie(movie):
    movie = dict(movie) 
    alt_titles = {}
    for key in ['Also known as','Arabic','Cantonese','Chinese','French',
                'Hangul','Hebrew','Hepburn','Japanese','Literally',
                'Mandarin','McCune–Reischauer','Original title','Polish',
                'Revised Romanization','Romanized','Russian',
                'Simplified','Traditional','Yiddish']:
        if key in movie:
            #if so, remove the key-value pair and add to the alternative titles dict
            alt_titles[key] = movie[key]
            movie.pop(key)
    if len(alt_titles) > 0:
        movie['alt_titles'] = alt_titles
        
    def change_column_name(old_name, new_name):
        if old_name in movie:
            movie[new_name] = movie.pop(old_name)
    change_column_name('Directed by', 'Director')
    change_column_name('Adaptation by', 'Writer(s)')
    change_column_name('Country of origin', 'Country')
    change_column_name('Directed by', 'Director')
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
  ```

The `read_files()` function then deletes columns that contain mostly null values. After condensing the Wikipedia data, the function cleans the following columns: box office, budget, release date, and running time.

## ETL_clean_kaggle_data.ipynb
This file continues to develop the `read_files()` function by extracting and cleaning the two Kaggle csv files. After, cleaning the kaggle metadata, it was merged with the Wikipedia data. Because the data contained similar information, I compared the competing data and dropped the following columns: title_wiki, release_date_wiki, Language, and Production company(s). The function below then fills in mising data from the Kaggle column with the data in the Wikipedia column and then drops the Wikipedia column from the merged DataFrame. 

```
def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
    df[kaggle_column] = df.apply(
        lambda row:row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column]
        , axis=1)
    df.drop(columns=wiki_column, inplace=True)
```

This fuction then transforms and merges the data in ratings.csv with the movies_df.

## ETL_create_database.ipynb
Finally, the `read_files()` completes the ETL process by loading the movie and ratings data to the PostgreSQL database. The images below show that all of the rows from the movies and ratings DataFrames were loaded correctly. 

#### Total Rows in Movies Table in SQL
![movies_query](https://user-images.githubusercontent.com/106405775/182002674-ef63aabb-23cc-44fc-940c-5e63959c6321.png)

#### Total Rows in Ratings Table in SQL
![ratings_query](https://user-images.githubusercontent.com/106405775/182002683-dd8dfd9c-d376-4660-8b4d-fb09d6ee8839.png)

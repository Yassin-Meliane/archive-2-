import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

#extract
df = pd.read_csv("~/Downloads/Thebrief/archive(2)/spotify_songs.csv")

# Cleaning data
# cleaning missing values
df["track_name"].fillna("Unknown_track_name", inplace=True)
df["track_artist"].fillna("Unknown_track_artist", inplace=True)
df["track_album_name"].fillna("Unknown_track_album_name", inplace=True)

# Converting data types
df["track_album_release_date"] = pd.to_datetime(df["track_album_release_date"], errors="coerce")

# Handling missing or invalid dates
df["track_album_release_date"].fillna(pd.to_datetime("1970-01-01"), inplace=True)
# Convert 'duration_ms' to minutes and cast to int64
df['duration_min'] = (df['duration_ms'] / (1000 * 60)).astype('int64')

# Drop the original 'duration_ms' column
df.drop('duration_ms', axis=1, inplace=True)

# Snowflake account credentials and connection details
user = "YASSIN"
password = ""
account = ""
database = "SPOTIFY_BRIEF"
schema = "SPOTIFY_BRIEF_SCHEMA"

# Create a connection to Snowflake
conn = snowflake.connector.connect(
    user=user,
    password=password,
    account=account,
    database=database,
    schema=schema
)

# Write the data from the DataFrame to Snowflake
write_pandas(conn, df, "spotify_komigen", auto_create_table=True)

# Close the connection
conn.close()


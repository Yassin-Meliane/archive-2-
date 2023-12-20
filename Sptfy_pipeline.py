import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
# Extract data from CSV
file_input = "~/Downloads/Thebrief/archive(2)/spotify_songs.csv"
df = pd.read_csv(file_input)
# Cleaning data
# cleaning missing values

df["track_name"].fillna("Unknown_track_name", inplace=True)
df["track_artist"].fillna("Unknown_track_artist", inplace=True)
df["track_album_name"].fillna("Unknown_track_album_name", inplace=True)
# Converting data types
df["track_album_release_date"] = pd.to_datetime(df["track_album_release_date"], errors="coerce")

# Creating new features, converting milliseconds to minutes
df['duration_min'] = df['duration_ms'] / (1000 * 60)

# Snowflake account credentials and connection details
user = "YASSIN"
password = "Swedenystad+1"
account = "HDSCWGU-RH60445"
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
# Ensuring that the table 'SPOTIFY_SONGS' exists in Snowflake with the appropriate schema
#success, nchunks, nrows, _ = write_pandas(conn, df, "spotify_songs")
write_pandas(conn, df, "spotify_brief", auto_create_table=True)
# Close the connection
conn.close()










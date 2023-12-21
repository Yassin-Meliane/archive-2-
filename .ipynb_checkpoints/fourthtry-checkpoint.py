import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
# Extract data from CSV

df = pd.read_csv("~/Downloads/Thebrief/archive(2)/spotify_songs.csv")

# Extract only the first 4 characters of the 'track_album_release_date' string

df['track_album_release_date'] = df['track_album_release_date'].astype(str).str[:4]

# Convert 'duration_ms' to minutes and cast to int64

df['duration_min'] = (df['duration_ms'] / (1000 * 60)).astype('int64')

# Drop the original 'duration_ms' column

df.drop('duration_ms', axis=1, inplace=True)

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
write_pandas(conn, df, "spotify_fact1", auto_create_table=True)
# Close the connection
conn.close()








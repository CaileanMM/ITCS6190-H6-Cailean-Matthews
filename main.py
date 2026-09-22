"""Hands-on L6: Music Streaming Analysis with the Spark Structured APIs.

Usage (on the Docker cluster from Hands-on L5):
    spark-submit main.py <input directory> <output directory>

Reads listening_logs.csv and songs_metadata.csv from the input directory and writes one
CSV result per task under the output directory (task1/ ... task4/).

Fill in the parts marked TODO. Each task is a function that returns a DataFrame; a task
whose function still returns None is skipped, so you can run the file after each step.
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, round as sround, hour, desc, row_number
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.window import Window

if len(sys.argv) != 3:
    print(__doc__)
    sys.exit(2)
in_dir, out_dir = sys.argv[1].rstrip("/"), sys.argv[2].rstrip("/")

spark = SparkSession.builder.appName("MusicAnalysis").getOrCreate()


def save(df, name):
    """Print a DataFrame and write it as a single CSV file with a header."""
    if df is None:
        print(f"\n=== {name}: not implemented yet ===")
        return
    print(f"\n=== {name} ===")
    df.show(20, truncate=False)
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{out_dir}/{name}")


# ---------------------------------------------------------------- Step 0: load the data
# Declare the schemas up front (slides "Define Schema"): no inference pass over the file,
# and the timestamp column becomes a real timestamp instead of a string.
#
# TODO: write the schema of listening_logs.csv as a StructType with four fields:
#   user_id STRING, song_id STRING, timestamp TIMESTAMP, duration_sec INT
logs_schema = None

# The schema of songs_metadata.csv, as a DDL string (the other form from the slides).
songs_schema = "song_id STRING, title STRING, artist STRING, genre STRING, mood STRING"

logs = spark.read.csv(f"{in_dir}/listening_logs.csv", header=True, schema=logs_schema)
songs = spark.read.csv(f"{in_dir}/songs_metadata.csv", header=True, schema=songs_schema)

logs.printSchema()
print(f"{logs.count()} log rows, {songs.count()} songs")

# One row per play with the song's metadata attached. Tasks 1 and 3 need the genre.
plays = logs.join(songs, "song_id")


# ---------------------------------------------------------------- Task 1
def task1_favorite_genre():
    """Each user's favorite genre: the genre the user played most often.

    Columns: user_id, genre, play_count. One row per user, ordered by user_id.
    Ties: the genre that comes first alphabetically.
    Hint: count plays per (user_id, genre), then keep the top row per user. A window with
    row_number() over Window.partitionBy("user_id").orderBy(...) does that in one step.
    """
    # TODO
    return None


# ---------------------------------------------------------------- Task 2
def task2_average_listen_time():
    """Average listening time per song, longest first.

    Columns: song_id, title, avg_duration_sec (rounded to 2 decimals), play_count.
    Ordered by avg_duration_sec descending.
    """
    # TODO
    return None


# ---------------------------------------------------------------- Task 3
def task3_genre_loyalty(favorite):
    """Genre loyalty score: the share of a user's plays that belong to their favorite genre.

    loyalty_score = play_count of the favorite genre / total plays of the user, rounded
    to 3 decimals. Return the 10 most loyal users.
    Columns: user_id, genre, play_count, total_plays, loyalty_score.
    Ordered by loyalty_score descending, then total_plays descending, then user_id.
    `favorite` is the DataFrame returned by task 1; join it with the total plays per user.
    """
    # TODO
    return None


# ---------------------------------------------------------------- Task 4
def task4_night_owls():
    """Users who listen between 12 AM and 5 AM (hour of the timestamp 0 to 4).

    Columns: user_id, night_plays (number of plays in that window).
    Ordered by night_plays descending, then user_id.
    Hint: hour("timestamp") works because the column is a timestamp, not a string.
    """
    # TODO
    return None


favorite = task1_favorite_genre()
save(favorite, "task1")
if favorite is not None:
    favorite.explain()          # the physical plan of task 1: paste it into your report

save(task2_average_listen_time(), "task2")
save(task3_genre_loyalty(favorite) if favorite is not None else None, "task3")
save(task4_night_owls(), "task4")

spark.stop()

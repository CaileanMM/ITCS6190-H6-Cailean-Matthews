"""Generate the two input files for Hands-on L6 (plain Python, no extra packages).

    python3 datagen.py <seed> [<output directory>]

Writes listening_logs.csv and songs_metadata.csv to the output directory, by default
shared-folder/input next to this file.
Use your student ID as the seed: the data is then yours, and it can be regenerated exactly
from that number. The same seed always produces the same files.
"""
import csv
import os
import random
import sys
from datetime import datetime, timedelta

if len(sys.argv) not in (2, 3) or not sys.argv[1].isdigit():
    print(__doc__)
    sys.exit(2)

seed = int(sys.argv[1])
random.seed(seed)

NUM_USERS, NUM_SONGS, NUM_LOGS = 100, 50, 1000
GENRES = ["Pop", "Rock", "Jazz", "Classical", "Hip-Hop"]
MOODS = ["Happy", "Sad", "Energetic", "Chill"]
START, END = datetime(2026, 3, 1), datetime(2026, 3, 28)

out_dir = (sys.argv[2] if len(sys.argv) == 3 else
           os.path.join(os.path.dirname(os.path.abspath(__file__)), "shared-folder", "input"))
os.makedirs(out_dir, exist_ok=True)

user_ids = [f"user_{i}" for i in range(1, NUM_USERS + 1)]
song_ids = [f"song_{i}" for i in range(1, NUM_SONGS + 1)]

# Songs: id, title, artist, genre, mood
with open(os.path.join(out_dir, "songs_metadata.csv"), "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["song_id", "title", "artist", "genre", "mood"])
    for s in song_ids:
        w.writerow([s, f"Title_{s}", f"Artist_{random.randint(1, 20)}",
                    random.choice(GENRES), random.choice(MOODS)])

# Listening logs: who played what, when, and for how long (seconds).
# Each user has a preferred genre, played more often than the others, so that the
# favorite-genre and loyalty tasks have something to find.
song_genre = {}
with open(os.path.join(out_dir, "songs_metadata.csv")) as f:
    for row in csv.DictReader(f):
        song_genre[row["song_id"]] = row["genre"]
by_genre = {g: [s for s in song_ids if song_genre[s] == g] for g in GENRES}
by_genre = {g: s for g, s in by_genre.items() if s}
preferred = {u: random.choice(list(by_genre)) for u in user_ids}
span = int((END - START).total_seconds())

with open(os.path.join(out_dir, "listening_logs.csv"), "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["user_id", "song_id", "timestamp", "duration_sec"])
    for _ in range(NUM_LOGS):
        u = random.choice(user_ids)
        s = random.choice(by_genre[preferred[u]]) if random.random() < 0.6 else random.choice(song_ids)
        t = START + timedelta(seconds=random.randint(0, span))
        w.writerow([u, s, t.strftime("%Y-%m-%d %H:%M:%S"), random.randint(30, 300)])

print(f"seed {seed}: wrote {NUM_LOGS} logs and {NUM_SONGS} songs to {out_dir}")

# Hands-on L6: Report

**Name:** Cailean Matthews
**Student ID:** 801412380
**Email:** matthewsmcailean@gmail.com

---

## Seed and commands

Seed used for `datagen.py`: `801412380`

The commands you ran, in order. If you deviated from the steps in the README, say where and
why.

```bash
python datagen.py 801412380
docker compose up -d

docker cp main.py spark-master:/opt/spark/work-dir/
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/main.py \
  /opt/spark/work-dir/shared/input \
  /opt/spark/work-dir/shared/output

# Same run with the event log on, to count the jobs after the application UI (port 4040) closed
docker exec spark-master mkdir -p /tmp/ev
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.eventLog.enabled=true --conf spark.eventLog.compress=false \
  --conf spark.eventLog.dir=file:/tmp/ev \
  /opt/spark/work-dir/main.py \
  /opt/spark/work-dir/shared/input \
  /opt/spark/work-dir/shared/output

docker compose down
```

Differences from the README: `docker exec` was run without `-it` because the shell was not
interactive, and I added the second run with the event log to count the jobs.

---

## Results

For each task, the first ten rows of your output (from the terminal or the CSV file) and one
or two sentences on what they say about your data.

### Task 1: favorite genre per user

```
user_id,genre,play_count
user_1,Classical,8
user_10,Rock,4
user_100,Pop,5
user_11,Rock,4
user_12,Classical,3
user_13,Rock,7
user_14,Rock,8
user_15,Rock,7
user_16,Jazz,10
user_17,Hip-Hop,9
```
All 100 users have a favorite genre. The favorites are spread fairly evenly with Pop at 24,
Rock at 24, Classical at 18, Hip-Hop at 18, Jazz at 16. Rows are ordered by user_id as a string, so
user_100 comes right after user_10.

### Task 2: average listening time per song

```
song_id,title,avg_duration_sec,play_count
song_45,Title_song_45,226.0,14
song_14,Title_song_14,206.19,16
song_26,Title_song_26,200.76,17
song_37,Title_song_37,198.58,19
song_1,Title_song_1,193.85,20
song_4,Title_song_4,193.35,23
song_42,Title_song_42,184.0,18
song_25,Title_song_25,183.36,22
song_12,Title_song_12,182.52,27
song_19,Title_song_19,182.35,23
```
Averages range from 226.0 s (song_45) down to 138.32 s (song_10). The songs at the top
have fewer plays than average (14 to 27), so a few long plays move their averages a lot.

### Task 3: genre loyalty score, top 10

```
user_id,genre,play_count,total_plays,loyalty_score
user_31,Jazz,12,12,1.0
user_59,Rock,9,9,1.0
user_1,Classical,8,8,1.0
user_82,Rock,5,5,1.0
user_70,Hip-Hop,3,3,1.0
user_84,Classical,11,12,0.917
user_87,Hip-Hop,10,11,0.909
user_18,Hip-Hop,9,10,0.9
user_14,Rock,8,9,0.889
user_29,Pop,8,9,0.889
```

Why do users with few plays tend to get a score of 1.0? Would you change the definition of
the score to account for that?

With only a few plays, its likely that every play falls in the preferred genre by
chance. user_70 gets 1.0 from 3 plays, the same score as user_31 with 12. The score
treats a very small sample as if it were strong evidence. I would need a minimum number
of plays (for example total_plays >= 5) or smooth the score toward the average.

### Task 4: night owls

```
user_id,night_plays
user_28,5
user_63,5
user_76,5
user_98,5
user_43,4
user_5,4
user_65,4
user_78,4
user_85,4
user_87,4
```

86 of the 100 users have at least one night play, but no one has more than 5. There are 192
night plays in total (19.2% of all plays), close to the 5/24 = 20.8% you would expect if
play times were spread evenly across the day. The generator does not seem to create real
night owls.

---

## The plan

Paste the `explain()` output of task 1:

```
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Sort [user_id#0 ASC NULLS FIRST], true, 0
   +- Exchange rangepartitioning(user_id#0 ASC NULLS FIRST, 200), ENSURE_REQUIREMENTS, [plan_id=975]
      +- Project [user_id#0, genre#7, play_count#28L]
         +- Filter (rank#38 = 1)
            +- Window [row_number() windowspecdefinition(user_id#0, play_count#28L DESC NULLS LAST, genre#7 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rank#38], [user_id#0], [play_count#28L DESC NULLS LAST, genre#7 ASC NULLS FIRST]
               +- WindowGroupLimit [user_id#0], [play_count#28L DESC NULLS LAST, genre#7 ASC NULLS FIRST], row_number(), 1, Final
                  +- Sort [user_id#0 ASC NULLS FIRST, play_count#28L DESC NULLS LAST, genre#7 ASC NULLS FIRST], false, 0
                     +- Exchange hashpartitioning(user_id#0, 200), ENSURE_REQUIREMENTS, [plan_id=968]
                        +- WindowGroupLimit [user_id#0], [play_count#28L DESC NULLS LAST, genre#7 ASC NULLS FIRST], row_number(), 1, Partial
                           +- Sort [user_id#0 ASC NULLS FIRST, play_count#28L DESC NULLS LAST, genre#7 ASC NULLS FIRST], false, 0
                              +- HashAggregate(keys=[user_id#0, genre#7], functions=[count(1)])
                                 +- Exchange hashpartitioning(user_id#0, genre#7, 200), ENSURE_REQUIREMENTS, [plan_id=962]
                                    +- HashAggregate(keys=[user_id#0, genre#7], functions=[partial_count(1)])
                                       +- Project [user_id#0, genre#7]
                                          +- BroadcastHashJoin [song_id#1], [song_id#4], Inner, BuildRight, false, false
                                             :- Filter isnotnull(song_id#1)
                                             :  +- FileScan csv [user_id#0,song_id#1] Batched: false, DataFilters: [isnotnull(song_id#1)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/listening_logs.csv], PartitionFilters: [], PushedFilters: [IsNotNull(song_id)], ReadSchema: struct<user_id:string,song_id:string>
                                             +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, false]),false), [plan_id=957]
                                                +- Filter isnotnull(song_id#4)
                                                   +- FileScan csv [song_id#4,genre#7] Batched: false, DataFilters: [isnotnull(song_id#4)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/songs_metadata.csv], PartitionFilters: [], PushedFilters: [IsNotNull(song_id)], ReadSchema: struct<song_id:string,genre:string>
```

Your reading of it: where are the two file scans, which operator is the join and which kind
of join did Spark choose, where are the shuffles (`Exchange`) and why are they needed, and
how does this match the diagram in the SQL / DataFrame tab of the Spark UI?

- **Scans:** the two FileScan csv nodes at the bottom, one per file. Each reads only the
  columns it needs.
- **Join:** BroadcastHashJoin. The songs table is small (50 rows), so Spark copies it to
  every executor instead of shuffling the logs.
- **Shuffles:** three Exchange nodes.
  1. hashpartitioning(user_id, genre): groups rows by key for the play count.
  2. hashpartitioning(user_id): puts each user's rows together for the window.
  3. rangepartitioning(user_id): the final orderBy sorts across all partitions.
- **Spark UI:** the diagram has the same scans, join, aggregates and window. Two
  differences, because the UI shows the plan after it runs: shuffles show as
  AQEShuffleRead (AQE merges small partitions), and in the show() query the final sort
  is replaced by TakeOrderedAndProject, since only 21 rows are needed.

---

## Transformations and actions

Which lines of your `main.py` are actions? How many jobs did the program launch according to
the Spark UI, and is that what you expected?

The actions are the two count() calls on line 57, plus show() (line 33) and the CSV
write (line 34) for each of the 4 tasks: **10 actions** in total. Everything else is a lazy
transformation, and explain() starts no job.

The program launched **42 jobs**, not 10. One action can start several jobs: with adaptive
execution each shuffle stage runs as its own job, the broadcast adds a job, and each
orderBy runs a sampling job first. I expected one job per action, but tasks with more
shuffles launched more jobs (task 1 show(): 4 jobs; task 3 write: 7 jobs).

---

## Problems and fixes

Anything that went wrong and what resolved it. Paste the actual error message. If nothing
went wrong, say so.



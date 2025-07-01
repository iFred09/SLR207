#!/usr/bin/env zsh

# Usage: ./perf_test.zsh <input-file>
if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <input-file>"
  exit 1
fi

FILE=$1
BASE=$(basename "$FILE")
LOG="perf_${BASE%.*}.csv"

# CSV header: File;Threads;AverageTimeSec
echo "File;Threads;AverageTimeSec" >| "$LOG"

# Compile once
echo "Compiling project..."
if ! mvn compile >/dev/null; then
  echo "Compile failed" >&2
  exit 1
fi

# For each thread-count
for threads in {1..10}; do
  times=()  # reset array

  for run in {1..10}; do
    echo "Running MapReduce with ${threads} threads, run #${run}..."
    # capture only the "real" line (e.g. real 1.23)
    time_sec=$(
      { /usr/bin/time -p mvn exec:java@mapreduce \
          -Dexec.args="$FILE $threads" >/dev/null; } \
      2>&1 | awk '/^real/ {print $2}'
    )
    if [[ -z "$time_sec" ]]; then
      echo "  ⚠️  Warning: failed to capture time for threads=$threads run=$run; defaulting to 0" >&2
      time_sec=0
    fi
    times+=("$time_sec")
  done

  # compute average with awk, 3 decimal places
  average=$(printf "%s\n" "${times[@]}" | awk '{sum+=$1} END{printf "%.3f", sum/NR}')

  # write semicolon-separated line
  echo "$BASE;$threads;$average" >>| "$LOG"
done

echo "Done. Averages written to $LOG"
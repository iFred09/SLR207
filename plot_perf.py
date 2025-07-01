#!/usr/bin/env python3
import argparse
import os
import pandas as pd
import matplotlib.pyplot as plt

def main():
    p = argparse.ArgumentParser(
        description="Plot MapReduce perf: threads vs average time"
    )
    p.add_argument("csv_file", help="Path to perf CSV")
    args = p.parse_args()

    # 1) load, with ';' sep and ',' decimal
    df = pd.read_csv(args.csv_file, sep=';', decimal=',')

    # 2) detect format & get (threads, avg_time) series
    if {"Run", "TimeSec"}.issubset(df.columns):
        # raw per-run data: group and average
        summary = (
            df
            .groupby("Threads", as_index=False)["TimeSec"]
            .mean()
            .rename(columns={"TimeSec": "AverageTimeSec"})
        )
    elif "AverageTimeSec" in df.columns:
        # already averaged (and numeric thanks to decimal=',')
        summary = df[["Threads", "AverageTimeSec"]].copy()
    else:
        raise ValueError(
            "CSV must have either columns (Run & TimeSec) or AverageTimeSec"
        )

    # ensure Threads is sorted numerically
    summary = summary.sort_values("Threads")

    # 3) plot
    plt.figure()
    plt.plot(summary["Threads"], summary["AverageTimeSec"], marker="o")
    plt.title("MapReduce Performance")
    plt.xlabel("Number of Threads")
    plt.ylabel("Average Time (seconds)")
    plt.grid(True)
    plt.tight_layout()

    # 4) save
    png = os.path.splitext(args.csv_file)[0] + ".png"
    plt.savefig(png)
    print(f"Saved plot to {png}")

    # and show
    plt.show()

if __name__ == "__main__":
    main()
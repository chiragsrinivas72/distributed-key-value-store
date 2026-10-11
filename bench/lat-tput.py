#!/usr/bin/env python3
import os
import re
import sys
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


def parse_result_file(path):
    rows = {
        "clients":    [],
        "lat_avg":    [],
        "lat_p50":    [],
        "lat_p90":    [],
        "lat_p99":    [],
        "throughput": [],
    }

    num_re = re.compile(r"^\s*\d")

    with open(path, "r") as f:
        for raw in f:
            line = raw.rstrip("\n")
            if not line.strip():
                continue
            if line.lstrip().startswith(("#", "-")):
                continue
            if not num_re.match(line):
                continue

            parts = line.split()
            if len(parts) < 6:
                continue

            try:
                clients    = int(parts[0])
                lat_avg    = float(parts[1])
                lat_p50    = float(parts[2])
                lat_p90    = float(parts[3])
                lat_p99    = float(parts[4])
                throughput = float(parts[5])
            except ValueError:
                continue

            rows["clients"].append(clients)
            rows["lat_avg"].append(lat_avg)
            rows["lat_p50"].append(lat_p50)
            rows["lat_p90"].append(lat_p90)
            rows["lat_p99"].append(lat_p99)
            rows["throughput"].append(throughput)

    if not rows["clients"]:
        raise RuntimeError(f"No data rows parsed from {path}")

    return rows


def plot_lat_tput(rows, out_path):
    order = sorted(range(len(rows["throughput"])),
                   key=lambda i: rows["throughput"][i])
    tp       = [rows["throughput"][i] for i in order]
    lat_avg  = [rows["lat_avg"][i]    for i in order]
    lat_p50  = [rows["lat_p50"][i]    for i in order]
    lat_p90  = [rows["lat_p90"][i]    for i in order]
    lat_p99  = [rows["lat_p99"][i]    for i in order]
    clients  = [rows["clients"][i]    for i in order]

    fig, ax = plt.subplots(figsize=(9, 6))

    ax.plot(tp, lat_avg, marker="o", linewidth=1.5, label="avg")
    ax.plot(tp, lat_p50, marker="s", linewidth=1.5, label="p50")
    ax.plot(tp, lat_p90, marker="^", linewidth=1.5, label="p90")
    ax.plot(tp, lat_p99, marker="D", linewidth=1.5, label="p99")

    for x, y, c in zip(tp, lat_avg, clients):
        ax.annotate(f"{c}c", xy=(x, y), xytext=(5, 5),
                    textcoords="offset points", fontsize=8, color="gray")

    ax.set_xlabel("Throughput (ops/sec)")
    ax.set_ylabel("Latency (ms)")
    ax.set_title("Latency vs. Throughput (3-replica KV cluster)")
    ax.grid(True, alpha=0.3)
    ax.legend(title="Percentile", loc="best")

    ax.set_yscale("log")

    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    print(f"Wrote {out_path}")


def main(argv):
    in_path  = argv[1] if len(argv) > 1 else "../build/app/result.txt"
    out_path = argv[2] if len(argv) > 2 else "lat-tput.png"

    if not os.path.exists(in_path):
        print(f"error: input file not found: {in_path}", file=sys.stderr)
        print("run ./tput in build/app first (it writes result.txt there)",
              file=sys.stderr)
        return 1

    rows = parse_result_file(in_path)
    plot_lat_tput(rows, out_path)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
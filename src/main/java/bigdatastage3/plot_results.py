import pandas as pd
import matplotlib.pyplot as plt

# CSV laden (Komma als Dezimaltrenner!)
df = pd.read_csv("C:/Users/nicob/OneDrive/Dokumente/Studium/Auslandssemester/ULPGC/Big_Data/Project/stage-3/results.csv", decimal=",")

NODE_COL = "Param: nodes"

def plot(benchmark, mode, title, ylabel):
    subset = df[
        (df["Benchmark"].str.contains(benchmark)) &
        (df["Mode"] == mode)
    ].sort_values(NODE_COL)

    plt.plot(subset[NODE_COL], subset["Score"], marker="o")
    plt.xlabel("Number of Nodes")
    plt.ylabel(ylabel)
    plt.title(title)
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(f"{benchmark}_{mode}.png")
    plt.clf()

# ---------- Plots ----------
plot("benchmarkIngestion", "thrpt",
     "Ingestion Throughput", "ops/s")

plot("benchmarkIndexing", "thrpt",
     "Indexing Throughput", "ops/s")

plot("benchmarkIngestion", "avgt",
     "Ingestion Latency", "s/op")

plot("benchmarkIndexing", "avgt",
     "Indexing Latency", "s/op")

plot("benchmarkQuery", "avgt",
     "Query Latency", "ms/op")

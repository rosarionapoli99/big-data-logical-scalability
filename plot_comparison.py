import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy import stats


def collect_input_paths_all_sizes(base_dir, dataset_sizes, mode):
    """
    Collects file paths for all dataset sizes.
    
    New structure: files are directly under dataset size folder (e.g., "25/")
    with naming convention: {dbms}_query{N}_{dbms}_{mode}.csv
    """
    queries = ["query1", "query2", "query3", "query4"]
    dbms_prefixes = {
        "MongoDB": "mongodb",
        "ArangoDB": "arangodb",
        "Neo4j": "neo4j"
    }
    
    input_matrix = {q: {db: [] for db in dbms_prefixes} for q in queries}
    
    for size in dataset_sizes:
        # New structure: just the size number (e.g., "25", "50")
        dataset_folder = f"{size}"
        base_path = os.path.join(base_dir, dataset_folder)
        
        for q in queries:
            for db, prefix in dbms_prefixes.items():
                # File naming: {prefix}_{query}_{prefix}_{mode}.csv
                fname = f"{prefix}_{q}_{prefix}_{mode}.csv"
                file_path = os.path.join(base_path, fname)
                input_matrix[q][db].append(file_path)
    
    return input_matrix


def plot_query_vs_size(input_matrix, dataset_sizes, mode, results_dir="results"):
    """
    Creates bar charts comparing DBMS performance across different dataset sizes.
    Uses logarithmic scale on Y-axis for better visualization.
    """
    if not os.path.exists(results_dir):
        os.makedirs(results_dir, exist_ok=True)
    
    dbms_labels = ["MongoDB", "ArangoDB", "Neo4j"]
    colors = ["#73c476", "#ffca56", "#659cef"]
    queries = list(input_matrix.keys())
    
    for q in queries:
        means = {db: [] for db in dbms_labels}
        cis = {db: [] for db in dbms_labels}
        
        for db in dbms_labels:
            for file in input_matrix[q][db]:
                # Check if file exists
                if not os.path.exists(file):
                    print(f"Warning: File not found: {file}")
                    means[db].append(np.nan)
                    cis[db].append(0)
                    continue
                
                df = pd.read_csv(file)
                times = df["execution_time_ms"]
                mean = np.mean(times)
                ci = stats.sem(times) * stats.t.ppf((1 + 0.95) / 2., len(times)-1) if len(times) > 1 else 0
                means[db].append(mean)
                cis[db].append(ci)
        
        x = np.arange(len(dataset_sizes))
        width = 0.25
        fig, ax = plt.subplots(figsize=(8, 5))
        
        for idx, db in enumerate(dbms_labels):
            ax.bar(x + idx*width - width, means[db], width, yerr=cis[db], capsize=5,
                   label=db, color=colors[idx], edgecolor="black")
        
        ax.set_xticks(x)
        ax.set_xticklabels([f"{sz}" for sz in dataset_sizes])
        ax.set_xlabel('Dataset size')
        ax.set_ylabel('Average execution time (ms)')
        ax.set_yscale('log')
        ax.set_title(f"Benchmark on Dataset Size (log scale Y)\n{mode.capitalize()} - {q.replace('query', 'Query ')}")
        ax.legend()
        plt.tight_layout()
        
        filename = os.path.join(results_dir, f"{q}_comparison_{mode}_log.png")
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        plt.close(fig)
        print(f"Saved: {filename}")


if __name__ == "__main__":
    base_dir = "."
    # Updated to match the new naming convention (no "x" suffix)
    dataset_sizes = ["25", "50", "75", "100"]  # Aggiungi le dimensioni che hai
    mode = "cold"   # "cold" or "warm"
    
    input_matrix = collect_input_paths_all_sizes(base_dir, dataset_sizes, mode)
    plot_query_vs_size(
        input_matrix, dataset_sizes, mode, results_dir="results"
    )

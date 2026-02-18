import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy import stats


def collect_input_paths(base_dir, dataset_size, mode):
    """
    Collects file paths for the benchmark results.
    
    New structure: all CSV files are directly under dataset_size folder (e.g., "25/")
    with naming convention: {dbms}_query{N}_{dbms}_{mode}.csv
    """
    dataset_folder = f"{dataset_size}"
    base_path = os.path.join(base_dir, dataset_folder)
    
    queries = ["query1", "query2", "query3", "query4"]
    dbms_prefixes = {
        "MongoDB": "mongodb",
        "ArangoDB": "arangodb",
        "Neo4j": "neo4j"
    }
    
    files = {q: {} for q in queries}
    
    for q in queries:
        for db, prefix in dbms_prefixes.items():
            # File naming: {prefix}_{query}_{prefix}_{mode}.csv
            fname = f"{prefix}_{q}_{prefix}_{mode}.csv"
            files[q][db] = os.path.join(base_path, fname)
    
    return files


def plot_all_queries(file_matrix, title_prefix="Benchmark", ylabel="Average execution time (ms)", results_dir="results"):
    """
    Creates bar charts comparing DBMS performance for each query.
    """
    if not os.path.exists(results_dir):
        os.makedirs(results_dir, exist_ok=True)
    
    dbms_labels = ["MongoDB", "ArangoDB", "Neo4j"]
    colors = ["#73c476", "#ffca56", "#659cef"]
    queries = list(file_matrix.keys())
    
    for i, query in enumerate(queries):
        means = []
        cis = []
        
        for dbms in dbms_labels:
            file_path = file_matrix[query][dbms]
            
            # Check if file exists before processing
            if not os.path.exists(file_path):
                print(f"Warning: File not found: {file_path}")
                means.append(0)
                cis.append(0)
                continue
            
            df = pd.read_csv(file_path)
            times = df["execution_time_ms"]
            mean = np.mean(times)
            ci = stats.sem(times) * stats.t.ppf((1 + 0.95) / 2., len(times)-1) if len(times) > 1 else 0
            means.append(mean)
            cis.append(ci)
        
        fig, ax = plt.subplots(figsize=(6, 5))
        x = np.arange(3)
        ax.bar(x, means, yerr=cis, capsize=5, color=colors, edgecolor="black")
        ax.set_xticks(x)
        ax.set_xticklabels(dbms_labels)
        ax.set_ylabel(ylabel)
        ax.set_xlabel('DBMS')
        ax.set_title(f"{title_prefix} - {query.capitalize()}")
        plt.tight_layout()
        
        filename = os.path.join(results_dir, f"{query}_{title_prefix.replace(' ', '_')}.png")
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        plt.close(fig)
        print(f"Grafico salvato: {filename}")


if __name__ == "__main__":
    # Base dir dove ci sono le cartelle con i dataset (25, 50, etc.)
    base_dir = "."
    dataset_size = "100"   # "25", "50", etc. (basato sulla nuova nomenclatura)
    mode = "warm"         # "cold" or "warm"
    
    file_matrix = collect_input_paths(base_dir, dataset_size, mode)
    
    plot_all_queries(
        file_matrix,
        title_prefix=f"Benchmark_{dataset_size}_{mode}",
        ylabel="Average execution time (ms)",
        results_dir="results"
    )

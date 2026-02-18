import time
import csv
from datetime import datetime


def execute_cold_and_warm_queries(
    dbms_type,
    connect_func,
    close_func,
    query_func,
    query,
    parameters=None,
    cold_iterations=31,
    warm_iterations=30,
    output_prefix="query"
):
    """
    Esegue 31 cold run (ognuna con connect/disconnect) + 30 warm run (senza disconnect) e salva due CSV distinti.
    
    Tutti i DBMS ora restituiscono timing SERVER-SIDE puri senza overhead.

    Args:
        dbms_type (str): tipo DBMS (es. 'neo4j', 'mongodb', 'arangodb')
        connect_func (callable): funzione per connettere (senza argomenti)
        close_func (callable): funzione per chiudere la connessione
        query_func (callable): funzione per lanciare la query
        query (str): query da eseguire
        parameters (dict): parametri opzionali
        cold_iterations (int): numero cold run (default 31)
        warm_iterations (int): numero warm run (default 30)
        output_prefix (str): prefisso file di output

    Output:
        - Un CSV per cold run, uno per warm run (es: query1_neo4j_cold.csv, query1_neo4j_warm.csv)
    """

    cold_csv = f"{output_prefix}_{dbms_type}_cold.csv"
    warm_csv = f"{output_prefix}_{dbms_type}_warm.csv"

    # --- Cold runs ---
    cold_times = []
    for i in range(cold_iterations):
        print(f"[COLD] Connessione a {dbms_type} (cold run {i+1}/{cold_iterations})")
        connect_func()
        
        # Esegui query
        result = query_func(query, parameters)
        
        # Estrai timing dal result
        if isinstance(result, dict) and 'execution_time_ms' in result:
            elapsed = result['execution_time_ms']
            if elapsed is None:
                print(f"  [ERROR] execution_time_ms è None per {dbms_type}!")
                elapsed = 0.0
        else:
            print(f"  [ERROR] Result non valido da {dbms_type}: {type(result)}")
            elapsed = 0.0
        
        close_func()
        
        # Nessuna sottrazione di overhead - già tempo server puro
        cold_times.append(elapsed)
        print(f"  --> {elapsed:.2f} ms")
        
        time.sleep(0.2)
    
    # Salva cold runs
    with open(cold_csv, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["iteration", "execution_time_ms"])
        for idx, t in enumerate(cold_times):
            writer.writerow([idx+1, t])
    
    print(f"[COLD] Salvato: {cold_csv}")

    # --- Warm runs ---
    warm_times = []
    print(f"\n[WARM] Connessione a {dbms_type} (tutte le {warm_iterations} iterazioni senza disconnessione)")
    connect_func()
    
    for i in range(warm_iterations):
        result = query_func(query, parameters)
        
        if isinstance(result, dict) and 'execution_time_ms' in result:
            elapsed = result['execution_time_ms']
            if elapsed is None:
                elapsed = 0.0
        else:
            elapsed = 0.0
        
        # Nessuna sottrazione di overhead - già tempo server puro
        warm_times.append(elapsed)
        print(f"  [WARM] run {i+1}/{warm_iterations} --> {elapsed:.2f} ms")
        
        time.sleep(0.2)
    
    close_func()

    # Salva warm runs
    with open(warm_csv, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["iteration", "execution_time_ms"])
        for idx, t in enumerate(warm_times):
            writer.writerow([idx+1, t])
    
    print(f"[WARM] Salvato: {warm_csv}")
    
    # Statistiche finali
    cold_avg = sum(cold_times) / len(cold_times) if cold_times else 0
    warm_avg = sum(warm_times) / len(warm_times) if warm_times else 0
    speedup = cold_avg / warm_avg if warm_avg > 0 else 0
    
    print(f"[INFO] Cold avg: {cold_avg:.2f} ms | Warm avg: {warm_avg:.2f} ms | Speedup: {speedup:.2f}x\n")

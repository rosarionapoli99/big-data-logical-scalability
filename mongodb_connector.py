import time
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
from datetime import datetime
import json


# Variabili globali per mantenere la connessione
_client = None
_database = None


def connect_mongodb(connection_string, database_name):
    """
    Stabilisce la connessione a MongoDB
    Args:
        connection_string (str): Connection string di MongoDB (es. "mongodb://localhost:27017")
        database_name (str): Nome del database
    Returns:
        bool: True se la connessione è riuscita, False altrimenti
    """
    global _client, _database
    _client = MongoClient(connection_string, serverSelectionTimeoutMS=5000)
    # Test della connessione
    _client.admin.command('ping')
    _database = _client[database_name]
    print(f"Connessione a MongoDB stabilita: {database_name}")
    return True


def close_mongodb():
    """Chiude la connessione al database MongoDB"""
    global _client
    if _client is not None:
        _client.close()
        _client = None
    print("Connessione a MongoDB chiusa")


def execute_mongodb_find_with_timing(collection_name, filter_query=None, projection=None, limit=None):
    """
    Esegue una query find su MongoDB e restituisce i risultati con timing
    Args:
        collection_name (str): Nome della collection
        filter_query (dict): Filtro per la query (opzionale)
        projection (dict): Proiezione dei campi (opzionale)
        limit (int): Limite di risultati (opzionale)
    Returns:
        dict: Dizionario contenente risultati, tempi di esecuzione e statistiche
    """
    global _database
    if _database is None:
        raise Exception("Connessione non stabilita. Chiamare connect_mongodb() prima")

    collection = _database[collection_name]

    # Timing: inizia DOPO aver ottenuto la collection (esclude lookup overhead)
    start_time = time.perf_counter()
    
    cursor = collection.find(filter_query or {}, projection)
    if limit:
        cursor = cursor.limit(limit)
    results = list(cursor)
    
    end_time = time.perf_counter()
    total_time = (end_time - start_time) * 1000  # in millisecondi

    serializable_results = []
    for doc in results:
        serializable_doc = {}
        for key, value in doc.items():
            serializable_doc[key] = str(value) if not isinstance(value, (str, int, float, bool, list, dict, type(None))) else value
        serializable_results.append(serializable_doc)

    return {
        'documents': serializable_results,
        'total_documents': len(serializable_results),
        'execution_time_ms': total_time,
        'collection': collection_name,
        'filter_query': filter_query,
        'projection': projection,
        'limit': limit,
        'timestamp': datetime.now().isoformat()
    }


def execute_mongodb_aggregate_with_timing(collection_name, pipeline):
    """
    Esegue una pipeline di aggregazione su MongoDB con timing
    Args:
        collection_name (str): Nome della collection
        pipeline (list): Pipeline di aggregazione
    Returns:
        dict: Dizionario con risultati e informazioni sui tempi
    """
    global _database
    if _database is None:
        raise Exception("Connessione non stabilita. Chiamare connect_mongodb() prima")

    collection = _database[collection_name]
    
    # Timing: inizia DOPO aver ottenuto la collection
    start_time = time.perf_counter()
    
    cursor = collection.aggregate(pipeline)
    results = list(cursor)
    
    end_time = time.perf_counter()
    total_time = (end_time - start_time) * 1000  # in millisecondi

    serializable_results = []
    for doc in results:
        serializable_doc = {}
        for key, value in doc.items():
            serializable_doc[key] = str(value) if not isinstance(value, (str, int, float, bool, list, dict, type(None))) else value
        serializable_results.append(serializable_doc)

    return {
        'documents': serializable_results,
        'total_documents': len(serializable_results),
        'execution_time_ms': total_time,
        'collection': collection_name,
        'pipeline': pipeline,
        'timestamp': datetime.now().isoformat()
    }


def benchmark_mongodb_query(collection_name, filter_query=None, projection=None, limit=None, iterations=5):
    """
    Esegue una query MongoDB multiple volte per ottenere statistiche di benchmark
    Args:
        collection_name (str): Nome della collection
        filter_query (dict): Filtro per la query
        projection (dict): Proiezione dei campi
        limit (int): Limite di risultati
        iterations (int): Numero di iterazioni
    Returns:
        dict: Statistiche aggregate del benchmark
    """
    results = []
    for i in range(iterations):
        result = execute_mongodb_find_with_timing(collection_name, filter_query, projection, limit)
        results.append(result['execution_time_ms'])
    return {
        'collection': collection_name,
        'filter_query': filter_query,
        'iterations': len(results),
        'avg_time_ms': sum(results) / len(results),
        'min_time_ms': min(results),
        'max_time_ms': max(results),
        'all_times_ms': results,
        'timestamp': datetime.now().isoformat()
    }


def get_mongodb_stats():
    """
    Ottiene statistiche generali del database MongoDB
    Returns:
        dict: Statistiche del database
    """
    global _database
    if _database is None:
        return {'error': 'Connessione non stabilita'}

    collections = _database.list_collection_names()
    stats = {
        'database_name': _database.name,
        'collections': collections,
        'collection_counts': {}
    }
    for collection_name in collections:
        count = _database[collection_name].count_documents({})
        stats['collection_counts'][collection_name] = count
    return stats

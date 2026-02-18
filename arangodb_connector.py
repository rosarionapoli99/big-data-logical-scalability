import time
from arango import ArangoClient
from datetime import datetime

# Variabili globali per mantenere la connessione
_client = None
_database = None

def connect_arangodb(host, port, username, password, database_name):
    """
    Stabilisce la connessione ad ArangoDB
    Args:
        host (str): Host di ArangoDB (es. "localhost")
        port (int): Porta di ArangoDB (es. 8529)
        username (str): Username per l'autenticazione
        password (str): Password per l'autenticazione
        database_name (str): Nome del database
    Returns:
        bool: True se la connessione è riuscita, False altrimenti
    """
    global _client, _database
    _client = ArangoClient(hosts=f'http://{host}:{port}')
    _database = _client.db(database_name, username=username, password=password)
    # Test della connessione
    _database.properties()
    print(f"Connessione ad ArangoDB stabilita: {database_name}")
    return True

def close_arangodb():
    """Chiude la connessione al database ArangoDB"""
    global _client, _database
    if _client is not None:
        _client = None
        _database = None
        print("Connessione ad ArangoDB chiusa")

def execute_arangodb_aql_with_timing(query, bind_vars=None):
    """
    Esegue una query AQL su ArangoDB e restituisce i risultati con informazioni sui tempi
    Args:
        query (str): Query AQL da eseguire
        bind_vars (dict): Variabili di bind per la query (opzionale)
    Returns:
        dict: Dizionario contenente risultati, tempi di esecuzione e statistiche
    """
    global _database
    if _database is None:
        raise Exception("Connessione non stabilita. Chiamare connect_arangodb() prima")

    start_time = time.time()
    cursor = _database.aql.execute(query, bind_vars=bind_vars or {})
    results = list(cursor)
    end_time = time.time()
    total_time = (end_time - start_time) * 1000  # in millisecondi

    # Serializza i risultati
    serializable_results = []
    for doc in results:
        if isinstance(doc, dict):
            serializable_doc = {}
            for key, value in doc.items():
                serializable_doc[key] = str(value) if not isinstance(value, (str, int, float, bool, list, dict, type(None))) else value
            serializable_results.append(serializable_doc)
        else:
            serializable_results.append(str(doc))

    return {
        'documents': serializable_results,
        'total_documents': len(serializable_results),
        'execution_time_ms': total_time,
        'query': query,
        'bind_vars': bind_vars,
        'timestamp': datetime.now().isoformat()
    }

def execute_arangodb_aql_with_profile(query, bind_vars=None):
    """
    Esegue una query AQL con profiling per ottenere statistiche dettagliate
    Args:
        query (str): Query AQL da eseguire
        bind_vars (dict): Variabili di bind per la query (opzionale)
    Returns:
        dict: Dizionario con risultati e informazioni di profiling
    """
    global _database
    if _database is None:
        raise Exception("Connessione non stabilita. Chiamare connect_arangodb() prima")

    start_time = time.time()
    cursor = _database.aql.execute(query, bind_vars=bind_vars or {}, profile=True)
    results = list(cursor)
    profile_info = cursor.profile
    end_time = time.time()
    total_time = (end_time - start_time) * 1000

    return {
        'documents': results,
        'total_documents': len(results),
        'execution_time_ms': total_time,
        'profile_info': profile_info,
        'query': query,
        'bind_vars': bind_vars,
        'timestamp': datetime.now().isoformat()
    }

def benchmark_arangodb_query(query, bind_vars=None, iterations=5):
    """
    Esegue una query ArangoDB multiple volte per ottenere statistiche di benchmark
    Args:
        query (str): Query AQL da testare
        bind_vars (dict): Variabili di bind per la query
        iterations (int): Numero di iterazioni
    Returns:
        dict: Statistiche aggregate del benchmark
    """
    results = []
    for i in range(iterations):
        result = execute_arangodb_aql_with_timing(query, bind_vars)
        results.append(result['execution_time_ms'])

    return {
        'query': query,
        'bind_vars': bind_vars,
        'iterations': len(results),
        'avg_time_ms': sum(results) / len(results),
        'min_time_ms': min(results),
        'max_time_ms': max(results),
        'all_times_ms': results,
        'timestamp': datetime.now().isoformat()
    }

def get_arangodb_stats():
    """
    Ottiene statistiche generali del database ArangoDB
    Returns:
        dict: Statistiche del database
    """
    global _database
    if _database is None:
        return {'error': 'Connessione non stabilita'}

    db_properties = _database.properties()
    collections = _database.collections()
    stats = {
        'database_name': db_properties.get('name'),
        'collections': [col['name'] for col in collections],
        'collection_counts': {},
        'database_properties': db_properties
    }
    for collection_info in collections:
        collection_name = collection_info['name']
        if not collection_name.startswith('_'):
            count_query = f"RETURN LENGTH({collection_name})"
            result = execute_arangodb_aql_with_timing(count_query)
            if result['documents']:
                stats['collection_counts'][collection_name] = result['documents'][0]
            else:
                stats['collection_counts'][collection_name] = 0
    return stats

def execute_arangodb_document_operation_with_timing(collection_name, operation, document_key=None, document_data=None):
    """
    Esegue operazioni CRUD sui documenti con timing
    Args:
        collection_name (str): Nome della collection
        operation (str): Tipo di operazione ('insert', 'get', 'update', 'delete')
        document_key (str): Chiave del documento (per get, update, delete)
        document_data (dict): Dati del documento (per insert, update)
    Returns:
        dict: Risultato dell'operazione con timing
    """
    global _database
    if _database is None:
        raise Exception("Connessione non stabilita. Chiamare connect_arangodb() prima")

    start_time = time.time()
    collection = _database.collection(collection_name)

    if operation == 'insert':
        result = collection.insert(document_data)
    elif operation == 'get':
        result = collection.get(document_key)
    elif operation == 'update':
        result = collection.update({'_key': document_key}, document_data)
    elif operation == 'delete':
        result = collection.delete(document_key)
    else:
        raise ValueError(f"Operazione non supportata: {operation}")

    end_time = time.time()
    total_time = (end_time - start_time) * 1000

    return {
        'result': result,
        'operation': operation,
        'collection': collection_name,
        'execution_time_ms': total_time,
        'timestamp': datetime.now().isoformat()
    }

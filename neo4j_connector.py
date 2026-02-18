import logging
import time
from neo4j import GraphDatabase
from datetime import datetime


_driver = None
_database = None


def connect_neo4j(uri, username, password, database=None):
    """
    Stabilisce la connessione a Neo4j
    
    Args:
        uri (str): URI del database Neo4j (es. "bolt://localhost:7687")
        username (str): Username per l'autenticazione
        password (str): Password per l'autenticazione
        database (str): Nome del database (opzionale)
    
    Returns:
        bool: True se la connessione è riuscita, False altrimenti
    """
    global _driver, _database
    _driver = GraphDatabase.driver(uri, auth=(username, password))
    _database = database or "neo4j"
    _driver.verify_connectivity()
    print(f"Connessione a Neo4j stabilita: {uri}")
    return True


def close_neo4j():
    """
    Chiude la connessione al database Neo4j
    """
    global _driver
    if _driver:
        _driver.close()
        _driver = None
        print("Connessione a Neo4j chiusa")


def execute_neo4j_query_with_timing(query, parameters=None):
    """
    Esegue una query Cypher e restituisce risultati con timing CLIENT-SIDE
    per essere consistente con MongoDB.
    
    Args:
        query (str): Query Cypher da eseguire
        parameters (dict): Parametri per la query (opzionale)
    
    Returns:
        dict: Contenente records, execution_time_ms e altre info
    """
    global _driver, _database
    if not _driver:
        raise Exception("Connessione a Neo4j non stabilita.")
    
    with _driver.session(database=_database) as session:
        # Timing: inizia DOPO aver aperto la sessione
        start_time = time.perf_counter()
        
        result = session.run(query, parameters or {})
        
        # Consuma i record
        records = [record for record in result]
        
        end_time = time.perf_counter()
        total_time = (end_time - start_time) * 1000  # in millisecondi
        
        return {
            'records': records,
            'total_records': len(records),
            'execution_time_ms': total_time,
            'query': query,
            'parameters': parameters,
            'timestamp': datetime.now().isoformat()
        }
import logging
import time
from neo4j import GraphDatabase
from datetime import datetime


_driver = None
_database = None


def connect_neo4j(uri, username, password, database=None):
    """
    Stabilisce la connessione a Neo4j
    
    Args:
        uri (str): URI del database Neo4j (es. "bolt://localhost:7687")
        username (str): Username per l'autenticazione
        password (str): Password per l'autenticazione
        database (str): Nome del database (opzionale)
    
    Returns:
        bool: True se la connessione è riuscita, False altrimenti
    """
    global _driver, _database
    _driver = GraphDatabase.driver(uri, auth=(username, password))
    _database = database or "neo4j"
    _driver.verify_connectivity()
    print(f"Connessione a Neo4j stabilita: {uri}")
    return True


def close_neo4j():
    """
    Chiude la connessione al database Neo4j
    """
    global _driver
    if _driver:
        _driver.close()
        _driver = None
        print("Connessione a Neo4j chiusa")


def execute_neo4j_query_with_timing(query, parameters=None):
    """
    Esegue una query Cypher e restituisce risultati con timing CLIENT-SIDE
    per essere consistente con MongoDB.
    
    Args:
        query (str): Query Cypher da eseguire
        parameters (dict): Parametri per la query (opzionale)
    
    Returns:
        dict: Contenente records, execution_time_ms e altre info
    """
    global _driver, _database
    if not _driver:
        raise Exception("Connessione a Neo4j non stabilita.")
    
    with _driver.session(database=_database) as session:
        # Timing: inizia DOPO aver aperto la sessione
        start_time = time.perf_counter()
        
        result = session.run(query, parameters or {})
        
        # Consuma i record
        records = [record for record in result]
        
        end_time = time.perf_counter()
        total_time = (end_time - start_time) * 1000  # in millisecondi
        
        return {
            'records': records,
            'total_records': len(records),
            'execution_time_ms': total_time,
            'query': query,
            'parameters': parameters,
            'timestamp': datetime.now().isoformat()
        }

import math
from neo4j_connector import connect_neo4j, close_neo4j, execute_neo4j_query_with_timing

def semantic_richness_calculator(uri, username, password, alpha=1, beta=1, gamma=1):
    # 1. Connessione a Neo4j
    connect_neo4j(uri=uri, username=username, password=password)
    
    # 2. Conteggio tipi nodi
    query_nodi = "MATCH (n) RETURN labels(n)[0] AS type, count(*) AS cnt"
    result_nodi = execute_neo4j_query_with_timing(query_nodi)["records"]
    num_types_nodi = len(result_nodi)
    total_nodi = sum([row['cnt'] for row in result_nodi])
    node_counts = [row['cnt'] for row in result_nodi]

    # 3. Conteggio tipi relazioni
    query_rel = "MATCH ()-[r]->() RETURN type(r) AS type, count(*) AS cnt"
    result_rel = execute_neo4j_query_with_timing(query_rel)["records"]
    num_types_rel = len(result_rel)
    total_rel = sum([row['cnt'] for row in result_rel])
    rel_counts = [row['cnt'] for row in result_rel]

    # 4. Calcolo Dtypes (diversità tipi)
    Dtypes = math.log(num_types_nodi) + math.log(num_types_rel)

    # 5. Calcolo entropia nodi HC
    HC = -sum([(cnt/total_nodi)*math.log(cnt/total_nodi) for cnt in node_counts if cnt > 0])
    # 6. Calcolo entropia relazioni HR
    HR = -sum([(cnt/total_rel)*math.log(cnt/total_rel) for cnt in rel_counts if cnt > 0])

    # 7. Calcolo finale ricchezza semantica SRKG
    SRKG = alpha*Dtypes + beta*HC + gamma*HR
    
    # 8. Chiudi connessione
    close_neo4j()

    print(f"Diversità tipi Dtypes: {Dtypes:.4f}")
    print(f"Entropia tipi nodi HC: {HC:.4f}")
    print(f"Entropia tipi relazioni HR: {HR:.4f}")
    print(f"Ricchezza Semantica SRKG: {SRKG:.4f}\n")
    return {
        'Dtypes': Dtypes,
        'HC': HC,
        'HR': HR,
        'SRKG': SRKG
    }

# --- Chiamata ESEMPLIFICATIVA ---
if __name__ == "__main__":
    # Sostituisci questi valori con quelli del tuo database
    uri = "bolt://localhost:7687"
    username = "neo4j"
    password = "11111111"
    # Chiamata funzione di calcolo
    semantic_richness_calculator(uri, username, password)

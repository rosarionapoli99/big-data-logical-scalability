import time
from datetime import datetime
import neo4j_connector
import mongodb_connector
import arangodb_connector
from query_runner import execute_cold_and_warm_queries

def print_section_header(title):
    print("\n" + "="*60)
    print(f"          {title}")
    print("="*60)

def connect_neo4j():
    return neo4j_connector.connect_neo4j(
        "bolt://localhost:7687", "neo4j", "11111111", "neo4j"
    )

def connect_mongodb():
    return mongodb_connector.connect_mongodb(
        "mongodb://localhost:27017", "test"
    )

def connect_arangodb():
    return arangodb_connector.connect_arangodb(
        host="localhost",
        port=8529,
        username="root",
        password="secretpass",
        database_name="test"
    )

# Aggiungi qui le query equivalenti per ArangoDB
generic_queries = [
    # Query 1 - Complex Scan - Filtri Multipli (NO LIMIT)
    ("Query 1 - Complex Scan - Filtri Multipli",
        {
            "neo4j": """
MATCH (t:TransazioneB2B)
WHERE t.importo_eur > 100.0 
  AND t.importo_eur < 500000.0
  AND t.aliquota_iva >= 0.10
  AND t.data_emissione >= date('2020-01-01')
  AND t.data_emissione <= date('2024-12-31')
RETURN t.id_transazione, t.importo_eur, t.aliquota_iva, t.data_emissione
""",
            "mongodb": {
                "collection": "TransazioneB2B",
                "pipeline": [
                    {
                        "$match": {
                            "$and": [
                                { "importo_eur": { "$gt": 100.0, "$lt": 500000.0 } },
                                { "aliquota_iva": { "$gte": 0.10 } },
                                { "data_emissione": { 
                                    "$gte": "2020-01-01", 
                                    "$lte": "2024-12-31" 
                                }}
                            ]
                        }
                    },
                    {
                        "$project": {
                            "id_transazione": 1,
                            "importo_eur": 1,
                            "aliquota_iva": 1,
                            "data_emissione": 1,
                            "_id": 0
                        }
                    }
                ]
            },
            "arangodb": """
FOR t IN TransazioneB2B
    FILTER t.importo_eur > 100.0 
      AND t.importo_eur < 500000.0
      AND t.aliquota_iva >= 0.10
      AND t.data_emissione >= '2020-01-01'
      AND t.data_emissione <= '2024-12-31'
    RETURN { 
        id: t.id_transazione, 
        importo: t.importo_eur, 
        iva: t.aliquota_iva,
        data: t.data_emissione
    }
"""
        }),

    # Query 2 - Join Azienda-Transazione (NO LIMIT)
    ("Query 2 - Join Azienda-Transazione",
        {
            "neo4j": """
MATCH (a:Azienda)
MATCH (a)<-[:EMESSA_DA_AZIENDA]-(t:TransazioneB2B)
RETURN a.nome, t.id_transazione, t.importo_eur
""",
            "mongodb": {
                "collection": "Azienda",
                "pipeline": [
                    {
                        "$lookup": {
                            "from": "TransazioneB2B",
                            "localField": "id_azienda",
                            "foreignField": "id_azienda_emittente",
                            "as": "transazioni"
                        }
                    },
                    { "$unwind": "$transazioni" },
                    {
                        "$project": {
                            "nome_azienda": "$nome",
                            "id_transazione": "$transazioni.id_transazione",
                            "importo": "$transazioni.importo_eur",
                            "_id": 0
                        }
                    }
                ]
            },
            "arangodb": """
FOR a IN Azienda
    FOR t IN INBOUND a EMESSA_DA_AZIENDA
        RETURN { 
            azienda: a.nome, 
            transazione: t.id_transazione, 
            imp: t.importo_eur 
        }
"""
        }),

    # Query 3 - Short Chain (NO LIMIT - DANGEROUS!)
    ("Query 3 - Short Chain",
        {
            "neo4j": """
MATCH (a:Azienda)
MATCH path = (a)<-[:EMESSA_DA_AZIENDA]-(t1)-[:DESTINATA_AD_AZIENDA]->(b:Azienda)
RETURN a.nome, b.nome
""",
            "mongodb": {
                "collection": "Azienda",
                "pipeline": [
                    {
                        "$graphLookup": {
                            "from": "TransazioneB2B",
                            "startWith": "$id_azienda",
                            "connectFromField": "id_azienda_destinataria", 
                            "connectToField": "id_azienda_emittente",
                            "maxDepth": 1, 
                            "as": "chain"
                        }
                    },
                    {
                        "$project": {
                            "start_node": "$nome",
                            "chain_length": { "$size": "$chain" }
                        }
                    },
                    { "$match": { "chain_length": { "$gte": 2 } } }
                ]
            },
            "arangodb": """
FOR a IN Azienda
    FOR v, e, p IN 2..2 ANY a EMESSA_DA_AZIENDA, DESTINATA_AD_AZIENDA
        RETURN { start: a.nome, end: v.nome }
"""
        }),

    # Query 4 - Deep Chain (WITH LIMIT)
    ("Query 4 - Deep Chain",
        {
            "neo4j": """
MATCH (a:Azienda)
WITH a LIMIT 25
MATCH (a)<-[:EMESSA_DA_AZIENDA]-(t1)-[:DESTINATA_AD_AZIENDA]->
      (b:Azienda)<-[:EMESSA_DA_AZIENDA]-(t2)-[:DESTINATA_AD_AZIENDA]->(c:Azienda)
RETURN a.nome, c.nome
LIMIT 200
""",
            "mongodb": {
                "collection": "Azienda",
                "pipeline": [
                    { "$limit": 25 },
                    {
                        "$graphLookup": {
                            "from": "TransazioneB2B",
                            "startWith": "$id_azienda",
                            "connectFromField": "id_azienda_destinataria", 
                            "connectToField": "id_azienda_emittente",
                            "maxDepth": 3, 
                            "as": "path"
                        }
                    },
                    {
                        "$project": {
                            "start_node": "$nome",
                            "path_length": { "$size": "$path" }
                        }
                    },
                    { "$match": { "path_length": { "$gte": 3 } } },
                    { "$limit": 200 }
                ]
            },
            "arangodb": """
FOR a IN Azienda
    LIMIT 25
    FOR v, e, p IN 4..4 ANY a EMESSA_DA_AZIENDA, DESTINATA_AD_AZIENDA
    LIMIT 200
    RETURN { start: a.nome, end: v.nome }
"""
        })
]





def execute_mongodb_query_wrapper(query_config, parameters=None):
    if "pipeline" in query_config:
        return mongodb_connector.execute_mongodb_aggregate_with_timing(
            query_config["collection"], query_config["pipeline"]
        )
    else:
        return mongodb_connector.execute_mongodb_find_with_timing(
            query_config["collection"], query_config["query"]
        )

def execute_arangodb_query_wrapper(query, parameters=None):
    return arangodb_connector.execute_arangodb_aql_with_timing(query)

def main():
    print_section_header("WORKFLOW: ARANGO/MONGO/NEO4J COLD/WARM BENCHMARK (QUERIES GENERICHE)")
    print(f"Avviato alle: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
         # --- MongoDB ---
    for idx, (descrizione, queries) in enumerate(generic_queries, 1):
        print_section_header(f"MONGODB - QUERY {idx}: {descrizione}")
        execute_cold_and_warm_queries(
            dbms_type="mongodb",
            connect_func=connect_mongodb,
            close_func=mongodb_connector.close_mongodb,
            query_func=execute_mongodb_query_wrapper,
            query=queries["mongodb"],
            parameters=None,
            cold_iterations=31,
            warm_iterations=30,
            output_prefix=f"mongodb_query{idx}"
        )
        time.sleep(2)
                # --- Neo4j ---
    for idx, (descrizione, queries) in enumerate(generic_queries, 1):
        print_section_header(f"NEO4J - QUERY {idx}: {descrizione}")
        execute_cold_and_warm_queries(
            dbms_type="neo4j",
            connect_func=connect_neo4j,
            close_func=neo4j_connector.close_neo4j,
            query_func=neo4j_connector.execute_neo4j_query_with_timing,
            query=queries["neo4j"],
            parameters=None,
            cold_iterations=31,
            warm_iterations=30,
            output_prefix=f"neo4j_query{idx}"
        )
        time.sleep(2)


        # --- ARANGODB PRIMA ---
    for idx, (descrizione, queries) in enumerate(generic_queries, 1):
        print_section_header(f"ARANGODB - QUERY {idx}: {descrizione}")
        execute_cold_and_warm_queries(
            dbms_type="arangodb",
            connect_func=connect_arangodb,
            close_func=arangodb_connector.close_arangodb,
            query_func=execute_arangodb_query_wrapper,
            query=queries["arangodb"],
            parameters=None,
            cold_iterations=31,
            warm_iterations=30,
            output_prefix=f"arangodb_query{idx}"
        )
        time.sleep(2)












    print_section_header("COMPLETATO")
    print(f"Finito alle: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")






    


if __name__ == "__main__":
    main()

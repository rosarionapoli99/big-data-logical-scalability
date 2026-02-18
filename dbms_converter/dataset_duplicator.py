from neo4j import GraphDatabase

# Constraint da rimuovere
drop_constraints = [
    "DROP CONSTRAINT constraint_drug_name IF EXISTS;",
    "DROP CONSTRAINT constraint_manufacturer_name IF EXISTS;",
    "DROP CONSTRAINT constraint_outcome_code IF EXISTS;",
    "DROP CONSTRAINT constraint_reaction_description IF EXISTS;",
    "DROP CONSTRAINT constraint_reportsource_code IF EXISTS;",
    "DROP CONSTRAINT constraint_therapy_primaryid IF EXISTS;"
]

cypher_nodes = [
    "MATCH (n:Case)        CREATE (m:Case)        SET m = properties(n), m.id = n.id + '_copy', m.original_id = n.id;",
    "MATCH (n:Drug)        CREATE (m:Drug)        SET m = properties(n), m.id = n.id + '_copy', m.original_id = n.id;",
    "MATCH (n:AgeGroup)    CREATE (m:AgeGroup)    SET m = properties(n), m.id = n.id + '_copy', m.original_id = n.id;",
    "MATCH (n:Outcome)     CREATE (m:Outcome)     SET m = properties(n), m.id = n.id + '_copy', m.original_id = n.id;",
    "MATCH (n:ReportSource)CREATE (m:ReportSource)SET m = properties(n), m.id = n.id + '_copy', m.original_id = n.id;",
    "MATCH (n:Therapy)     CREATE (m:Therapy)     SET m = properties(n), m.id = n.id + '_copy', m.original_id = n.id;",
    "MATCH (n:Manufacturer)CREATE (m:Manufacturer)SET m = properties(n), m.id = n.id + '_copy', m.original_id = n.id;",
    "MATCH (n:Reaction)    CREATE (m:Reaction)    SET m = properties(n), m.id = n.id + '_copy', m.original_id = n.id;",
]

cypher_rels = [
    "MATCH (a:Case)-[r:IS_PRIMARY_SUSPECT]->(b:Drug)      MATCH (copy_a:Case {original_id: a.id}) MATCH (copy_b:Drug {original_id: b.id}) CREATE (copy_a)-[copy_r:IS_PRIMARY_SUSPECT]->(copy_b) SET copy_r = properties(r), copy_r.id = r.id + '_copy', copy_r.original_id = r.id;",
    "MATCH (a:Case)-[r:IS_SECONDARY_SUSPECT]->(b:Drug)    MATCH (copy_a:Case {original_id: a.id}) MATCH (copy_b:Drug {original_id: b.id}) CREATE (copy_a)-[copy_r:IS_SECONDARY_SUSPECT]->(copy_b) SET copy_r = properties(r), copy_r.id = r.id + '_copy', copy_r.original_id = r.id;",
    "MATCH (a:Case)-[r:FALLS_UNDER]->(b:AgeGroup)         MATCH (copy_a:Case {original_id: a.id}) MATCH (copy_b:AgeGroup {original_id: b.id}) CREATE (copy_a)-[copy_r:FALLS_UNDER]->(copy_b) SET copy_r = properties(r), copy_r.id = r.id + '_copy', copy_r.original_id = r.id;",
    "MATCH (a:Case)-[r:REPORTED_BY]->(b:ReportSource)     MATCH (copy_a:Case {original_id: a.id}) MATCH (copy_b:ReportSource {original_id: b.id}) CREATE (copy_a)-[copy_r:REPORTED_BY]->(copy_b) SET copy_r = properties(r), copy_r.id = r.id + '_copy', copy_r.original_id = r.id;",
    "MATCH (a:Case)-[r:RESULTED_IN]->(b:Outcome)          MATCH (copy_a:Case {original_id: a.id}) MATCH (copy_b:Outcome {original_id: b.id}) CREATE (copy_a)-[copy_r:RESULTED_IN]->(copy_b) SET copy_r = properties(r), copy_r.id = r.id + '_copy', copy_r.original_id = r.id;",
    "MATCH (a:Case)-[r:HAS_INTERACTION]->(b:Reaction)     MATCH (copy_a:Case {original_id: a.id}) MATCH (copy_b:Reaction {original_id: b.id}) CREATE (copy_a)-[copy_r:HAS_INTERACTION]->(copy_b) SET copy_r = properties(r), copy_r.id = r.id + '_copy', copy_r.original_id = r.id;",
    "MATCH (a:Case)-[r:REGISTERED]->(b:Manufacturer)      MATCH (copy_a:Case {original_id: a.id}) MATCH (copy_b:Manufacturer {original_id: b.id}) CREATE (copy_a)-[copy_r:REGISTERED]->(copy_b) SET copy_r = properties(r), copy_r.id = r.id + '_copy', copy_r.original_id = r.id;",
    "MATCH (a:Therapy)-[r:PRESCRIBED]->(b:Drug)           MATCH (copy_a:Therapy {original_id: a.id}) MATCH (copy_b:Drug {original_id: b.id}) CREATE (copy_a)-[copy_r:PRESCRIBED]->(copy_b) SET copy_r = properties(r), copy_r.id = r.id + '_copy', copy_r.original_id = r.id;",
    "MATCH (a:Case)-[r:RECEIVED]->(b:Therapy)             MATCH (copy_a:Case {original_id: a.id}) MATCH (copy_b:Therapy {original_id: b.id}) CREATE (copy_a)-[copy_r:RECEIVED]->(copy_b) SET copy_r = properties(r), copy_r.id = r.id + '_copy', copy_r.original_id = r.id;",
    "MATCH (a:Drug)-[r:IS_INTERACTING]->(b:Drug)          MATCH (copy_a:Drug {original_id: a.id}) MATCH (copy_b:Drug {original_id: b.id}) CREATE (copy_a)-[copy_r:IS_INTERACTING]->(copy_b) SET copy_r = properties(r), copy_r.id = r.id + '_copy', copy_r.original_id = r.id;",
    "MATCH (a:Drug)-[r:IS_CONCOMITANT]->(b:Drug)          MATCH (copy_a:Drug {original_id: a.id}) MATCH (copy_b:Drug {original_id: b.id}) CREATE (copy_a)-[copy_r:IS_CONCOMITANT]->(copy_b) SET copy_r = properties(r), copy_r.id = r.id + '_copy', copy_r.original_id = r.id;",
]

def run_clone_queries(uri, user, pwd, n_copies=10):
    driver = GraphDatabase.driver(uri, auth=(user, pwd))
    with driver.session() as session:
        # 1. Droppa constraint per evitare errori di unicità
        for drop in drop_constraints:
            session.run(drop)
        print("Constraint rimosse con successo.")
        
        # 2. Clona nodi e relazioni
        for i in range(n_copies):
            for q in cypher_nodes:
                session.run(q)
            for q in cypher_rels:
                session.run(q)
            print(f"Clonazione {i+1} completata")
    driver.close()

# Esempio d'uso:
run_clone_queries('bolt://localhost:7687', 'neo4j', '11111111', 1)

import os
import csv
import json
from neo4j import GraphDatabase
from arango import ArangoClient

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "11111111"
EXPORT_FOLDER = "dbms_converter/export_csv"
ARANGO_HOST = "http://localhost:8529"
ARANGO_DB = "test"
ARANGO_USER = "root"
ARANGO_PASS = "secretpass"

NODE_LABELS = ["Case", "Drug", "Therapy", "Manufacturer", "Reaction", "Outcome", "ReportSource", "AgeGroup"]
EDGE_TYPES = [
    "IS_PRIMARY_SUSPECT", "IS_SECONDARY_SUSPECT", "IS_CONCOMITANT", "IS_INTERACTING",
    "PRESCRIBED", "RECEIVED", "REGISTERED", "HAS_REACTION", "RESULTED_IN", "REPORTED_BY", "FILLED_BY"
]

def ensure_export_dir():
    if not os.path.exists(EXPORT_FOLDER):
        os.makedirs(EXPORT_FOLDER, exist_ok=True)

def serialize_properties(props):
    if props is None:
        return {}
    def convert(v):
        if isinstance(v, dict):
            return {k: convert(val) for k, val in v.items()}
        elif hasattr(v, "iso_format"):
            return v.iso_format()
        elif hasattr(v, "isoformat"):
            return v.isoformat()
        elif isinstance(v, list):
            return [convert(val) for val in v]
        else:
            return v
    return convert(props)

def export_nodes(session):
    for label in NODE_LABELS:
        query = f"MATCH (n:`{label}`) RETURN elementId(n) AS _id, properties(n) AS properties"
        filename = os.path.join(EXPORT_FOLDER, f"{label}.csv")
        with open(filename, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["_id", "properties"])
            for record in session.run(query):
                properties = serialize_properties(record["properties"])
                writer.writerow([record["_id"], json.dumps(properties)])

def export_edges(session):
    for rel_type in EDGE_TYPES:
        query = (
            f"MATCH (a)-[r:`{rel_type}`]->(b) "
            "RETURN elementId(r) AS _id, elementId(a) AS from_id, elementId(b) AS to_id, properties(r) AS properties"
        )
        filename = os.path.join(EXPORT_FOLDER, f"{rel_type}.csv")
        with open(filename, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["_id", "from_id", "to_id", "properties"])
            for record in session.run(query):
                properties = serialize_properties(record["properties"])
                writer.writerow([record["_id"], record["from_id"], record["to_id"], json.dumps(properties)])

def build_id_label_mapping():
    """Costruisce una mappa elementId → label per tutti i nodi esportati"""
    mapping = {}
    for label in NODE_LABELS:
        path = os.path.join(EXPORT_FOLDER, f"{label}.csv")
        if not os.path.exists(path): continue
        with open(path, encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                mapping[row["_id"]] = label
    return mapping

def import_to_arango():
    client = ArangoClient(hosts=ARANGO_HOST)
    db = client.db(ARANGO_DB, username=ARANGO_USER, password=ARANGO_PASS)

    id_to_label = build_id_label_mapping()  # Mappatura globale id → label

    # Import nodi
    for label in NODE_LABELS:
        path = os.path.join(EXPORT_FOLDER, f"{label}.csv")
        if not os.path.exists(path): continue
        if db.has_collection(label):
            db.delete_collection(label)
        col = db.create_collection(label)
        with open(path, encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            docs = []
            for row in reader:
                doc = {"_key": row["_id"]}
                if row["properties"] and row["properties"] != "{}":
                    doc.update(json.loads(row["properties"]))
                docs.append(doc)
            if docs: col.insert_many(docs)
        print(f"Imported {label} nodes: {len(docs)}")

    # Import archi
    for rel_type in EDGE_TYPES:
        path = os.path.join(EXPORT_FOLDER, f"{rel_type}.csv")
        if not os.path.exists(path): continue
        if db.has_collection(rel_type):
            db.delete_collection(rel_type)
        col = db.create_collection(rel_type, edge=True)
        with open(path, encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            docs = []
            for row in reader:
                from_label = id_to_label.get(row["from_id"], "UNKNOWN")
                to_label = id_to_label.get(row["to_id"], "UNKNOWN")
                doc = {
                    "_key": row["_id"],
                    "_from": f"{from_label}/{row['from_id']}",
                    "_to": f"{to_label}/{row['to_id']}"
                }
                if row["properties"] and row["properties"] != "{}":
                    doc.update(json.loads(row["properties"]))
                docs.append(doc)
            if docs: col.insert_many(docs)
        print(f"Imported {rel_type} edges: {len(docs)}")

if __name__ == "__main__":
    ensure_export_dir()
    print("Connecting to Neo4j...")
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    with driver.session() as session:
        print("Exporting nodes...")
        export_nodes(session)
        print("Exporting edges...")
        export_edges(session)
    print("Export completed in folder:", EXPORT_FOLDER)
    print("Connecting to ArangoDB and importing data...")
    import_to_arango()
    print("ArangoDB import completed.")

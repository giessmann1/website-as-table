import database_wrapper
import re
from neo4j import GraphDatabase

def extract_nodes_and_relationships(pos_data):
    """
    Extract nodes, relationships, and properties from POS-tagged data in list-of-lists format
    while assigning adjectives to nouns and adverbs to verbs based on positioning.
    """
    nouns = []
    verbs = []
    properties = {}

    # Parse the data to separate nouns, verbs, and position-specific properties
    for i, entry in enumerate(pos_data):
        original, lemma, pos = entry

        if re.compile(r'N.*').match(pos):
            nouns.append((lemma, i))  # Track noun position for accurate relationship mapping
            properties[lemma] = []
        elif re.compile(r'V.*').match(pos):
            verbs.append((lemma, i))  # Track verb position for accurate relationship mapping
            properties[lemma] = []
        elif re.compile(r'ADJ.*').match(pos) and i > 0 and re.compile(r'N.+').match(pos_data[i - 1][2]):
            properties[nouns[-1][0]].append(lemma)  # Assign ADJ to the last NOUN
        elif re.compile(r'ADV.*').match(pos) and verbs:
            properties[verbs[-1][0]].append(lemma)  # Assign ADV to the last VERB

    # Prepare output for nodes
    nodes = [{"label": noun[0], "properties": properties[noun[0]]} for noun in nouns]

    # Generate relationships based on nouns and verbs positioning
    relationships = []
    verb_index = 0
    
    for i in range(len(nouns) - 1):
        from_noun, from_pos = nouns[i]
        to_noun, to_pos = nouns[i + 1]

        if verb_index < len(verbs) and verbs[verb_index][1] < to_pos:
            relationship = {
                "from": from_noun,
                "to": to_noun,
                "type": verbs[verb_index][0],
                "properties": properties[from_noun] + properties[verbs[verb_index][0]]
            }
            relationships.append(relationship)
            verb_index += 1
        else:
            relationship = {
                "from": from_noun,
                "to": to_noun,
                "type": "RELATES_TO",
                "properties": properties[from_noun]
            }
            relationships.append(relationship)

    # Return data structure for Neo4j import
    if relationships:
        return {
            "nodes": nodes,
            "relationships": relationships
        }
    else:
        return None
    
class Neo4jConnector:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def create_nodes_and_relationships(self, neo4j_data):
        with self.driver.session() as session:
            # Create nodes
            for node in neo4j_data['nodes']:
                session.run(
                """
                MERGE (n:Node {label: $label})
                ON CREATE SET n.properties = $properties
                ON MATCH SET n.properties = $properties
                """,
                label=node['label'],
                properties=node['properties']
            )

            # Create relationships
            for relationship in neo4j_data['relationships']:
                session.run(
                    f"""
                    MATCH (a:Node {{label: $from_label}}), (b:Node {{label: $to_label}})
                    MERGE (a)-[r:{relationship['type']}]->(b)
                    ON CREATE SET r.properties = $properties
                    """,
                    from_label=relationship['from'],
                    to_label=relationship['to'],
                    properties=relationship['properties']
            )
                

if __name__ == '__main__':
    try:
        db = database_wrapper.mongo_authenticate('./')['scrapydb']
        cols = db.list_collection_names()
        print('Connection working:', cols)
    except Exception as e:
        print('Connection not working.')
        print(e)
        exit(1)


    col = db["websitedata"]

    data = database_wrapper.get_latest_entry_by_source(col, "https://www.neumuenster.de/gesellschaft-soziales/familien-und-jugendhilfe/vormundschaften")

    text = database_wrapper.POS_tagger(data["PreprocessedData"], data["lang"])

    neo4j_connector = Neo4jConnector("bolt://localhost:7687", "neo4j", "password")

    for s in text:
        neo4j_data = extract_nodes_and_relationships(s)
        if neo4j_data is not None:
            print("New relationships loaded into Neo4j.")
            neo4j_connector.create_nodes_and_relationships(neo4j_data)
        else:
            print("No relationships found. Nothing to load into Neo4j.")

    neo4j_connector.close()
import urllib
import pymongo
import os
import hashlib
from HanTa import HanoverTagger as ht
tagger_de = ht.HanoverTagger('morphmodel_ger.pgz')
tagger_en = ht.HanoverTagger('morphmodel_en.pgz')
import nltk
import re
from neo4j import GraphDatabase
# import ssl

# try:
#     _create_unverified_https_context = ssl._create_unverified_context
# except AttributeError:
#     pass
# else:
#     ssl._create_default_https_context = _create_unverified_https_context

# nltk.download()

# Returns connection object at database level
def mongo_authenticate(path):
    with open(f'{path}.secrets/host.txt', 'r') as f_open:
        host = f_open.readlines()[0]
    host = urllib.parse.quote_plus(host)

    port = 27017
    with open(f'{path}.secrets/mongodb_user.txt', 'r') as f_open:
        username = f_open.readlines()[0]
    username = urllib.parse.quote_plus(username)
    with open(f'{path}.secrets/mongodb_pwd.txt', 'r') as f_open:
        password = f_open.readlines()[0]
    password = urllib.parse.quote_plus(password)

    client = pymongo.MongoClient(
        f'mongodb://{username}:{password}@{host}:{port}', authSource='admin'
    )
    return client

# Inserts one document in collection, requires connection object on collection level and dictionary
def insert_one_in_collection(col, doc):
    col.insert_one(doc)

# Get website data based on given SourceURL
def get_all_entries_by_source(col, source_url):
    return list(col.find({"SourceURL": source_url}))

# Get latest website data based on given SourceURL
def get_latest_entry_by_source(col, source_url):
    return col.find_one(
        {"SourceURL": source_url},
        sort=[('Datetime', pymongo.DESCENDING)]
    )

# Returns all entries
def get_all_entries(col, start_url):
    return list(col.find({"StartURL": start_url}))

# Update single entries, adding new columns is also possible
def update_row(col, source_url, timestamp, field, value):
    query = {
        "SourceURL": source_url,
        "Datetime": timestamp
    }
    update = {"$set": {field: value}}
    col.update_one(query, update, upsert=True)

# Update many entries, adding new columns is also possible
def update_many_rows(col, query, field, value):
    update = {"$set": {field: value}}
    col.update_many(query, update, upsert=True)

# Extract IMG data as file, given a website data as dict with Type = IMG
def extract_img(website_data):
    if (website_data is None):
        print("Website data not found, None given.")
        return
    if (website_data["Type"] != "IMG"):
        print("The given website data is not of type IMG.")
        return

    img_data = website_data["Data"]
    filename = os.path.join("./", website_data["SourceURL"].split("/")[-1])
    if (filename.endswith(('.jpg', '.jpeg', '.png', '.gif')) is False):
        filename += ".jpg"  # Default image extension if none is present

    # Write the binary data to file
    with open(filename, "wb") as file:
        file.write(img_data)

    print(f"Saved image file to {filename}")

# Extract PDF data as file, given a website data as dict with Type = PDF
def extract_pdf(website_data):
    if (website_data is None):
        print("Website data not found, None given.")
        return
    if (website_data["Type"] != "PDF"):
        print("The given website data is not of type PDF.")
        return

    pdf_data = website_data["Data"]
    filename = os.path.join("./", website_data["SourceURL"].split("/")[-1])

    # Write the binary data to file
    with open(filename, "wb") as file:
        file.write(pdf_data)

    print(f"Saved pdf file to {filename}")

# SHA-256 a given object
def hash_object(obj):
    if (isinstance(obj, bytes)):
        obj_bytes = obj
    else:
        obj_bytes = str.encode(obj)

    hash_object = hashlib.sha256(obj_bytes)
    hex_dig = hash_object.hexdigest()
    return hex_dig

def POS_tagger(text, lang):
    if lang == "de":
        sent = text.split('\n')
        tokenized_POS_tagged_sent = list()
        for s in sent:
            tokenized_sent = (nltk.word_tokenize(s))
            tokenized_POS_tagged_sent.append(tagger_de.tag_sent(tokenized_sent))
        return tokenized_POS_tagged_sent
    
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

    
# Run the module directly to check if connection work
if __name__ == '__main__':
    try:
        db = mongo_authenticate('./')['scrapydb']
        cols = db.list_collection_names()
        print('Connection working:', cols)
    except Exception as e:
        print('Connection not working.')
        print(e)
        exit(1)


    col = db["websitedata"]

    data = get_latest_entry_by_source(col, "https://www.neumuenster.de/gesellschaft-soziales/familien-und-jugendhilfe/vormundschaften")

    text = POS_tagger(data["PreprocessedData"], data["lang"])

    neo4j_connector = Neo4jConnector("bolt://localhost:7687", "neo4j", "password")

    for s in text:
        neo4j_data = extract_nodes_and_relationships(s)
        if neo4j_data is not None:
            print("New relationships loaded into Neo4j.")
            neo4j_connector.create_nodes_and_relationships(neo4j_data)
        else:
            print("No relationships found. Nothing to load into Neo4j.")

    neo4j_connector.close()

    
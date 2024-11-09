import urllib
import pymongo
import os
import hashlib
from HanTa import HanoverTagger as ht
tagger_de = ht.HanoverTagger('morphmodel_ger.pgz')
tagger_en = ht.HanoverTagger('morphmodel_en.pgz')
import nltk
import ssl

try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context

nltk.download('punkt')

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

# Multi-language (de, en) POS-tagging a multi-line, multi-sentence text
def POS_tagger(text, lang):
    sent = text.split('\n')
    tokenized_POS_tagged_sent = list()
    if lang == "de":
        for s in sent:
            tokenized_sent = (nltk.word_tokenize(s))
            tokenized_POS_tagged_sent.append(tagger_de.tag_sent(tokenized_sent))
        return tokenized_POS_tagged_sent
    elif lang == "en":
        for s in sent:
            tokenized_sent = (nltk.word_tokenize(s))
            tokenized_POS_tagged_sent.append(tagger_en.tag_sent(tokenized_sent))
        return tokenized_POS_tagged_sent
    return "Language not supported."

    
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
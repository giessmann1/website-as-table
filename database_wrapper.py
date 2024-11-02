import urllib
import pymongo
import base64
import os

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
        sort = [('Datetime', pymongo.DESCENDING)]
        )

# Extract IMG data as file, given a website data as dict with Type = IMG
def extract_img(website_data):
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
    if (website_data["Type"] != "PDF"):
        print("The given website data is not of type PDF.")
        return
    
    pdf_data = website_data["Data"]
    filename = os.path.join("./", website_data["SourceURL"].split("/")[-1])

    # Write the binary data to file
    with open(filename, "wb") as file:
        file.write(pdf_data)

    print(f"Saved pdf file to {filename}")
    

# Run the module directly to check if connection works
if __name__ == '__main__':
    try:
        db = mongo_authenticate('./')['scrapydb']
        cols = db.list_collection_names()
        print('Connection working:', cols)
    except Exception as e:
        print('Connection not working.')
        print(e)
        exit(1)

    collection = db['websitedata']
    
    extract_pdf(get_latest_entry_by_source(collection, "https://benhoyt.com/cv/ben-hoyt-cv-resume.pdf"))
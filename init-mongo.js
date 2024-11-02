db = new Mongo().getDB("scrapydb");
db.createCollection('websitedata', { capped: false });
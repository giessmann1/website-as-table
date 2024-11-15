# Website-as-Table
Easy-to-use, recursive, crawling-protection breaking website scraper, presenting the data in a table. It comes with separate retrieval of text, img and pdf file formats as well as changes detection. 

❗ This is an actively maintained repository. If you have suggestions for further improvement or find bugs: [Email me](mailto:nico.giessmann@uni-luebeck.de)

## Setup instructions

Create Python virtual environment:
```bash
python3 -m venv .env
source .env/bin/activate
pip3 install -r requirements.txt
```

Create essential files:
```bash
# Create .secrets/ directory
mkdir .secrets/

# Create mongodb_user.txt and mongodb_pwd.txt and set your own username and password (no update in Python scripts necessary). Be aware of newlines, which need to be removed!

echo -n "admin" > .secrets/mongodb_user.txt
echo -n "password" > .secrets/mongodb_pwd.txt
echo -n "localhost" > .secrets/host.txt
echo -n "example.com" > urls.txt # Enter the URLs you want to crawl here
```

Docker installation needed, see: https://docs.docker.com/engine/install/.

Starting the db:
```bash
sh startdb.sh
```

## Start crawling

Manual (one-time) start:
```bash
# Make sure venv is activated and db is up running
scrapy runspider generic_spider.py --nolog -a url_file=urls.txt
```

For automated start with crontab, see crawler-starter.sh
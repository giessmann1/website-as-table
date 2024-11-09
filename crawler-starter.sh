#!/bin/bash

# Add this to your crontab 0 23 * * 0 /bin/bash $HOME/interamtdb/scraper-starter.sh

cd $HOME/website-as-table
filename=crawler-log.txt
if [ ! -f $filename ]
then
    touch $filename
fi

# If you use Python Virtual Environment
source .env/bin/activate
echo $(date +'%Y-%m-%d') >> $filename
scrapy runspider generic_spider.py --nolog -a url_file=urls.txt >> $filename
deactivate
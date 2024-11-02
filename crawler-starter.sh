#!/bin/bash

# Add this to your crontab 0 23 * * * /bin/bash $HOME/interamtdb/scraper-starter.sh

cd $HOME/website-as-table
filename=crawler-log.txt
if [ ! -f $filename ]
then
    touch $filename
fi

# If you use Python Virtual Environment
source .env/bin/activate
echo $(date +'%Y-%m-%d') | tr "\n" " " >> $filename
scrapy runspider generic_spider.py --nolog -a start_url=https://www.mach.de >> $filename
deactivate
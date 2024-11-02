from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from datetime import datetime
from database_wrapper import *

class GenericSpider(CrawlSpider):
    name = 'generic_spider'
    allowed_domains = []
    custom_settings = {
        "DEPTH_LIMIT": 0, # No limit on crawl depth
        "DOWNLOAD_DELAY": 2, # 2-second delay between requests to avoid overloading servers
        "ROBOTSTXT_OBEY": False # Set this to True to respect robots.txt
    }

    rules = (
        Rule(LinkExtractor(), callback='parse_item', follow=True),
    )

    def __init__(self, start_url=None, *args, **kwargs):
        super(GenericSpider, self).__init__(*args, **kwargs)
        if start_url:
            self.start_urls = [start_url]
            self.allowed_domains = [start_url.split("//")[-1].split("/")[0]]
            self.start_url = start_url
        else:
            raise ValueError("Please provide a start_url argument with the URL to scrape.")

        # Initialize MongoDB connection
        self.client = mongo_authenticate('./')
        self.db = self.client['scrapydb']
        self.collection = self.db['websitedata']


    def parse_item(self, response):
        # Common data for each page
        page_data = {
            "StartURL": self.start_url,
            "SourceURL": response.url,
            "Datetime": datetime.now(),
            "Type": "HTML",
            "LinksTo": [link for link in response.css('a::attr(href)').getall() 
                         if link.startswith('/') or self.allowed_domains[0] in link]
        }

        # Insert HTML data
        page_data["Data"] = response.body.decode('utf-8')
        insert_one_in_collection(self.collection, page_data)
        
        # Parse and save images
        for img_url in response.css('img::attr(src)').getall():
            yield response.follow(img_url, self.parse_image)
        
        # Parse and save PDFs
        for pdf_url in response.css('a::attr(href)').re(r'.*\.pdf$'):
            yield response.follow(pdf_url, self.parse_pdf)

    def parse_image(self, response):
        # Save image data
        img_data = {
            "StartURL": self.start_url,
            "SourceURL": response.url,
            "Datetime": datetime.now(),
            "Type": "IMG",
            "Data": response.body,
            "LinksTo": []
        }
        insert_one_in_collection(self.collection, img_data)

    def parse_pdf(self, response):
        # Save PDF data
        pdf_data = {
            "StartURL": self.start_url,
            "SourceURL": response.url,
            "Datetime": datetime.now(),
            "Type": "PDF",
            "Data": response.body,
            "LinksTo": []
        }
        insert_one_in_collection(self.collection, pdf_data)

    def close(self, reason):
        # Close MongoDB connection when spider closes
        self.client.close()
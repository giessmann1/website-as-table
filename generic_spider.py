from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from datetime import datetime
import database_wrapper
from bs4 import BeautifulSoup
import re
from urllib.parse import urlparse

# HTML Tag Categories, based on: https://developer.mozilla.org/en-US/docs/Web/HTML/Element

# Root Element
ROOT_ELEMENTS = ["html"]

# Document Metadata
DOCUMENT_METADATA = ["base", "head", "link", "meta", "style", "title"]

# Sectioning Root
SECTIONING_ROOT = ["body"]

# Content Sectioning
CONTENT_SECTIONING = ["address", "article", "aside", "footer", "header",
                      "h1", "h2", "h3", "h4", "h5", "h6", "hgroup", "main", "nav", "section"]

# Text Content
TEXT_CONTENT = ["blockquote", "dd", "div", "dl", "dt", "figcaption",
                "figure", "hr", "li", "main", "ol", "p", "pre", "ul"]

# Inline Text Semantics
INLINE_TEXT_SEMANTICS = ["a", "abbr", "b", "bdi", "bdo", "br", "cite", "code", "data", "dfn", "em", "i", "kbd", "mark",
                         "q", "rb", "rp", "rt", "rtc", "ruby", "s", "samp", "small", "span", "strong", "sub", "sup", "time", "u", "var", "wbr"]

# Image and Multimedia
IMAGE_MULTIMEDIA = ["area", "audio", "img", "map", "track", "video"]

# Embedded Content
EMBEDDED_CONTENT = ["embed", "iframe", "object",
                    "param", "picture", "portal", "source"]

# Scripting
SCRIPTING = ["canvas", "noscript", "script"]

# Demarcating Edits
DEMARCATING_EDITS = ["del", "ins"]

# Table Content
TABLE_CONTENT = ["caption", "col", "colgroup", "table",
                 "tbody", "td", "tfoot", "th", "thead", "tr"]

# Forms
FORMS = ["button", "datalist", "fieldset", "form", "input", "label", "legend",
         "meter", "optgroup", "option", "output", "progress", "select", "textarea"]

# Interactive Elements
INTERACTIVE_ELEMENTS = ["details", "dialog", "menu", "summary"]

# Web Components
WEB_COMPONENTS = ["slot", "template"]

# Obsolete and Deprecated Elements
OBSOLETE_ELEMENTS = ["applet", "acronym", "bgsound", "dir", "frame", "frameset", "noframes", "isindex",
                     "keygen", "listing", "menuitem", "nextid", "noembed", "plaintext", "rb", "rtc", "strike", "xmp"]

# SVG Elements
SVG_ELEMENTS = ["svg", "animate", "circle", "ellipse", "line", "path", "polygon", "polyline", "rect", "text",
                "g", "defs", "filter", "linearGradient", "radialGradient", "stop", "use", "symbol", "marker", "pattern"]

# MathML Elements
MATHML_ELEMENTS = ["math", "mi", "mo", "mn", "ms", "mtext", "mspace", "msub", "msup", "msubsup", "mfrac", "mroot", "msqrt", "mstyle",
                   "merror", "mpadded", "mphantom", "mfenced", "menclose", "munder", "mover", "munderover", "mtable", "mtr", "mtd", "mlabeledtr"]


class GenericSpider(CrawlSpider):
    name = 'generic_spider'
    allowed_domains = []
    custom_settings = {
        "DEPTH_LIMIT": 0,  # No limit on crawl depth
        "DOWNLOAD_DELAY": 1,  # 1-second delay between requests to avoid overloading servers
        "ROBOTSTXT_OBEY": False  # Set this to True to respect robots.txt
    }

    rules = (
        Rule(LinkExtractor(), callback='parse_item', follow=True),
    )

    def __init__(self, url_file=None, *args, **kwargs):
        super(GenericSpider, self).__init__(*args, **kwargs)
        if url_file:
            # Load start URLs from the text file
            with open(url_file, 'r') as f:
                self.start_urls = [line.strip() for line in f if line.strip()]

            # Set allowed_domains based on each start URL's domain
            self.allowed_domains = [
                urlparse(url).netloc for url in self.start_urls]
        else:
            raise ValueError(
                "Please provide a url_file argument with the path to a text file containing URLs.")

        # Initialize MongoDB connection
        self.client = database_wrapper.mongo_authenticate('./')
        self.db = self.client['scrapydb']
        self.collection = self.db['websitedata']

        # Initialize page counters for each URL
        self.page_counts = {url: 0 for url in self.start_urls}

    def parse_item(self, response):
        # Determine the starting URL for this response
        start_url = next((url for url in self.start_urls if urlparse(
            url).netloc in response.url), None)

        # Get HTML data
        data_raw = response.body.decode('utf-8')
        data_preprocessed = clean_html(data_raw)

        # No usefull information on this side
        if data_preprocessed == "":
            print(f"'{response.url}' has no usefull information and is not crawled.")
            return

        hash = database_wrapper.hash_object(data_preprocessed)

        # Check if SourceURL was crawled before and changes occured
        latest_entry = database_wrapper.get_latest_entry_by_source(
            self.collection, response.url)
        if latest_entry is not None:
            if latest_entry["Hash"] == hash:
                print(
                    f"'{response.url}' was crawled before and no changes occured.")
                return

        # Extract all links on the page (same domain only)
        current_links = [link for link in response.css('a::attr(href)').getall()
                         if link.startswith('/') or any(domain in link for domain in self.allowed_domains)]

        # Common data for each page
        page_data = {
            "StartURL": start_url,
            "SourceURL": response.url,
            "Datetime": datetime.now(),
            "Type": "HTML",
            "LinksTo": current_links,
            "title": response.css('title::text').get(),
            "meta_description": response.css('meta[name="description"]::attr(content)').get(),
            "meta_keywords": response.css('meta[name="keywords"]::attr(content)').get(),
            "RawData": data_raw,
            "PreprocessedData": data_preprocessed,
            "Hash": hash
        }
        database_wrapper.insert_one_in_collection(self.collection, page_data)
        print(f"'{response.url}' is crawled.")
        # Increment page count for this specific start URL
        self.page_counts[start_url] += 1

        # Parse and save images
        for img_url in response.css('img::attr(src)').getall():
            yield response.follow(img_url, self.parse_image)

        # Parse and save PDFs
        for pdf_url in response.css('a::attr(href)').re(r'.*\.pdf$'):
            yield response.follow(pdf_url, self.parse_pdf)

    def parse_image(self, response):
        # Determine the starting URL for this response
        start_url = next((url for url in self.start_urls if urlparse(
            url).netloc in response.url), None)

        # Get HTML data
        data_raw = response.body
        hash = database_wrapper.hash_object(data_raw)

        # Check if SourceURL was crawled before and changes occured
        latest_entry = database_wrapper.get_latest_entry_by_source(
            self.collection, response.url)
        if latest_entry is not None:
            if latest_entry["Hash"] == hash:
                print(
                    f"'{response.url}' was crawled before and no changes occured.")
                return

        # Save image data
        img_data = {
            "StartURL": start_url,
            "SourceURL": response.url,
            "Datetime": datetime.now(),
            "Type": "IMG",
            "LinksTo": [],
            "RawData": data_raw,
            "Hash": hash
        }
        database_wrapper.insert_one_in_collection(self.collection, img_data)
        print(f"'{response.url}' is crawled.")
        # Increment page count for this specific start URL
        self.page_counts[start_url] += 1

    def parse_pdf(self, response):
        # Determine the starting URL for this response
        start_url = next((url for url in self.start_urls if urlparse(
            url).netloc in response.url), None)

        # Get HTML data
        data_raw = response.body
        hash = database_wrapper.hash_object(data_raw)

        # Check if SourceURL was crawled before and changes occured
        latest_entry = database_wrapper.get_latest_entry_by_source(
            self.collection, response.url)
        if latest_entry is not None:
            if latest_entry["Hash"] == hash:
                print(
                    f"'{response.url}' was crawled before and no changes occured.")
                return

        # Save PDF data
        pdf_data = {
            "StartURL": start_url,
            "SourceURL": response.url,
            "Datetime": datetime.now(),
            "Type": "PDF",
            "LinksTo": [],
            "RawData": data_raw,
            "Hash": hash
        }
        database_wrapper.insert_one_in_collection(self.collection, pdf_data)
        print(f"'{response.url}' is crawled.")
        # Increment page count for this specific start URL
        self.page_counts[start_url] += 1

    def close(self, reason):
        # Print the total number of pages crawled for each URL
        for url, count in self.page_counts.items():
            print(f"Website {url}: Total pages crawled: {count}")

        # Close MongoDB connection
        self.client.close()

# Clean up HMTL / preprocessing function
def clean_html(raw_html):
    # Credit to: https://stackoverflow.com/questions/328356/extracting-text-from-html-file-using-python
    soup = BeautifulSoup(raw_html, features="html.parser")

    # Remove HMTL elements not relevant for content
    for tag in soup(DOCUMENT_METADATA + IMAGE_MULTIMEDIA + EMBEDDED_CONTENT + SCRIPTING + DEMARCATING_EDITS + FORMS + INTERACTIVE_ELEMENTS + WEB_COMPONENTS + OBSOLETE_ELEMENTS + SVG_ELEMENTS + MATHML_ELEMENTS + ["footer", "header", "nav"]):
        tag.extract()

    # Unwrap inline elements (except br), so that the text is semantically understood when performing get_text()
    for tag in soup.findAll(INLINE_TEXT_SEMANTICS):
        tag.unwrap()

    # Dirty solution: reparsing needed for BS to understand the unwrap
    soup = BeautifulSoup(soup.prettify(), features="html.parser")

    for tag in soup.find_all(TEXT_CONTENT):  # True finds all tags
        if tag.string:  # Only modify if tag has text
            tag.string = re.sub(
                ' +', ' ', tag.string.replace("\n", "")).strip()

    return soup.get_text(separator='\n', strip=True)

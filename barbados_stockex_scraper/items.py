# -*- coding: utf-8 -*-
import scrapy

class PDFItem(scrapy.Item):
    file_urls = scrapy.Field()
    files = scrapy.Field()



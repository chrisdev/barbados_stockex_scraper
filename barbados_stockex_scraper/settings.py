# -*- coding: utf-8 -*-

# Scrapy settings for barbados_stockex_scraper project
#
# For simplicity, this file contains only the most important settings by
# default. All the other settings are documented here:
#
#     http://doc.scrapy.org/en/latest/topics/settings.html
#


SPIDER_MODULES = ['barbados_stockex_scraper.spiders']
NEWSPIDER_MODULE = 'barbados_stockex_scraper.spiders'
ITEM_PIPELINES = {
    'barbados_stockex_scraper.files.FilesPipeline': 100,
}
try:
    from local_settings import *
except ImportError:
    pass

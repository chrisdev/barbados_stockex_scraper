from scrapy.spider import BaseSpider
from dateutil.parser import parse
from datetime import date
from barbados_stockex_scraper.items import PDFItem
from dateutil import rrule
from dateutil.rrule import MO, TU, WE, TH, FR


def bdate_range(start, end):
    return rrule.rrule(
        rrule.DAILY,
        byweekday=(MO, TU, WE, TH, FR),
        dtstart=start,
        until=end
    )


class BSETrading(BaseSpider):
    name = 'bse_trading_pdf'
    allowed_domains = ['http://www.bse.com.bb']
    start_urls = ["http://www.bse.com.bb", ]

    def __init__(self, start_date=None, end_date=None):
        self.start = None
        self.end = None
        if start_date is None and end_date is None:
            self.start = date.today()
        else:
            if start_date:
                self.start = parse(start_date)
            else:
                self.start = date.today()
            if end_date:
                self.end = parse(end_date)
            else:
                self.end = None

    @property
    def file_urls(self):
        domain = "http://www.bse.com.bb"
        path = "sites/default/files/trading_reports"

        if self.end:
            return [
                "{0}/{1}/{2}.pdf".format(domain, path, dd.strftime("%Y%m%d"))
                for dd in bdate_range(start=self.start, end=self.end)]
        else:
            return [
                "{0}/{1}/{2}.pdf".format(
                    domain, path, self.start.strftime("%Y%m%d")
                )
            ]

    def parse(self, response):
        for url in self.file_urls:
            yield PDFItem(
                file_urls=[{
                    'file_url': url,
                    'file_name': url.split('/')[-1]
                }]
            )

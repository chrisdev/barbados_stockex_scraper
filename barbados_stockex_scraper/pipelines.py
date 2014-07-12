from barbados_stockex_scraper.files import FilesPipeline
from scrapy.http import Request
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.layout import LAParams
from pdfminer.converter import TextConverter
from cStringIO import StringIO
from scrapy.utils.misc import md5sum
from scrapy import log
import re
import calendar
from dateutil import parser

date_expr = re.compile(
    r'(?:%s) (?:\d{1}|\d{2}), \d{4}' % '|'.join(
    calendar.month_name[1:])
)
nums_expr = re.compile(r'\n\d+(?:,\d+)?')
num_clean = lambda x: x.replace(',', '').strip()


class PDFPipeline(FilesPipeline):
    def get_media_requests(self, item, info):
        for file_spec in item['file_urls']:
            yield Request(
                url=file_spec["file_url"], meta={"file_spec": file_spec}
            )

    def file_path(self, request, response=None, info=None):
        return request.meta["file_spec"]["file_name"]

    def process_index_data(self, data):
        index_items = ['local', 'crosslist', 'composite']
        dd = parser.parse(date_expr.findall(data)[0]).strftime('%Y-%m-%d')
        nums = nums_expr.findall(data)
        ix_data = [
            (dd, "{}-ix".format(x), num_clean(y))
            for x, y in zip(*[index_items, nums[:3]])
        ]
        mkt_cap = [
            (dd, "{}-mktcap".format(x), num_clean(y))
            for x, y in zip(*[index_items, nums[6:9]])
        ]
        return ix_data + mkt_cap

    def process_security_data(self, instr):
        dd = parser.parse(date_expr.findall(instr)[0]).strftime('%Y-%m-%d')
        data_list = instr.splitlines()
        start = data_list.index('Decline ') + 2
        for cnt, r in enumerate(data_list[start:], start=start):
            if not r:
                pos = cnt + 1
                break
        symbol_list = data_list[start:pos - 1]
        ret_list = []
        for col in ['volume', 'high', 'close', 'change']:
            ser_name = ['{}-{}'.format(s, col) for s in symbol_list]
            ret_list += [(dd, x, num_clean(y)) for x, y in
                         zip(*[ser_name,
                               data_list[pos: pos + len(symbol_list) + 1]])]
            pos = pos + len(symbol_list) + 1

        return ret_list

    def file_downloaded(self, response, request, info):

        path = self.file_path(request, response=response, info=info)
        buf = StringIO(response.body)

        # Define parameters to the PDF device objet
        rsrcmgr = PDFResourceManager()
        outstr = StringIO()
        laparams = LAParams()
        codec = 'utf-8'

        # Create a PDF device object
        device = TextConverter(
            rsrcmgr, outstr, codec=codec, laparams=laparams
        )

        # Create a PDF interpreter object
        interpreter = PDFPageInterpreter(rsrcmgr, device)
        #page
        for cnt, page in enumerate(PDFPage.get_pages(buf), start=0):
            interpreter.process_page(page)
            data = outstr.getvalue()
            #if cnt == 1:
                #ix_data = self.process_index_data(data)
                #for row in ix_data:
                    #log.msg(','.join(row),
                            #level=log.WARNING
                            #)
            if cnt == 0:
                ix_data = self.process_security_data(data)
                for row in ix_data:
                    log.msg(','.join(row),
                            level=log.WARNING
                            )

        self.store.persist_file(path, buf, info)
        checksum = md5sum(buf)
        return checksum

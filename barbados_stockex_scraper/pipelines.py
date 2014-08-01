from barbados_stockex_scraper.files import FilesPipeline
from scrapy.http import Request
from cStringIO import StringIO
from scrapy.utils.misc import md5sum
import tempfile
import subprocess

from scrapy import log
import re
import calendar
from dateutil import parser

date_expr = re.compile(
    r'(?:%s) (?:\d{1}|\d{2}), \d{4}' % '|'.join(
    calendar.month_name[1:])
)
nums_expr = re.compile(r'\n\d+(?:,\d+)?')
num_clean = lambda x: x.replace(',', '').replace('$','').strip()


class PDFPipeline(FilesPipeline):
    def get_media_requests(self, item, info):
        for file_spec in item['file_urls']:
            yield Request(
                url=file_spec["file_url"], meta={"file_spec": file_spec}
            )

    def file_path(self, request, response=None, info=None):
        return request.meta["file_spec"]["file_name"]

    def get_index_data(self, dd, lns):
        ixs = [i for i, j in enumerate(lns) if 'Local' in j]
        ix_start = ixs[0]
        index_items = ['LOCAL', 'CROSSLIST', 'COMPOSITE']
        ret_list = []
        for c, itm in enumerate(index_items):
            ln = lns[ix_start + c].split()
            ret_list.append([dd, itm, num_clean(ln[1])])

        ix_start = ixs[1]
        for c, itm in enumerate(index_items):
            ln = lns[ix_start + c].split()
            ret_list.append([dd, '{}-MKT_CAP'.format(itm), num_clean(ln[1])])

        return ret_list

    def market_data(self, dd, lns, search_str):
        ixs = [i for i, j in enumerate(lns) if search_str in j]
        if not ixs:
            return []
        ix_start = ixs[1] + 4
        ret_list = []
        for ln in lns[ix_start:]:
            if not ln:
                break
            d_list = ln.split()
            nums = d_list[-5:]
            symbol = ' '.join(d_list[: len(d_list) - 5])
            volume = num_clean(nums[0])
            high = num_clean(nums[1])
            low = num_clean(nums[2])
            close = num_clean(nums[3])
            change = num_clean(nums[4])
            sopen = float(close) - float(change)
            res = [
                [dd, '{}-VOLUME'.format(symbol), volume],
                [dd, '{}-HIGH'.format(symbol), high],
                [dd, '{}-LOW'.format(symbol), low],
                [dd, '{}-HIGH'.format(symbol), high],
                [dd, '{}-OPEN'.format(symbol), sopen],
            ]
            ret_list.append(res)

        return ret_list

    def text_extract(self, buf):
        tf = tempfile.NamedTemporaryFile()
        tf.write(buf.read())
        tf.seek(0)

        otf = tempfile.NamedTemporaryFile()
        out, err = subprocess.Popen(
            ['pdftotext', "-layout", tf.name, otf.name]).communicate()

        return otf.read()

    def file_downloaded(self, response, request, info):

        path = self.file_path(request, response=response, info=info)
        buf = StringIO(response.body)
        txt = self.text_extract(buf)
        lns = [ln.strip() for ln in txt.splitlines()]
        dd = parser.parse(lns[1]).strftime('%Y-%m-%d')

        data = self.get_index_data(dd, lns)
        for d in data:
            log.msg("|".join(d), level=log.INFO)

        reg_mkt = self.market_data(dd, lns, "Regular Market")

        for r in reg_mkt:
            for el in r:
                log.msg(str(el), level=log.INFO)

        self.store.persist_file(path, buf, info)
        checksum = md5sum(buf)
        return checksum

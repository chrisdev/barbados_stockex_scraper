from barbados_stockex_scraper.files import FilesPipeline
from scrapy.http import Request


class PDFPipeline(FilesPipeline):
    def get_media_requests(self, item, info):
        for file_spec in item['file_urls']:
            yield Request(
                url=file_spec["file_url"], meta={"file_spec": file_spec}
            )

    def file_path(self, request, response=None, info=None):
        return request.meta["file_spec"]["file_name"]

from pdftables import PDFDocument
from pdftables import page_to_tables

fh = open('tests/20140627.pdf', 'rb')
doc = PDFDocument.from_fileobj(fh)
page - doc.get_page(1)
tables = page_to_tables(page)

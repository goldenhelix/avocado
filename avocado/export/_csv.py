import csv
from _base import BaseExporter


class UnicodeWriter(object):
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.

    Adapted from https://github.com/jdunck/python-unicodecsv/blob/master/unicodecsv/__init__.py   # noqa
    """

    def __init__(self, f, dialect=csv.excel, encoding='utf-8', *args, **kwds):
        self.encoding = encoding
        self.writer = csv.writer(f, dialect, *args, **kwds)

    def writerow(self, row):
        self.writer.writerow([s.encode("utf-8") if 'encode' in dir(s) else s for s in row])

    def row_to_str(self, row):
        return '\t'.join([s.encode("utf-8").replace(',',';') if 'encode' in dir(s) else s for s in row])

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


class CSVExporter(BaseExporter):
    short_name = 'TSV'
    long_name = 'Tab-Separated Values (TSV)'

    file_extension = 'tsv'
    content_type = 'text/tsv'

    preferred_formats = ('tsv', 'string')

    def generator(self, iterable, *args, **kwargs):
        header = []
        buff = self.get_file_obj(None)
        writer = UnicodeWriter(buff, quoting=csv.QUOTE_MINIMAL)

        for i, row_gen in enumerate(self.read(iterable, *args, **kwargs)):
            row = []

            for data in row_gen:
                if i == 0:
                    header.extend(data.keys())

                row.extend(data.values())

            if i == 0:
                yield writer.row_to_str(header) + '\n'

            yield writer.row_to_str([str(s) for s in row]) + '\n'

    def write(self, iterable, buff=None, *args, **kwargs):
        header = []
        buff = self.get_file_obj(buff)
        writer = UnicodeWriter(buff, quoting=csv.QUOTE_MINIMAL)

        for i, row_gen in enumerate(self.read(iterable, *args, **kwargs)):
            row = []

            for data in row_gen:
                if i == 0:
                    header.extend(data.keys())

                row.extend(data.values())

            if i == 0:
                writer.writerow(header)

            writer.writerow(row)

        return buff

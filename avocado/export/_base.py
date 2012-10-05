try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict
from cStringIO import StringIO
from avocado.formatters import registry as formatters


class BaseExporter(object):
    "Base class for all exporters"

    preferred_formats = []

    def __init__(self, concepts):
        self.concepts = concepts
        self.params = []

        for concept in concepts:
            cfields = concept.concept_fields.select_related('field')
            fields = [c.field for c in cfields]
            formatter = formatters[concept.formatter_name](concept)
            self.params.append((self._get_keys(fields), len(cfields), formatter))

    def get_file_obj(self, name):
        if name is None:
            return StringIO()
        if isinstance(name, basestring):
            return open(name, 'w+')
        return name

    def _format_row(self, row):
        for keys, length, formatter in self.params:
            part, row = row[:length], row[length:]
            values = OrderedDict(zip(keys, part))
            yield formatter(values, self.preferred_formats)

    def _get_keys(self, fields):
        # Best case scenario, no conflicts, return as is. Otherwise
        # duplicates found will be suffixed with a '_N' where N is the
        # occurred position.
        keys = [f.field_name for f in fields]

        if len(set(keys)) != len(fields):
            cnts = {}
            for i, key in enumerate(keys):
                if keys.count(key) > 1:
                    cnts.setdefault(key, 0)
                    keys[i] = '{}_{}'.format(key, cnts[key])
                    cnts[key] += 1
        return keys

    def read(self, iterable):
        "Takes an iterable that produces rows to be formatted."
        for row in iterable:
            yield self._format_row(row)

    def write(self, iterable, *args, **kwargs):
        raise NotImplemented

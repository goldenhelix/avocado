from avocado.models import DataConcept, DataView, DataField
from avocado.formatters import Formatter
from cStringIO import StringIO
from itertools import tee

def get_type_map(model_version_id, model_type):  
    if model_type=='sample':
        type_map = {'name':'String'}
    elif model_type=='project':
        type_map = {'chr':'String',	'start':'Integer',	'stop':'Integer', 'refalt':'String'}
    else:
        ref_alt = DataField.objects.filter(model_version_id=model_version_id, field_name='ref_alts')
        if ref_alt.exists():
            type_map = {'chr':'String',	'pos_start':'Integer',	'pos_stop':'Integer', 'ref_alts':'String'}
        else:
            type_map = {'chr':'String',	'pos_start':'Integer',	'pos_stop':'Integer'}

    results = DataField.objects.filter(model_version_id=model_version_id)
    for result in results:
        type_map[result.field_name] = result.type

    return type_map

def get_allowed_value_map(model_version_id):
    allowed_value_map = {f.field_name:f.allowed_values for f in DataField.objects.filter(model_version_id=model_version_id)}
    return allowed_value_map

def get_title_map(model_version_id):
    tmap = {f.field_name:f.name for f in DataField.objects.filter(model_version_id=model_version_id)}
    return tmap

class BaseExporter(object):
    "Base class for all exporters"
    file_extension = 'txt'
    content_type = 'text/plain'
    preferred_formats = ()

    def __init__(self, concepts=None):
        if concepts is None:
            concepts = ()
        elif isinstance(concepts, DataView):
            node = concepts.parse()
            concepts = node.get_concepts_for_select()

        self.type_map = {}
        self.allowed_value_map = {}
        self.title_map = {}
        self.params = []
        self.row_length = 0
        self.concepts = concepts

        for concept in concepts:
            self.add_formatter(concept)

    def __repr__(self):
        return u'<{0}: {1}/{2}>'.format(self.__class__.__name__,
                                        len(self.params), self.row_length)

    def add_formatter(self, formatter, length=None, index=None):
        if isinstance(formatter, DataConcept):
            length = formatter.concept_fields.count()
            formatter = formatter.format
        elif isinstance(formatter, Formatter):
            length = len(formatter.keys)
        elif length is None:
            raise ValueError('A length must be supplied with the to '
                             'denote how much of the row will be formatted.')

        params = (formatter, length)
        self.row_length += length

        if index is not None:
            self.params.insert(index, params)
        else:
            self.params.append(params)

    def get_file_obj(self, name=None):
        if name is None:
            return StringIO()
        if isinstance(name, basestring):
            return open(name, 'w+')
        return name

    def _format_row(self, row, **kwargs):
        
        for formatter, length in self.params:
            values, row = row[:length], row[length:]
            yield formatter(values, preferred_formats=self.preferred_formats,
                            **kwargs)

    def has_field(self, field_collection, field):
        for key in field:
            if key in field_collection:
                return True
        return False

    def format_row(self, row_gen, header):
        new_row = []
        added_fields = []
        
        for field in row_gen:
            field.pop('_id', None)
            if 'pos_start' in field: field['pos_start'] = str(int(field['pos_start']) + 1)
            elif 'start' in field: field['start'] = str(int(field['start']) + 1)
            if self.type_map and self.has_field(self.type_map, field) and not self.has_field(added_fields, field):
                for key in field:            
                    added_fields.append(key)
                    if field[key]=='null' or field[key]=='n/a': 
                        field[key] = None

                    if isinstance(field[key], basestring):
                        if field[key].startswith('[') and field[key].endswith(']'):
                            field[key] = str(field[key]).replace('[', '').replace(']', '').replace("'", '').replace(', ', ',')
                        field[key] = field[key].replace('\n', '').replace(';', '')

                    if field[key] and key in self.type_map:
                        if self.type_map[key].startswith('String') or self.type_map[key].startswith('Flag'):
                            field[key] = field[key].replace('"', '').strip()
                            if self.type_map[key]=='Flag': 
                                if field[key] in ['true', 'false']:
                                    field[key] = field[key].capitalize()
                                else:
                                    field[key] = None
                            elif key in self.allowed_value_map and self.allowed_value_map[key] and field[key] not in self.allowed_value_map[key]:
                                field[key] = None
                        elif self.type_map[key].startswith('Integer'):
                            try: 
                                field[key] = int(round(float(field[key])))
                            except ValueError:
                                field[key] = str(field[key]).replace('[', '').replace(']', '').replace("'", '').replace(', ', ',')
                                pass
                        elif self.type_map[key].startswith('Float'):
                            try:
                                field[key] =  float(field[key])
                            except ValueError:
                                field[key] = str(field[key]).replace('[', '').replace(']', '').replace("'", '').replace(', ', ',')
                                pass

                new_row.append(field)
                  
        return new_row

    def read(self, iterable, force_distinct=True, offset=None, limit=None,
             *args, **kwargs):
        """Takes an iterable that produces rows to be formatted.

        If `force_distinct` is true, rows will be filtered based on the slice
        of the row that is going to be formatted.

        If `offset` is defined, only rows that are produced after the offset
        index are returned.

        If `limit` is defined, once the limit has been reached (or the
        iterator is exhausted), the loop will exit.
        """
        
        if 'model_version_id' in kwargs:
            model_version_id = kwargs['model_version_id']
            model_type = kwargs['model_type']
            self.type_map = get_type_map(model_version_id, model_type)
            self.allowed_value_map = get_allowed_value_map(model_version_id)
            self.title_map = get_title_map(model_version_id)
        else:
            model_version_id = None

        emitted = 0
        unique_rows = set()
        header = []

        for i, row in enumerate(iterable):
            if limit is not None and emitted >= limit:
                break

            _row = row[:self.row_length]

            if force_distinct:
                
                newrow = []
                for i in range(0, len(_row)):
                    if type(_row[i]) is list:
                        newrow.append(str(_row[i]))
                    else:
                        newrow.append(_row[i])
                _row = tuple(newrow)
                    
                _row_hash = hash(tuple(_row))

                if _row_hash in unique_rows:
                    continue

                unique_rows.add(_row_hash)

            if offset is None or i >= offset:
                emitted += 1
                formatted_row = self._format_row(_row, **kwargs)
                formatted_row, row_gen = tee(formatted_row)
                if i==0:                             
                    for data in row_gen:
                        header.extend(data.keys())

                if model_version_id:
                    yield self.format_row(formatted_row, header)
                else:                
                    yield list(formatted_row)

    def write(self, iterable, *args, **kwargs):
        for row_gen in self.read(iterable, *args, **kwargs):
            row = []
            for data in row_gen:
                row.extend(data.values())
            yield tuple(row)

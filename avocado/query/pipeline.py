from django.utils.importlib import import_module
from modeltree.tree import trees
from avocado.formatters import RawFormatter
from avocado.conf import settings
import gc

QUERY_PROCESSOR_DEFAULT_ALIAS = 'default'

def queryset_iterator(sql, params, cursor, chunksize=500000):
    '''''
    Perform SQL query in chunks without holding query in memory
    '''

    # get the first chunk
    sql = sql.rstrip(';')
    chunked_sql = sql + ' LIMIT ' + str(chunksize)
    cursor.execute(chunked_sql, params)    
    rows = cursor.fetchall()

    chunk_count = 0
    while rows:
        for row in rows:
            yield row

        # get the next chunk
        chunk_count += 1
        offset = chunk_count*chunksize
        chunked_sql = sql + ' LIMIT ' + str(chunksize) + ' OFFSET ' + str(offset)
        cursor.execute(chunked_sql, params)
        rows = cursor.fetchall()

        gc.collect()

class QueryProcessor(object):
    """Prepares and builds a QuerySet for export.

    Overriding or extending these methods enable customizing the behavior
    pre/post-construction of the query.
    """
    def __init__(self, context=None, view=None, tree=None, include_pk=True):
        self.context = context
        self.view = view
        self.tree = tree
        self.include_pk = include_pk

    def get_queryset(self, queryset=None, **kwargs):
        "Returns a queryset based on the context and view."
        if self.context:
            queryset = self.context.apply(queryset=queryset, tree=self.tree)

        if self.view:
            queryset = self.view.apply(queryset=queryset, tree=self.tree,
                                       include_pk=self.include_pk)

        if queryset is None:
            queryset = trees[self.tree].get_queryset()
     
        return queryset

    def get_exporter(self, klass, **kwargs):
        "Returns an exporter prepared for the queryset."
        exporter = klass(self.view)

        if self.include_pk:
            pk_name = trees[self.tree].root_model._meta.pk.name
            exporter.add_formatter(RawFormatter(keys=[pk_name]), index=0)

        return exporter

    def get_iterable(self, offset=None, limit=None, queryset=None, **kwargs):
        "Returns an iterable that can be used by an exporter."
        if queryset is None:
            queryset = self.get_queryset(**kwargs)

        if offset is not None and limit is not None:
            queryset = queryset[offset:offset + limit]
        elif offset is not None:
            queryset = queryset[offset:]
        elif limit is not None:
            queryset = queryset[:limit]

        compiler = queryset.query.get_compiler(queryset.db)
        sql, params = compiler.as_sql()
        if not sql:
            return iter([])

        tables = queryset.query.tables
        if len(tables)>0 and tables[0].startswith('p_') and 'LIMIT' not in sql:
            return queryset_iterator(sql, params, compiler.connection.cursor())
        else:
            return compiler.results_iter()
        


class QueryProcessors(object):
    def __init__(self, processors):
        self.processors = processors
        self._processors = {}

    def __getitem__(self, key):
        return self._get(key)

    def __len__(self):
        return len(self._processors)

    def __nonzero__(self):
        return True

    def _get(self, key):
        # Import class if not cached
        if key not in self._processors:
            toks = self.processors[key].split('.')
            klass_name = toks.pop()
            path = '.'.join(toks)
            klass = getattr(import_module(path), klass_name)
            self._processors[key] = klass
        return self._processors[key]

    def __iter__(self):
        return iter(self.processors)

    @property
    def default(self):
        return self[QUERY_PROCESSOR_DEFAULT_ALIAS]


query_processors = QueryProcessors(settings.QUERY_PROCESSORS)

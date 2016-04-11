import csv
import vcf
from vcf.model import _Call, _Record, make_calldata_tuple
from _base import BaseExporter, get_type_map
from cStringIO import StringIO
from django.db import connection
import os
import uuid
import time
from avocado.models import DataView, DataContext;
from serrano.resources.base import prune_view_columns, get_alias_map
from avocado.query.pipeline import QueryProcessor, queryset_iterator
from django.db.backends.postgresql_psycopg2 import base
from django.conf import settings
from ceviche.utils import to_str, is_none


def get_path(relative_path):
    my_path = os.path.dirname(os.path.abspath(__file__))
    full_path = my_path + relative_path
    return full_path

def alias(data_type):
    if 'Bool' in data_type or 'Choice' in data_type:
        ceviche_type = 'String'
    else:
        ceviche_type = data_type.replace(' Array', '')

    if ceviche_type in ['Integer', 'Float', 'Flag', 'Character', 'String']:
        return ceviche_type
    else:
        return 'String'

def list_to_str(lst):
    if type(lst)==list:
        string = ', '.join([to_str(s) for s in lst])
    else:
        string = lst

    return string

def get_nested_parens(s):
    openp = []
    closep = []
    for i in range(0, len(s)):
        if s[i]=='(':
            openp.append({'closed':False, 'i':i})
            closep.append(-1)
        if s[i]==')':
            for j in xrange(len(openp)-1, 0-1, -1):
                if openp[j]['closed']==False:
                    closep[j] = i+1
                    openp[j]['closed'] = True
                    break
        
    groups = []
    for i, paren in enumerate(openp):
        closei = closep[i]
        groups.append(s[paren['i']:closei])

    return groups[::-1]


# This function is some string hackery to allow all sample to be included in the query
# even when a sample filter has been applied (TODO: Remove when new export is added)
def fix_sample_queries(sql):
    groups = get_nested_parens(sql)

    for i, group in enumerate(groups):
        if '.samples =' in group:
            clauses = group.split('AND')
            sample_clause = [c for c in clauses if '.samples =' in c][0]
            sample_clause = sample_clause.strip().replace('(', '').replace(')', '')
            inverse  = 'NOT ' + sample_clause
            to_replace = group
            replacement = ' '.join(['(', group, 'OR', inverse, ')'])
            sql = sql.replace(to_replace, replacement)

    return sql


class VCFWriter(object):
    """
    A VCF writer which will write rows to VCF file "f",
    which is encoded in the given encoding.

    Adapted from https://github.com/jdunck/python-unicodecsv/blob/master/unicodecsv/__init__.py   # noqa
    """

    def __init__(self, f, dialect=csv.excel, encoding='utf-8', *args, **kwds):
        self.encoding = encoding
        if f:
            self.file = f
        else:
            self.file = StringIO()

    def stream_response_generator(self):
        yield "<html><body>\n"
        for x in range(1,11):
            yield "<div>%s</div>\n" % x
        yield "</body></html>\n"


    def record_to_str(self, record):
        ffs = self.writer._map(str, [record.CHROM, record.POS, record.ID, record.REF]) \
              + [self.writer._format_alt(record.ALT), record.QUAL or '.', self.writer._format_filter(record.FILTER),
                 self.writer._format_info(record.INFO)]
        if record.FORMAT:
            ffs.append(record.FORMAT)

        samples = [self.writer._format_sample(record.FORMAT, sample)
            for sample in record.samples]
        rec_str = '\t'.join(ffs + samples)
        return rec_str

    def generate_file(self, request, selected_samples, format_fields, model_version, version):
        
        # pyvcy requires that gt is the first sample field
        format_fields = ['gt'] + [f for f in format_fields if not f.lower()=='gt']

        format_str = ':'.join([f.upper() for f in format_fields])

        primary_table = model_version['model_name']
        matrix_table = model_version['model_name'] + '_matrix'
        entity_table = model_version['model_name'] + '_entity'

        # get the current context
        contexts = list(DataContext.objects.filter(model_version_id=model_version['id'], user_id=request.user.id).order_by('modified'))
        if contexts:
            context = contexts[len(contexts)-1]
        else:
            context = DataContext()

        # get the complete sample list
        query = 'select "samples" from ' + entity_table + ';'
        cursor = connection.cursor()
        cursor.execute(query)
        sample_list =  [r[0] for r in cursor.fetchall()]

        views = list(DataView.objects.filter(model_version_id=model_version['id'], user_id=request.user.id).order_by('modified'))
        if views:
            view = views[len(views)-1]
            view = prune_view_columns(view, model_version['id'])

        # construct the query associated with the current view and filter
        processor = QueryProcessor(context=context, view=view, tree=model_version['model_name'])
        queryset = processor.get_queryset(request=request)

        # fix queries that include sample filters
        # this is a hack that will be removed when 
        # the export backend in replaced
        children = queryset.query.where.children
        for i, child in enumerate(children):
            sqls = []
            for sql in child.sqls:
                sqls.append(fix_sample_queries(sql))
            children[i].sqls = sqls
        queryset.query.where.children = children

            
        queryset.query.select.append((primary_table, 'start'))
        queryset.query.alias_map = get_alias_map(model_version['model_name'], queryset.query.alias_map) 
        matrix_join = (primary_table, matrix_table, '_id', '_id')
        queryset.query.join(matrix_join, promote=True)
        
    
        # get the vcf header and row indicies of relevant fields
        vcf_header = []
        relevant_idxs = []
        for i, select_item in enumerate(queryset.query.select):
            if not select_item[1] in ['_id', '_entity_id'] and not (type(select_item)==tuple and select_item[1] in ['_id', '_entity_id']):
                vcf_header.append(select_item[1])
                relevant_idxs.append(i)

        # add format fields to select statement
        queryset.query.select.append((matrix_table, '_entity_id'))
        for symbol in format_fields:
            select_item = (matrix_table, symbol)
            queryset.query.select.append(select_item)

        # execute the query
        compiler = queryset.query.get_compiler(using=queryset.db)
        sql, params = compiler.as_sql()
        #sql, params = base.prepare_query(sql, params, cursor)


        # prepare vcf writer
        self.init_writer(vcf_header, model_version['id'], model_version['model_type'], sample_names=selected_samples, format_fields=format_fields)
        template_lines = self.template_content(vcf_header, model_version['id'], model_version['model_type'], sample_names=selected_samples, format_fields=format_fields)
        for line in template_lines:
            yield line + '\n'

        sample_indexes = {s:i for i, s in enumerate(selected_samples)}
        sample_data = [None] * len(sample_indexes)

        tables = queryset.query.tables
        if len(tables)>0 and tables[0].startswith('p_') and 'LIMIT' not in sql:
            iterater = queryset_iterator(sql, params, compiler.connection.cursor())
        else:
            cursor.execute(sql, params)
            iterator = cursor.fetchall()

        for r in iterater:
            vcf_row = [r[i] for i in relevant_idxs]
            start = vcf_row[vcf_header.index('start')]

            # iterate over the format fields in the row
            entity_id = r[queryset.query.select.index((matrix_table, '_entity_id'))]
            entity_name = sample_list[entity_id]
            if entity_name in selected_samples:
                sample = {'name':entity_name, 'values':[]}
                for i, value in enumerate(r[relevant_idxs[-1]+2:]):
                    select_name = queryset.query.select[relevant_idxs[-1]+2+i]
                    if select_name not in ['_id', '_entity_id'] and not (type(select_name)==tuple and select_name[1] in ['_id', '_entity_id']):
                            sample['values'].append(value)

                sample_data[sample_indexes[entity_name]] = sample

            if all(x is not None for x in sample_data):
                if 'start' in vcf_header: vcf_row[vcf_header.index('start')] = int(vcf_row[vcf_header.index('start')]) + 1
                record = self.get_record(vcf_row, vcf_header, format_str=format_str, sample_indexes=sample_indexes, samples=sample_data)
                yield self.record_to_str(record) + '\n'
                sample_data = [None] * len(sample_indexes)


    # writes a vcf row to the files
    # samples is a list of dictionaries ordered based on sample_index
    #   Each dict is of the form {'name':sample_name, 'values':[list of values]}
    def get_record(self, row, header, format_str=None, sample_indexes={}, samples=[]):
        if 'chr' in header: chrom = row[header.index('chr')]
        else: chrom = None

        if 'pos_start' in header: pos = int(row[header.index('pos_start')])
        elif 'start' in header: pos = int(row[header.index('start')])
        else: pos = -1

        if 'ref_alts' in header: 
            ref = row[header.index('ref_alts')].split('/')[0]
            alts = row[header.index('ref_alts')].split('/')[1:]
        elif 'refalt' in header:
            ref = row[header.index('refalt')].split('/')[0]
            alts = row[header.index('refalt')].split('/')[1:]
        else: 
            ref = '.'
            alts = '.'

        info = {}
        for label in header:
            if label=='_id':
                continue
            if not label.lower() in ['chr', 'pos_start', 'pos_stop', 'ref_alts', 'start', 'one_based_start', 'stop', 'refalt']:
                if (row[header.index(label)] or row[header.index(label)]==0) and not row[header.index(label)]=='n/a' and label in self.type_map:
                    if isinstance(row[header.index(label)], basestring):
                        row[header.index(label)] = row[header.index(label)].replace(' ', '_')
                        if is_none(row[header.index(label)]):
                            row[header.index(label)] = '?'
                    elif type(row[header.index(label)])==list:
                        for i, v in enumerate(row[header.index(label)]):
                            if isinstance(v, basestring): 
                                row[header.index(label)][i] = v.replace(' ', '_')
                                if is_none(row[header.index(label)]):
                                    row[header.index(label)] = '?'

                    if self.type_map[label].startswith('String') or self.type_map[label].startswith('Choice'):
                        if type(row[header.index(label)])==list:
                            row[header.index(label)] = list_to_str(row[header.index(label)])
                        if not is_none(row[header.index(label)]):
                            info[label] = row[header.index(label)].replace('"', '')
                    elif self.type_map[label].startswith('Flag') or self.type_map[label].startswith('Boolean'):
                        if row[header.index(label)]=='true' or row[header.index(label)] is True:
                            info[label] = True
                    elif self.type_map[label].startswith('Integer'):
                        try: 
                            info[label] = int(round(float(row[header.index(label)])))
                        except (ValueError, TypeError):
                            info[label] = list_to_str(row[header.index(label)])
                    elif self.type_map[label].startswith('Float'):
                        try: 
                            info[label] = float(row[header.index(label)])
                        except (ValueError, TypeError):
                            info[label] = list_to_str(row[header.index(label)])
                            pass
                    else:
                        if type(row[header.index(label)])==list:
                            row[header.index(label)] = list_to_str(row[header.index(label)])
                        else:
                            info[label] = row[header.index(label)].replace('"', '')

        record = _Record(chrom, pos, '.', ref, alts, '.', '.', info, format_str,  sample_indexes)

        sample_calls = []
        for sample in samples:
            call_data = self.samp_fmt(*sample['values'])
            call = _Call(record, sample['name'], call_data)
            sample_calls.append(call)          

        record.samples = sample_calls
        return record

    def writerow(self, row, header, format_str='.', sample_indexes={}, samples=[]):
        record = self.get_record(row, header, format_str=format_str, sample_indexes=sample_indexes, samples=samples)
        self.writer.write_record(record)
        self.writer.flush()

    def writerows(self, rows, header):
        for row in rows:
            self.writerow(row, header)
    
    def get_date(self):
        m_d_y = time.strftime("%x").split('/')
        filedate = '20' + m_d_y[2] + m_d_y[0] + m_d_y[1]
        return filedate

    def get_source(self):
        return 'GRCh_37_g1k,Chromosome,Homo sapiens'

    def template_content(self, header, model_version_id, model_type, sample_names=[], format_fields=[]):
        self.samp_fmt = make_calldata_tuple([f.upper() for f in format_fields])
        template_lines = ["##fileformat=VCFv4.1", "##source=Ceviche", "##fileDate=" + self.get_date(), "##reference=" + self.get_source()]

        self.type_map = get_type_map(model_version_id, model_type)

        for label in header:
            if label=='_id':
                continue
            if not label.lower() in ['chr', 'pos_start', 'pos_stop', 'ref_alts', 'start', 'one_based_start', 'stop', 'refalt'] and label in self.type_map:
                if 'Array' in self.type_map[label]:
                    number = '.'
                else:
                    number = '1'
                newline = "##INFO=<ID=" + label + ",Number=" + number +",Type=" + alias(self.type_map[label]) + """,Description="">"""
                template_lines.append(newline)

        for field in format_fields:
            if 'Array' in self.type_map[field]:
                number = '.'
            else:
                number = '1'
            newline = "##FORMAT=<ID=" + field.upper() + ",Number=" + number +",Type=" + alias(self.type_map[field])+ """,Description="">"""
            template_lines.append(newline)

        template_lines.append("""##FILTER=<ID=LowQual,Description="Low quality">""")

        header = "#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO"
        if format_fields:
            header += '\tFORMAT'
        
        if sample_names:
            header += "\t" + "\t".join(sample_names)
        template_lines.append(header)
        return template_lines

    def init_writer(self, header, model_version_id, model_type, sample_names=[], format_fields=[]):
        template_lines = self.template_content(header, model_version_id, model_type, sample_names=sample_names, format_fields=format_fields)
        template_fname = settings.WAREHOUSE_PATH + "/tmp/template_" + str(uuid.uuid4()) + ".vcf"
        if not os.path.exists(settings.WAREHOUSE_PATH + "/tmp/"):
            os.makedirs(settings.WAREHOUSE_PATH + "/tmp/")
        f = open(template_fname, 'w')
        for line in template_lines:
            f.write(line + "\n")
        f.close()
        template = vcf.Reader(filename=template_fname)
        self.writer = vcf.Writer(self.file, template)


class VCFExporter(BaseExporter):
    short_name = 'VCF'
    long_name = 'Variant Call Format (VCF)'

    file_extension = 'vcf'
    content_type = 'text/vcf'

    preferred_formats = ('vcf', 'string')

    def generator(self, iterable, *args, **kwargs):
            model_version_id = kwargs['model_version_id']
            model_type = kwargs['model_type']
            header = []
            buff = self.get_file_obj(None)
            writer = VCFWriter(buff)

            for i, row_gen in enumerate(self.read(iterable, *args, **kwargs)):
                row = []

                for data in row_gen:
                    if i==0:
                        header.extend(data.keys())

                    row.extend(data.values())

                if i==0:
                    writer.init_writer(header, model_version_id, model_type)
                    template_lines = writer.template_content(header, model_version_id, model_type)
                    for line in template_lines:
                        yield line + '\n'

                record = writer.get_record(row, header)
                yield writer.record_to_str(record) + '\n'

    def write(self, iterable, buff=None, *args, **kwargs):
        model_version_id = kwargs['model_version_id']
        model_type = kwargs['model_type']
        header = []
        buff = self.get_file_obj(buff)
        writer = VCFWriter(buff)

        for i, row_gen in enumerate(self.read(iterable, *args, **kwargs)):
            row = []

            for data in row_gen:
                if i==0:
                    header.extend(data.keys())

                row.extend(data.values())

            if i==0:
                writer.init_writer(header, model_version_id, model_type)

            writer.writerow(row, header)

        return buff

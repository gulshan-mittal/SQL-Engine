#!/usr/bin/python3

import sys
import collections
import sqlparse
import prettytable
from .table import Table


class Select:
    
    def __init__(self, print_type):
        self.__cond_conjunctions = []
        self.__distinct_cols = []
        self.__join = False
        self.__tables = []
        self.__agg_functions = collections.OrderedDict()
        self.__query_table = Table("Query Table")
        self.__response_table = Table("Response Table")
        self.__error_messages_not_exist = {
            1146: "ERROR 1146 (42S02): Table doesn't exist",
            404: "ERROR 404 (1234): Data for table doesn't exist"
        }
        self.__join_cols = []
        self.__error_messages_syntax = {
            1064: "ERROR 1064 (42000): You have an error in your SQL syntax; check the manual for the right syntax to use.",
        }
        self.__cols = []
        self.__conds = []
        self.__error_messages_not_loaded = {
            500: "ERROR 500 (56789): Table couldn't be loaded"
        }
        self.print_type = print_type


    def raise_error_not_exist(self,code):
        error = self.__error_messages_not_exist[code]
        print(error)

    def parse(self, parsed_query):
        data = [[], [], []]
        incomplete_query = 1
        idx = 0
        token_list = parsed_query.tokens
        length = len(token_list)
        dict_binary = {'1': 1, '0': 0, '2': 2} 
        for itr in range(length):
            if (isinstance(token_list[itr], sqlparse.sql.Identifier) or isinstance(token_list[itr], sqlparse.sql.Function)):
                data[idx].append(token_list[itr])
            elif isinstance(token_list[itr], sqlparse.sql.IdentifierList):
                for cf in token_list[itr].get_identifiers():
                    data[idx].append(cf)
            elif str(token_list[itr]).upper() == 'FROM':
                idx += 1
            elif isinstance(token_list[itr], sqlparse.sql.Where):
                idx += 1
                for w in token_list[itr]:
                    if isinstance(w, sqlparse.sql.Comparison) == True:
                        data[idx].append(w)
                    elif (isinstance(w, sqlparse.sql.Token) == True and (str(w).upper() == 'AND' or str(w).upper() == 'OR')):
                        upper_str = str(w).upper()
                        self.__cond_conjunctions.append(upper_str)
                    elif (isinstance(w, sqlparse.sql.Token) == True) and str(w) == ';':
                        incomplete_query = 0
            elif (isinstance(token_list[itr], sqlparse.sql.Token) == True) and str(token_list[itr]) == ';':
                incomplete_query = 0
        if incomplete_query:
            self.raise_error_syntax(1064)
            return -1

        for _, identifier in enumerate(data[0]):
            if isinstance(identifier, sqlparse.sql.Identifier) == True:
                self.__cols.append(str(identifier))
            elif isinstance(identifier, sqlparse.sql.Function) == True:
                if str(identifier[0]).upper() == 'DISTINCT':
                    col = str(identifier[1])
                    self.__distinct_cols.append(col.lstrip('(').rstrip(')'))
                else :
                    self.__agg_functions[identifier] = dict_binary['0']
        self.__conds = data[dict_binary['2']]
        self.__tables = data[dict_binary['1']]
        return 0

    def cartesian_product(self, table):
        
        self.__query_table.clean_cols()
        length_query_table_fields = len(self.__query_table.get_fields()) 
        field_idx =  length_query_table_fields + 1
        
        for field in table.get_fields():
            string_int = 'INTEGER'
            self.__query_table.add_field(table.get_name() + "." + field, string_int, field_idx)
            field_idx = field_idx + 1

        if len(self.__query_table.get_rows()) == 0:
            get_rows_table = table.get_rows()
            self.__query_table.set_rows(get_rows_table)
        else :
            rows = []
            query_table_rows = self.__query_table.get_rows()
            for row1 in query_table_rows:
                get_rows_table = table.get_rows()
                for row2 in get_rows_table:
                    row = row1 + row2
                    rows.append(row)
            self.__query_table.set_rows(rows)

    def raise_error_not_loaded(self,code):
        error = self.__error_messages_not_loaded[code]
        print(error)
    
    def union(self, rows):
        get_response_table_rows = self.__response_table.get_rows() 
        rows_union = list(set(get_response_table_rows).union(set(rows)))
        self.__response_table.set_rows(rows_union)

    def intersection(self, rows):
        get_response_table_rows = self.__response_table.get_rows() 
        rows_intersection = list(set(get_response_table_rows).intersection(set(rows)))
        self.__response_table.set_rows(rows_intersection)
    

    def compare(self, lhs, rhs, op):
        sign_dict = {'lt' :'<', 'gt':'>', 'lteqt':'<=', 'gteqt': '>=', 'eqt': '=', 'ltgt': '<>'}
        if op == sign_dict['gt']:
            return lhs > rhs
        elif op == sign_dict['lt']:
            return lhs < rhs
        elif op == sign_dict['gteqt']:
            return lhs >= rhs
        elif op == sign_dict['lteqt']:
            return lhs <= rhs
        elif op == sign_dict['eqt']:
            return lhs == rhs
        elif op == sign_dict['ltgt']:
            return lhs != rhs

    def process_conditions(self, db):

        get_query_table_fields = self.__query_table.get_fields()
        self.__response_table.set_fields(get_query_table_fields)
        response_rows_list = []
        
        for _, cond in enumerate(self.__conds):
            
            data = []
            
            for _, x in enumerate(cond):
                if (isinstance(x, sqlparse.sql.Identifier) == True)  or (not x.is_whitespace and isinstance(x, sqlparse.sql.Token) == True):
                    string_x = str(x)
                    data.append(string_x)
            
            lhs, op, rhs = [x for x in data]
            if len(self.__tables) == 1:
                field_name = [x.strip() for x in lhs.split('.')]
                table_name_check1 = ''
                for i in range(len(field_name) - 1):
                    table_name_check1 = table_name_check1 + '.' + field_name[i]
                table_name_check1 = table_name_check1[1:]
                field_name_check_1 = field_name[-1]
                if len(field_name) > 1 and table_name_check1 == str(self.__tables[0]):
                    lhs = field_name_check_1

            if lhs not in self.__response_table.get_fields():
                print("ERROR 1054 (42S22): Unknown column " + lhs + " in 'field list'")
                return 'UNK'

            lhs_split = lhs.split()[0]
            rhs_split = rhs.split()[0] 
            if lhs_split != rhs_split and op == '=':
                self.__join = True
                pair_append = (lhs, rhs)
                self.__join_cols.append(pair_append)
            
            response_rows = []
            get_table_rows_query = self.__query_table.get_rows()
            for itr in range(len(get_table_rows_query)):
                try :
                    lhs_val = int(lhs)
                except ValueError:
                    row = get_table_rows_query[itr]
                    row_idx_lhs = self.__query_table.get_idx(lhs)
                    lhs_val = row[row_idx_lhs- 1]
                try :
                    rhs_val = int(rhs)
                except ValueError:
                    if rhs not in self.__response_table.get_fields():
                        print("ERROR 1054 (42S22): Unknown column " + rhs + " in 'field list'")
                        return 'UNK'
                    row = get_table_rows_query[itr]
                    row_idx_rhs = self.__query_table.get_idx(rhs)
                    rhs_val = row[row_idx_rhs - 1]
                if self.compare(lhs_val, rhs_val, op):
                    row = get_table_rows_query[itr]
                    response_rows.append(row)
            response_rows_list.append(response_rows)
        
        length_conjuction = len(self.__cond_conjunctions)
        if length_conjuction == 0:
            get_response_rowlist = response_rows_list[0]
            self.__response_table.set_rows(get_response_rowlist)
        else :
            get_response_rowlist = response_rows_list[0]
            self.union(get_response_rowlist)
            idx = 1
            cond_conjunctions = self.__cond_conjunctions 
            for conjunction in range(len(cond_conjunctions)):
                if cond_conjunctions[conjunction] == 'OR':
                    res_list_row = response_rows_list[idx]
                    self.union(res_list_row)
                elif cond_conjunctions[conjunction] == 'AND':
                    res_list_row = response_rows_list[idx]
                    self.intersection(res_list_row)
                idx = idx + 1

    def process_agg_functions(self, func, fields):
        
        result = 0
        field_name = [x.strip() for x in fields.split('.')]
        func_dict = {'max' : 'MAX', 'min': 'MIN', 'sum':'SUM', 'avg':'AVG'}
        table_name_check1 = ''
        for i in range(len(field_name) - 1):
            table_name_check1 = table_name_check1 + '.' + field_name[i]
        table_name_check1 = table_name_check1[1:]
        field_name_check_1 = field_name[-1]

        if len(field_name) == 1 and self.__response_table.has_field(field_name[0]):
            field = field_name[0]
        elif len(field_name) > 1 and table_name_check1 == str(self.__tables[0]) and self.__response_table.has_field(field_name_check_1):
            field = field_name_check_1
        else:
            print("ERROR 1054 (42S22): Unknown column " + fields + " in 'field list'")
            return None

        if func == func_dict['max']:
            result = -sys.maxsize - 1
            for _, col_val in enumerate(self.__response_table.get_col(field)):
                if self.chect_gt(col_val,result):
                    result = col_val
        elif func == func_dict['min']:
            result = sys.maxsize
            for _, col_val in enumerate(self.__response_table.get_col(field)):
                if self.chect_lt(col_val,result):
                    result = col_val
        elif func == func_dict['sum']:
            result = 0
            for _, col_val in enumerate(self.__response_table.get_col(field)):
                result = result + col_val
        elif func == func_dict['avg']:
            result = 0
            for _, col_val in enumerate(self.__response_table.get_col(field)):
                result = result + col_val
            length_response_table_cols = len(self.__response_table.get_col(field)) 
            result = result / length_response_table_cols
        return result

    def chect_lt(self, val1,val2):
        if val1 < val2:
            return True
        return False

    def chect_gt(self, val1,val2):
        if val1 > val2:
            return True
        return False

    def print_response(self):
        
        if self.print_type == 1:
            response_table = prettytable.PrettyTable()
        
        if len(self.__cols) > 0 or len(self.__distinct_cols) > 0:

            distinct_col_vals, row_col_idxs, distinct_col_idxs , response_fields = [], [], [], []
            
            distict_cols = self.__distinct_cols

            for field in range(len(distict_cols)):
                if (distict_cols[field] in self.__response_table.get_fields()) == True:
                    distinct_col_idxs.append(self.__response_table.get_field_val(distict_cols[field])['idx'])
                    row_col_idxs.append(self.__response_table.get_field_val(distict_cols[field])['idx'])
                    if (len(self.__tables) > 0) and (len(self.__tables) <  2):
                        response_fields.append(str(self.__tables[0]) + "." + distict_cols[field])
                    else :
                        response_fields.append(distict_cols[field])
                else :
                    print("ERROR 1054 (42S22): Unknown column " + distict_cols[field] + " in 'field list'")
                    return
            
            columns_data = self.__cols 
            for field in range(len(columns_data)):
                
                if len(self.__tables) == 1:
                    field_name_in_response_table = [x.strip() for x in columns_data[field].split('.')]
                    field_name_check = field_name_in_response_table[len(field_name_in_response_table) - 1]
                    table_name_check = ''
                    for i in range(len(field_name_in_response_table) - 1):
                        table_name_check = table_name_check + '.' + field_name_in_response_table[i]
                    table_name_check = table_name_check[1:]
                    if len(table_name_check) == 0 and self.__response_table.has_field(field_name_check):
                        field_name_print = field_name_check
                    elif len(table_name_check) > 0 and self.__response_table.has_field(field_name_check) and table_name_check == str(self.__tables[0]):
                        field_name_print = field_name_check
                    else:
                        field_name_print = columns_data[field]
                        print("ERROR 1054 (42S22): Unknown column " + field_name_print + " in 'field list'")
                        return
                else:
                    field_name_print = columns_data[field]
                    if (field_name_print in self.__response_table.get_fields()) == False:
                        length_table = len(self.__tables)
                        for itr in range(length_table):
                            temp_field_name = str(self.__tables[itr]) + '.' + field_name_print
                            if temp_field_name in self.__response_table.get_fields():
                                field_name_print = temp_field_name
                                break

                # print(field_name_print)
                if (field_name_print in self.__response_table.get_fields()) == True:
                    row_col_idxs.append(self.__response_table.get_field_val(field_name_print)['idx'])
                    
                    if (len(self.__tables) > 0) and (len(self.__tables) <  2):
                        name_of_table_append = str(self.__tables[0])
                        response_fields.append(name_of_table_append + "." + field_name_print)
                    else :
                        response_fields.append(field_name_print)
                else :
                    print("ERROR 1054 (42S22): Unknown column " + field_name_print + " in 'field list'")
                    return
            
            if self.print_type == 1:
                response_table.field_names = response_fields
            else:
                self.print_field(response_fields)
            
            for _, row in enumerate(self.__response_table.get_rows()):
                response_row = []
                distinct_col_tuple = []
                for _,i in enumerate(distinct_col_idxs):
                    tp_append = row[i-1]
                    distinct_col_tuple.append(tp_append)
                
                length_distinct_cols = len(self.__distinct_cols)
                if length_distinct_cols > 0:
                    if (tuple(distinct_col_tuple) in distinct_col_vals) == True:
                        continue
                    else :
                        tuple_append = tuple(distinct_col_tuple)
                        distinct_col_vals.append(tuple_append)
                
                for _, i in enumerate(row_col_idxs):
                    tp2_append = str(row[i-1])
                    response_row.append(tp2_append)

                if (self.print_type > 0) and (self.print_type < 2):
                    response_table.add_row(response_row)
                else:
                    self.print_rows(response_row)
        elif len(self.__agg_functions) > 0 :
            
            response_fields, response_row = [], []
            
            for _, agg_func in enumerate(self.__agg_functions):
                string_func = str(agg_func)
                response_fields.append(string_func)
                field = str(agg_func[1])
                self.__agg_functions[agg_func] = self.process_agg_functions(str(agg_func[0]).upper(), field.lstrip('(').rstrip(')'))
            
            for _, agg_func in enumerate(self.__agg_functions):
                if self.__agg_functions[agg_func] is None:
                    return 
                response_row.append(self.__agg_functions[agg_func])

            if self.print_type == 1:
                response_table.field_names = response_fields
                response_table.add_row(response_row)
            else:
                self.print_field(response_fields)
                self.print_rows(response_row)
       
        if self.print_type == 1:
            print(response_table)

    def __execute__(self, db):
        if len(self.__tables) == 0:
            self.raise_error_syntax(1064)
            return 
        elif len(self.__tables) == 1:
            if db.has_table(str(self.__tables[0])):
                self.__query_table = db.get_table(str(self.__tables[0]))
            else:
                print("ERROR 1052 (42S02): Unknown Table " + str(self.__tables[0]) + " in 'database'")
                self.__clean__()
                return
        else :
            for table in self.__tables:
                if not db.has_table(str(table)):
                    self.raise_error_not_exist(1146)
                    return
                else :
                    if db.has_table(str(table)):
                        self.cartesian_product(db.get_table(str(table)))
                    else:
                        print("ERROR 1052 (42S02): Unknown Table " + table + " in 'database'")
                        self.__clean__()
                        return

        # Process given conditions (if any) 
        if len(self.__conds) > 0:
            if self.process_conditions(db) == 'UNK':
                self.__clean__()
                return
        else :
            get_fields_query_table = self.__query_table.get_fields()
            get_rows_query_table = self.__query_table.get_rows()
            self.__response_table.set_fields(get_fields_query_table)
            self.__response_table.set_rows(get_rows_query_table)

        if len(self.__cols) == 0 and len(self.__agg_functions) == 0:
            get_fields_query_table_1 = self.__query_table.get_fields() 
            for _, field in enumerate(get_fields_query_table_1):
                self.__cols.append(field)
        
        if (self.__join and ((self.__join_cols[0][0] in self.__cols) == True) and ((self.__join_cols[0][1] in self.__cols) == True)):
            itm_to_rmv = self.__join_cols[0][1]
            self.__cols.remove(itm_to_rmv)
        
        self.print_response()
        self.__clean__()

    def raise_error_syntax(self,code):
        error = self.__error_messages_syntax[code]
        print(error)

    def execute(self, db, query):
        if self.parse(query) != -1:
            self.__execute__(db)

    def print_field(slef, fields):
        length = len(fields)
        for i in range(length-1):
            print(fields[i], end = ',')
        print(fields[length-1])

    def __clean__(self):
        self.__response_table = Table("Response Table")
        self.__cond_conjunctions, self.__conds, self.__tables, self.__distinct_cols, self.__cols = [], [], [], [], []
        self.__agg_functions = collections.OrderedDict()
        self.__query_table = Table("Query Table")

    def print_rows(self, arr):
        length = len(arr)
        for i in range(length - 1):
            print(arr[i],end = ',')
        print(arr[length-1])

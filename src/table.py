#!/usr/bin/python3


import collections

class Table:

    def __init__(self, name):
        self.__fields = collections.OrderedDict()
        self.__name = name
        self.__rows = []
        self.__cols = {}


    def get_idx(self, field):
        result =  {'res': self.__fields[field]['idx']}
        return result['res']


    def set_rows(self, rows):
        self.__rows = rows
        for _, row in enumerate(self.__rows):
            for itr, field in enumerate(self.__fields):
                self.__cols[field].append(row[itr])

    def add_row(self, row):
        self.__rows.append(row)

    def get_name(self):
        return self.__name

    def add_col_value(self, field, value):
        self.__cols[field].append(value)

    def get_fields(self):
        return self.__fields

    def clean_cols(self):
        for _, field in enumerate(self.__cols):
            self.__cols[field] = []

    def load(self):
        filename = "./files/" + self.__name + '.csv' 
        with open(filename) as file:
            lines = file.readlines()
            for _ , line in enumerate(lines):
                row = tuple([int(x.strip().strip('"')) for x in line.strip().split(',')])
                self.__rows.append(row)
                for i, field in enumerate(self.__fields):
                    self.__cols[field].append(row[i])

    def get_field_val(self, field):
        return self.__fields[field]

    def add_field(self, field, type, field_num):
        number_field = field_num
        dictionary  = {'dict' : {'type': type, 'idx': number_field}}
        self.__fields[field] = dictionary['dict']
        self.__cols[field] = []

    def get_col(self, field):
        result = {'res': self.__cols[field]}
        return result['res']

    def set_fields(self, fields):
        self.__fields = collections.OrderedDict()
        for _, field in enumerate(fields):
            self.__fields[field] = fields[field]
            self.__cols[field] = []

    def get_rows(self):
        result = {'res': self.__rows}
        return result['res']

    def has_field(self, field):
        return field in self.__fields

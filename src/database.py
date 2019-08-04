#!/usr/bin/python3

from .table import Table


class Database:
    def __init__(self, name, metadata_file):
        self.__error_messages = {
            '1146': "ERROR 1146 (42S02): Table doesn't exist",
            '404': "ERROR 404 (1234): Data for table doesn't exist",
            '500': "ERROR 500 (56789): Table couldn't be loaded"
        }
        self.__metadata_file = metadata_file
        self.__tables = {}
        self.__name = name
        self.load_metadata()
        self.load_tables()

    def load_tables(self):
        for _, table in enumerate(self.__tables):
            self.__tables[table].load()

    def load_metadata(self):
        context_dict = {'start': '<begin_table>', 'end': '<end_table>'}
        with open(self.__metadata_file) as file:
            content = [x.strip() for x in file.readlines()]
            i = 0
            length = len(content)
            while i <= (length - 1):
                if content[i] == context_dict['start']:
                    table = Table(content[i+1])
                    i = i + 2
                    field_idx = 1
                    while content[i] != context_dict['end']:
                        table.add_field(content[i], 'INTEGER', field_idx)
                        field_idx = field_idx + 1
                        i = i + 1
                    tb_name = table.get_name()
                    self.__tables[tb_name] = table
                i = i + 1
    
    def has_table(self, table):
        bool_check = table in self.__tables 
        return bool_check

    def get_table(self, table):
        result = {'res': self.__tables[table]}
        return result['res']

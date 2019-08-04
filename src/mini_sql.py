#!/usr/bin/python3

import sys
import sqlparse

from .database import Database
from .query import Query
from .select_query import Select


class Engine:
    def __init__(self, print_type):
        self.__db = Database("TEST_DB", "./files/metadata.txt")
        self.__types = {
            'SELECT': Select(print_type),
        }
        self.__history = []
        self.__queries = []
        self.__raw_input = ""

    def execute(self, raw_input = ""):
        
        length = len(raw_input) 
        if length:
            self.__raw_input = raw_input[0]
        self.__history.append(self.__raw_input)
        
        diction = {'1': True}
        while diction['1']:

            if not self.__raw_input:
                sql_name = "MiniSQL=> "
                self.__raw_input = input(sql_name)
            
            for _, raw_query in enumerate(sqlparse.split(self.__raw_input)):
                app_query = Query(raw_query)
                self.__queries.append(app_query)
            
            # Parse and execute each query 
            for _, query in enumerate(self.__queries):
                if query.parse():
                    typ = query.get_type()
                    parsed_query = query.get_parsed_query()
                    self.__types[typ].execute(self.__db, parsed_query)

            self.__clean__()

    def __clean__(self):
        self.__raw_input = ""
        self.__queries = []

#!/usr/bin/python3

import sqlparse


class Query:

    def __init__(self, raw_query):
        self.__supported_types = ['SELECT']
        self.__error_messages_bye = {
            0: "See you soon. Goodbye!"
        }
        self.__raw_query = raw_query
        self.__error_messages_select_query = {
            404: "Error 404: You have used a DDL/DML type in your SQL syntax, which is currently not supported by the MiniSQL engine."
        }
        self.__parsed_query = ""
        self.__error_messages_syntax = {
            1064: "ERROR 1064 (42000): You have an error in your SQL syntax; check the manual for the right syntax to use.",
        }

    def raise_msg_bye(self, code):
        print(self.__error_messages_bye[code])

    def get_type(self):
        return self.__parsed_query.get_type()

    def bool_support_query(self, query_type):
        return self.type_supported(query_type)

    def get_parsed_query(self):
        return self.__parsed_query


    def type_supported(self, query_type):
        if query_type not in self.__supported_types:
            self.raise_msg_selct_query_syntax(404)
            return False
        return True

    def parse(self):
        parsed_query = sqlparse.parse(self.__raw_query)[0]
        if self.bool_check_query(parsed_query):
            if self.bool_support_query(parsed_query.get_type()):
                self.__parsed_query = parsed_query
                return True
        return False

    def bool_check_query(self, parsed_query):
        return self.type_verify(parsed_query)

    def raise_msg_syntax(self, code):
        print(self.__error_messages_syntax[code])

    def raise_msg_selct_query_syntax(self, code):
        print(self.__error_messages_select_query[code])


    def type_verify(self, parsed_query):
        ukwn = u'UNKNOWN'
        if parsed_query.get_type() == ukwn:
            if str(parsed_query.tokens[0]).upper() == 'EXIT' or str(parsed_query.tokens[0]).upper() == 'QUIT':
                self.raise_msg_bye(0)
                exit(0)
            else :
                self.raise_msg_syntax(1064)
                return False
        return True

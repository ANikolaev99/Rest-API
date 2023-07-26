# coding: utf-8

from datetime import datetime
from datetime import date
import re

NAME_CLASS = "Work_any_class"

class Work_any_class(object):
    def __init__(self, obj_db = None):
        self.obj_db = obj_db

    def convert_str_tuple(self, data_str, table_name):
        # преобразует строку, подготовленную для записи в БД 
        # в кортеж, который получается при запросе строки из БД
        #data = [str(item) for item in data_str.split(',')]
        data = [item for item in data_str.split('|')]
        new_data = []
        columns = self.obj_db.get_column_list(table_name)
        if len(columns) != 0:
            for value_column, column in zip(data, columns):
                if value_column == 'NULL':
                    new_data.append(None)
                else:
                    if column[2] == 'integer':
                        new_data.append(int(value_column)) 
                    elif column[2] == 'real':
                        new_data.append(float(value_column))
                    elif column[2] == 'character varying':
                        ansi_escape = re.compile(r'(\x9B|\x1B\[)[0-?]*[ -\/]*[@-~]')
                        value_column_correct = ansi_escape.sub('',value_column)
                        value_column_correct_1 = value_column_correct.replace("\"", '')
                        new_data.append(value_column_correct_1)
                    elif column[2] == 'date':
                        new_data.append(str(value_column))
                    elif column[2] == 'timestamp without time zone' or column[2] == 'timestamp':
                        new_data.append(str(value_column))
                    else:
                        new_data.append('')
        return tuple(new_data)

    @staticmethod
    def control_empty_val(value):
        try:
            if value is None or value == '' or len(value) == 0:
                return False
            else:
                return True
        except Exception as exc:
            if isinstance(value, int) or isinstance(value, float) or isinstance(value, bool):
                return value
            else:
                return False

    @staticmethod
    def len_c(obj):
        try:
            size = len(obj)
            return size
        except Exception as exs:
            return 0

    @staticmethod
    def check_key(dictionary, key):
        if key in dictionary:
            return True
        else:
            raise KeyError("Not key '{}' in dict".format(key))

    @staticmethod
    def control_data(data):
        if isinstance(data, dict):
            return True
        else:
            raise TypeError("Data is not dict")

    @staticmethod
    def search_key(my_dict, my_key):
        if my_key in my_dict:
            return True
    
        for value in my_dict.values():
            if isinstance(value, dict):
                if Work_any_class.search_key(value, my_key):
                    return True

        return False

    @staticmethod
    def get_value(my_dict, my_key):
        if my_key in my_dict:
            return my_dict[my_key]
    
        for value in my_dict.values():
            if isinstance(value, dict):
                if Work_any_class.get_value(value, my_key):
                    return value[my_key]

        return False

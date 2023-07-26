# coding: utf-8

from datetime import date
from Print_msg import Print_msg

NAME_CLASS = "Work_db"

class Work_db(object):
    def __init__(self, conn, cursor):
        self.conn = conn
        self.cursor = cursor
        self.obj_print = Print_msg(0)

    def check_table_is_exists(self, table_name):
        # проверка существования таблицы
        sql = ("""SELECT EXISTS(
                    SELECT *
                    FROM information_schema.tables
                    WHERE
                      table_schema = 'public' AND
                      table_name = '%s'
              );""" % (table_name))
        self.cursor.execute(sql)
        result = self.cursor.fetchone()
        return(result[0])

    def table_management(self, sql):
        # операция DDL таблицей
        self.cursor.execute(sql)
        self.conn.commit()

    def get_max_value_column(self, table_name, column_name, column_where_name='', value_where_name='',
                             type_value_where='str'):
        # получить максимальное значение в колонке
        if column_where_name == '' or value_where_name == '': 
            sql = '''SELECT MAX("{}") FROM {} LIMIT 1'''.format(column_name, table_name)

        if column_where_name != '' and value_where_name != '' and type_value_where == 'str': 
            sql = '''SELECT MAX("{}") FROM {} 
                     WHERE "{}" = '{}' 
                     LIMIT 1'''.format(column_name, table_name, column_where_name, value_where_name)

        if column_where_name != '' and value_where_name != '' and type_value_where != 'str': 
            sql = '''SELECT MAX("{}") FROM {} 
                     WHERE "{}" = {} 
                     LIMIT 1'''.format(column_name, table_name, column_where_name, value_where_name)

        self.cursor.execute(sql)
        result = self.cursor.fetchone()
        if result is None or result[0] is None:
            return 0
        else:
            return(result[0])

    def get_min_value_column(self, table_name, column_name, column_where_name='', value_where_name='',
                             type_value_where='str'):
        # получить минимальное значение в колонке
        if column_where_name == '' or value_where_name == '': 
            sql = '''SELECT MIN("{}") FROM {} LIMIT 1'''.format(column_name, table_name)

        if column_where_name != '' and value_where_name != '' and type_value_where == 'str': 
            sql = '''SELECT MIN("{}") FROM {} 
                     WHERE "{}" = '{}' 
                     LIMIT 1'''.format(column_name, table_name, column_where_name, value_where_name)

        if column_where_name != '' and value_where_name != '' and type_value_where != 'str': 
            sql = '''SELECT MIN("{}") FROM {} 
                     WHERE "{}" = {} 
                     LIMIT 1'''.format(column_name, table_name, column_where_name, value_where_name)

        self.cursor.execute(sql)
        result = self.cursor.fetchone()
        if result is None or result[0] is None:
            return 0
        else:
            return(result[0])

    def find_value_column(self, table_name, column_name, value, type_value='str'):
        # поиск значения в колонке
        if type_value == 'str': 
            sql = '''SELECT "{}" FROM {} WHERE "{}" = '{}' '''.format(column_name, table_name, column_name, value)

        if type_value != 'str': 
            sql = '''SELECT "{}" FROM {} WHERE "{}" = {} '''.format(column_name, table_name, column_name, value)

        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        return len(result), result

    def select_simple(self, table_name, column_name, column_where_name, value_where_name,
                      type_value_where='str'):
        name_func = '.select_simple'
        # простой запрос
        if column_name != '*':
            if type_value_where == 'str': 
                sql = '''SELECT "{}" FROM {} 
                         WHERE "{}" = '{}' '''.format(column_name, table_name, column_where_name, value_where_name)

            if type_value_where != 'str': 
                sql = '''SELECT "{}" FROM {} 
                         WHERE "{}" = {}'''.format(column_name, table_name, column_where_name, value_where_name)
        else:
            if type_value_where == 'str': 
                sql = '''SELECT {} FROM {} 
                         WHERE "{}" = '{}' '''.format(column_name, table_name, column_where_name, value_where_name)

            if type_value_where != 'str': 
                sql = '''SELECT {} FROM {} 
                         WHERE "{}" = {}'''.format(column_name, table_name, column_where_name, value_where_name)

        try:
            self.cursor.execute(sql)
            result = self.cursor.fetchall()
            return len(result), result
        except Exception as exc:
            msg = "{} error in SQL query {} {}".format(NAME_CLASS + name_func, sql, str(exc))
            self.obj_print.out_msg_error(msg)
            return 0, None

    def get_value_cell(self, table_name, column_name_value, column_condition, value_condition, type_value_condtition='str'):
        # получить значение из ячейки [column_where_name + value_where_name][column_name]
        if type_value_condtition == 'str': 
            sql = '''SELECT "{}" FROM {} WHERE "{}" = '{}' LIMIT 1'''.format(column_name_value, table_name, column_condition, 
                                                                      value_condition)

        if type_value_condtition != 'str': 
            sql = '''SELECT "{}" FROM {} WHERE "{}" = {} LIMIT 1'''.format(column_name_value, table_name, column_condition, 
                                                                    value_condition)

        self.cursor.execute(sql)
        result = self.cursor.fetchone()
        if result is None or result[0] is None:
            return 0
        else:
            return(result[0])

    def set_value_cell(self, table_name, column_name_update, new_value, column_condition, value_condition, 
                       type_new_value='str', type_value_condtition='str'):
        # обновить значение ячейки
        if type_new_value == 'str' and type_value_condtition == 'str':
            sql = '''UPDATE {} SET "{}" = '{}' WHERE "{}" = '{}' '''.format(table_name, column_name_update, new_value, 
                                                                            column_condition, value_condition)
        if type_new_value != 'str' and type_value_condtition == 'str':
            sql = '''UPDATE {} SET "{}" = {} WHERE "{}" = '{}' '''.format(table_name, column_name_update, new_value, 
                                                                            column_condition, value_condition)
        if type_new_value == 'str' and type_value_condtition != 'str':
            sql = '''UPDATE {} SET "{}" = '{}' WHERE "{}" = {} '''.format(table_name, column_name_update, new_value, 
                                                                            column_condition, value_condition)
        if type_new_value != 'str' and type_value_condtition != 'str':
            sql = '''UPDATE {} SET "{}" = {} WHERE "{}" = {} '''.format(table_name, column_name_update, new_value, 
                                                                            column_condition, value_condition)
        self.cursor.execute(sql)
        self.conn.commit()

    def get_column_list(self, table_name):
        # получить список столбцов таблицы вида:
        # <class 'list'> [(1, 'f_id', 'integer', "nextval('_factories_f_id_seq'::regclass)"), 
        # (2, 'name_eng', 'character varying', None)
        sql = """
        SELECT
            ordinal_position,
            column_name,            
            data_type,
            column_default
        FROM
            information_schema.columns
        WHERE
            table_name = '{}'
        ORDER BY ordinal_position
        """.format(table_name)
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        return result

    def get_current_time(self):
        sql = """SELECT NOW()"""
        self.cursor.execute(sql)
        result = self.cursor.fetchone()
        if result is None or result[0] is None:
            return 0
        else:
            return(result[0])

    def insert_row_into_table(self, table_name, row, write='yes'):
        # вставка строки в таблицу. строка row в виде словаря {'name_column': value}
        # по умолчанию запись данных в таблицу write='yes'
        # или возврат подготовленных данных
        name_func = ".insert_row_into_table"
        def current_id(name_column):
            num_id = 0
            sql = """SELECT MAX("{}") FROM {} LIMIT 1""".format(name_column, table_name)
            self.cursor.execute(sql)
            result = self.cursor.fetchone()
            if result is None or result[0] is None:
                return 0
            else:
                return(result[0])  # int

        def get_delimeter(pos, size):
            if pos == size-1:
                return ''
            else:
                if write=='yes':
                    return ','
                else:
                    return '|'

        def get_default_value(column):
            if column[3] is None and column[2] == 'character varying':
                return 'NULL'
            if column[3] is None and column[2] == 'integer':
                return str(0) 
            if column[3] is None and column[2] == 'real':
                return str(0)
            if column[3] is None and column[2] == 'date':
                return '\'' + str(date.today()) + '\'' 
            if column[3] is None and (
                column[2] == 'timestamp without time zone' or column[2] == 'timestamp'):
                return '\'' + str(self.get_current_time()) + '\''  

        # получим список столбцов
        columns = self.get_column_list(table_name)
        size = len(columns)
        data = ''  # строка для insert
        for i, column in enumerate(columns):
            # проверка на столбец с id
            if column[1].find('id') != -1 and column[2] == 'integer':
                cur_id = 0
                if write == 'yes':
                    # получим текущий id
                    cur_id = current_id(column[1])
                data += str(cur_id + 1) + get_delimeter(i, size)
            else:
                if column[1] in row:
                    # есть данные для записи в этот столбец
                    try:
                        value_column = row[column[1]]
                        if write == 'yes':
                            data += '\'' + str(value_column) + '\'' + get_delimeter(i, size)
                        else:
                            data += str(value_column) + get_delimeter(i, size)
                    except Exception as exc:
                        if write == 'yes':
                            data += '\'' + 'error_data' + '\'' + get_delimeter(i, size)
                        else:
                            data += 'error_data' + get_delimeter(i, size)
                        msg = "{} data error, column {} {}".format(NAME_CLASS + name_func, column[1], 
                                                                       str(exc))
                        self.obj_print.out_msg_error(msg)
                else:
                    # нет данных для записи в столбец, добавляем дефолтное значение                    
                    try:
                        data += get_default_value(column) + get_delimeter(i, size)
                    except Exception as exc:
                        data += '' + get_delimeter(i, size)
                        msg = "{} error in default value, column {} {}".format(NAME_CLASS + name_func, column[1], 
                                                                       str(exc))
                        self.obj_print.out_msg_error(msg)
        # вставка строки в таблицу
        if write == 'yes':
            #sql = """INSERT INTO {} VALUES ({})""".format(table_name, data)
            #self.cursor.execute(sql)
            #self.conn.commit()
            return data
        else:
            return data

    def insert_row_into_table_2(self, table_name, data):
        # вставка готовой строки в таблицу
        sql = """INSERT INTO {} VALUES ({})""".format(table_name, data)
        self.cursor.execute(sql)
        self.conn.commit()

    def control_null_field(self, table_name, column_condition, value_condition, field_lst):
        # проверка на пустые поля NULL в таблице. список полей в field_lst
        name_func = '.control_null_field'
        sql = 'SELECT '
        try:
            for field in field_lst:
                sql += '\"' + field + '\"' + ','
            sql = sql[:-1]    
            sql += ' FROM ' + table_name + ' WHERE ' + '\"' + column_condition + '\"' + ' = ' + '\'' + value_condition + '\'' + ' AND '
            for i, field in enumerate(field_lst):
                if i <= len(field_lst)-2:
                    sql += '\"' + field + '\"' + ' IS NOT NULL AND '
                else:
                    sql += '\"' + field + '\"' + ' IS NOT NULL '        
            self.cursor.execute(sql)
            result = self.cursor.fetchall()
            return len(result), result
        except Exception as exc:
            msg = "{} error {} forming sql query {}".format(NAME_CLASS + name_func, str(exc), sql)
            self.obj_print.out_msg_error(msg)
            return 1, []

    def delete_row(self, table_name, column_condition, value_condition, type_value_condtition='str'):
        name_func = '.delete_row'
        # удаляет строку из таблицы
        if len(column_condition) > 0:
            if type_value_condtition == 'str' and len(value_condition) > 0:
                sql = """DELETE FROM {} WHERE "{}" = '{}' """.format(table_name, column_condition, value_condition)
                self.cursor.execute(sql)
                self.conn.commit()
                return True
            elif type_value_condtition != 'str' and value_condition != 0:
                sql = """DELETE FROM {} WHERE "{}" = {} """.format(table_name, column_condition, value_condition)
                self.cursor.execute(sql)
                self.conn.commit()
                return True
            else:
                msg = "{} error. There is no condition for selecting a row for deletion ".format(NAME_CLASS + name_func)
                self.obj_print.out_msg_error(msg)
                msg = "{} type_value_condtition {}, value_condition {} ".format(NAME_CLASS + name_func, 
                                                                                type_value_condtition,
                                                                                str(value_condition))
                self.obj_print.out_msg_info(msg)
                return False
        else:
            msg = "{} error. No column specified for row selection condition for deletion".format(
                                                                                       NAME_CLASS + name_func)
            self.obj_print.out_msg_error(msg)
            msg = "{} column_condition {} ".format(NAME_CLASS + name_func, column_condition)
            self.obj_print.out_msg_info(msg)
            return False
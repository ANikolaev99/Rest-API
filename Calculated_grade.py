# coding: utf-8

import os
import sys
import argparse
import configparser
import json
from json import JSONDecodeError
import jmespath
import re
import psycopg2
import chardet
import shared
from enum import Enum
from Work_db import Work_db
from Work_api_code import Work_api_code
from Work_grade import Work_grade
from Work_any_class import Work_any_class as wac
from Work_translator import Work_translator
from Print_msg import Print_msg
from datetime import datetime
from Send_message import SendMessage as sms

class Main():
    def __init__(self, dbname, user, password, host, message=0):
        self.dbname = dbname  # доступ к бд
        self.user = user
        self.password = password # на сервере в KZ изменить пароль  
        self.host = host
        # управление логированием: not_print = 0, console = 1, win_interface = 2, console_interface = 3
        self.message = message  # обьект поле вывода интерфейса
        self.obj_print = Print_msg(self.message)
        self.name_eng = ""
        self.name_ch = ""
        self.credit_code = ""
        self.name_class = 'Main'
        self.path_db = r'C:\Program Files\PostgreSQL\11'
        self.path_json_file = r'C:\Users\sasha\PycharmProjects\pythonProject'
        self.name_json_file = 'json.txt'  # файл с json для расчета оценки
        self.name_json_file_decode = 'json_decode.txt'  # файл с оценкой и комментариями по итогу обработки json

        # подключение к БД
        self.conn = 0
        try:
            self.conn = psycopg2.connect(dbname=self.dbname, user=self.user, password=self.password, host=self.host)
            self.conn.autocommit = True
        except Exception as exc:
            msg = "{} Error {}".format(self.name_class, str(exc))
            self.obj_print.out_msg_error(msg) 
            return

        self.cursor = self.conn.cursor()
        self.obj_db = Work_db(self.conn, self.cursor)

        self.name_func = ''

        # таблицы для работы
        self.table_name = ['_system_log', '_correct_translator', '_factories', '_factory_reports', '_factory_risk',
                           '_factory_court_cases', '_factory_beneficial', '_factory_news', '_factories_short']
        self.table_properties = {
            '_system_log': """CREATE TABLE IF NOT EXISTS _system_log (Date_event timestamp, Name_process varchar, 
                            Comment varchar, Code_finish smallint)""",
            '_correct_translator': """CREATE TABLE IF NOT EXISTS _correct_translator (перевод varchar, перевод_коррект varchar)""",
            '_factories': """CREATE TABLE IF NOT EXISTS _factories (f_id serial, Name_eng varchar, Name_ch varchar,
                            Оценка int, Плюсы varchar, Риски_высокие varchar, Риски_средние varchar, Риски_низкие varchar,
                            Предупреждения varchar,
                            CreditCode varchar, Директор varchar, Статус varchar, Дата_рег date, Тип_собств varchar,
                            Надзор varchar, Персонал varchar, Индустрия varchar, Подиндустрия varchar, Категория varchar,
                            Адрес varchar, Телефон varchar, Email varchar, WebSite varchar, Род_деят varchar, Главк varchar,
                            IsOnStock varchar, StockType varchar, StockNum varchar, Date_rec timestamp, update_grade int
                            )""",
            '_factory_reports': """CREATE TABLE IF NOT EXISTS _factory_reports (f_id serial, Name_eng varchar, Name_ch varchar,
                            CreditCode varchar, Email varchar, Итого_активы real, Капитал_владельца real, Доход
                            real, Прибыль real, Доход_осн_бизнес real, Чистая_прибыль real, Сумма_по_налогам real,
                            Обязательства real, Кредиты real, Господдержка real, Иностранные_инвесторы varchar, 
                            Ино_вклад_капитал varchar, Ино_доля_капитал varchar, Акционеры varchar, Вклад_капитал varchar,
                            Руководство varchar, Должности varchar, Долг_накоп_страх real, Долг_безраб_страх real,
                            Долг_мед_страх real, Долг_компенс_работникам real, Дата_отчета timestamp, Date_rec timestamp)""",
            '_factory_risk': """CREATE TABLE IF NOT EXISTS _factory_risk (f_id serial, Name_eng varchar, Name_ch varchar,
                            CreditCode varchar, Email varchar, Штрафы varchar, Описание varchar, Даты varchar, Уст_капитал real,
                            Date_rec timestamp)""",
            '_factory_court_cases': """CREATE TABLE IF NOT EXISTS _factory_court_cases (f_id serial, Name_eng varchar, Name_ch varchar,
                            Всего_дел real, Как_ответчик real, Как_истец real, Причины_судов varchar,
                            Date_rec timestamp)""",
            '_factory_beneficial': """CREATE TABLE IF NOT EXISTS _factory_beneficial (f_id serial, Name_eng varchar, Name_ch varchar,
                            Бенефициары varchar, Примечание varchar, Бенефициар varchar, Доля_бенефициара varchar, Тип_владения varchar,
                            Позиция varchar, Date_rec timestamp)""",
            '_factory_news': """CREATE TABLE IF NOT EXISTS _factory_news (f_id serial, Name_eng varchar, Name_ch varchar,
                            CreditCode varchar, нейтральные real, позитивные real, негативные real,
                            процент_положительных real)""",
            '_factories_short': """CREATE TABLE IF NOT EXISTS _factories_short (f_id serial, Name_eng varchar, Name_ch varchar,
                            CreditCode varchar, Директор varchar, Статус varchar, Дата_рег date, Тип_собств varchar,
                            Надзор varchar, Адрес varchar, Род_деят varchar, Уст_капитал real,
                            IsOnStock varchar, StockType varchar, StockNum varchar, Date_rec timestamp
                            )"""
        }
        self.name_main_table_api_code = {
            'all': '_factories',  # для всех запросов
            '430': '_factories',
            '213': '_factory_reports',
            '736': '_factory_risk',
            '930': '_factory_court_cases',
            '1003': '_factory_beneficial',
            '701': '_factory_news',
            '410': '_factories_short'
        }
        # названия файлов для загрузки таблиц
        self.table_files = {
            '_system_log': '',
            '_correct_translator': '_correct_translator.csv',
            '_factories': '',
            '_factory_reports': '',
            '_factory_risk': '',
            '_factory_court_cases': ''
        }
        # номера api кодов для обработки
        self.right_api_code = ['430', '213', '736', '930', '1003', '701', '410']
        # номера api кодов которые не надо обрабатывать
        self.not_right_api_code = ['886']

    def set_right_api_code(self, new_api_code):
        self.right_api_code = new_api_code

    def control_china_language(self, s):
        # проверка на китайские иероглифы в строке
        if s is None or not isinstance(s, str):
            return False
        else:
            chinese_pattern = re.compile(r'[\u4e00-\u9fff]+')
            return bool(chinese_pattern.search(s))

    def check_input(self, input):
        # проверка на credit code в строке поиска
        if isinstance(input, str) and input is not None:
            if ' ' not in input:
                if input.isnumeric() or input.isalnum():
                    return True
        return False

    def get_name_from_search_text(self, search_text):
        if isinstance(search_text, str) and search_text is not None and search_text != '':
            # если в строке есть иероглифы
            if self.control_china_language(search_text):
                self.name_ch = search_text
                obj_translator = Work_translator(self.obj_db)
                self.name_eng = obj_translator.translator_eng(search_text)
                self.credit_code = ''
            else:
                # если в строке сервис код
                if self.check_input(search_text):
                    self.name_ch = ''
                    self.name_eng = ''
                    self.credit_code = search_text
                else:
                    self.name_ch = ''
                    self.name_eng = search_text
                    self.credit_code = ''


    def decrypt_json_and_grade(self, api_code, log_data, search_text):
        name_func = '.decrypt_json_and_grade'
        # разобрать log_data и рассчитать оценку предприятия
        if isinstance(log_data, str):
            # удаление двойных кавычек и экранирования из строки
            json_line_s = re.sub('"{', '{', log_data)
            json_line_s_1 = re.sub('}"', '}', json_line_s)
            json_line_s_2 = json_line_s_1.replace('\\', "")
            log_data = json_line_s_2

        #msg = "{} api_code {}, log_data {}".format(self.name_class + name_func, api_code, 
        #                                           str(log_data).encode('utf-8'))
        #self.obj_print.out_msg_info(msg)
        #msg = "{} api_code {}, search_text {}".format(self.name_class + name_func, api_code, 
        #                                           str(search_text).encode('utf-8'))
        #self.obj_print.out_msg_info(msg)

        obj_api = API(self)
        parsing_result = obj_api.decrypt_json(api_code, log_data)
        # flag_command_line = False
        if parsing_result != 'error':
            # получен словарь {'name_eng': 'State Grid Corporation of China', 'name_ch': ...}

            # сохраним имя предприятия
            if 'name_eng' in parsing_result:
                if parsing_result['name_eng'] != "" and parsing_result['name_eng'] is not None:
                    self.name_eng = parsing_result['name_eng']
            if 'name_ch' in parsing_result:
                if parsing_result['name_ch'] != "" and parsing_result['name_ch'] is not None:
                    self.name_ch = parsing_result['name_ch']

            # если имя предприятия неизвестно, попробуем получить его из search_text
            if 'name_eng' in parsing_result and 'name_ch' in parsing_result:
                if parsing_result['name_eng'] == "" and parsing_result['name_ch'] == "":
                    self.get_name_from_search_text(search_text)
                    parsing_result['name_eng'] = self.name_eng
                    parsing_result['name_ch'] = self.name_ch
                    if 'CreditCode' in parsing_result:
                        parsing_result['CreditCode'] = self.credit_code
                    #msg = "{} self.name_eng {}, self.name_ch {}, self.credit_code {}".format(
                    #    self.name_class + name_func, self.name_eng, self.name_ch, self.credit_code)
                    #obj_print.out_msg_info(msg)
            else:
                if 'name_eng' not in parsing_result:
                    msg = "{} no key name_eng in parsing_result for api_code {}".format(
                        self.name_class + name_func, api_code)
                    obj_print.out_msg_error(msg)
                if 'name_ch' not in parsing_result:
                    msg = "{} no key name_ch in parsing_result for api_code {}".format(
                        self.name_class + name_func, api_code)
                    obj_print.out_msg_error(msg)

            debug_api_code = '410'
            if api_code == debug_api_code:
                msg = "{} parsing_result_{} {}".format(self.name_class + name_func, debug_api_code,
                                                       str(parsing_result).encode('utf-8'))
                obj_print.out_msg_info(msg)

            # если таблицы нет, создадим ее
            if not self.obj_db.check_table_is_exists(self.name_main_table_api_code[api_code]):
                obj_api.control_and_create_any_table(self.name_main_table_api_code[api_code])

            # получим строку подготовленную для записи в таблицу
            row = self.obj_db.insert_row_into_table(self.name_main_table_api_code[api_code], parsing_result)

            # проверяем есть ли предприятие в соответствующей таблице по имени
            if len(self.name_eng) != 0 and self.name_eng != "":
                len_result, result = self.obj_db.find_value_column(self.name_main_table_api_code[api_code], 
                                                                   'name_eng', self.name_eng)
                if len_result == 0:
                    # предприятия в таблице нет
                    self.obj_db.insert_row_into_table_2(self.name_main_table_api_code[api_code], row)
                else:
                    # предприятия есть в таблице. удаляем старую строку и записываем новую
                    if self.obj_db.delete_row(self.name_main_table_api_code[api_code], 'name_eng', 
                                              self.name_eng):
                        self.obj_db.insert_row_into_table_2(self.name_main_table_api_code[api_code], row)

            # получим строку подготовленную для расчета оценки
            row = self.obj_db.insert_row_into_table(self.name_main_table_api_code[api_code], parsing_result, 
                                                    'no')
            #msg = "{} row {}".format(self.name_class + name_func, str(row).encode('utf-8'))
            #obj_print.out_msg_info(msg)

            # строку конвертируем в кортеж, который получается при запросе строки из БД
            obj_work_any = wac(self.obj_db)
            row_tuple = obj_work_any.convert_str_tuple(row, self.name_main_table_api_code[api_code])
            #msg = "{}\n {} row_tuple_{} {}".format("*"*10, self.name_class + name_func, debug_api_code,
            #                                           str(row_tuple).encode('utf-8'))
            #obj_print.out_msg_info(msg)

            if len(row_tuple) != 0:
                # расчет оценки
                plus = {}
                high_risk = {}
                middle_risk = {}
                low_risk = {}
                warnings = {}
                complex_grade = {}
                len_dict = []
                columns = self.obj_db.get_column_list(self.name_main_table_api_code[api_code])
                obj_grade = Work_grade(self.obj_db, columns)
                if api_code == '430':
                    plus, high_risk, middle_risk, low_risk, warnings, complex_grade, len_dict = \
                        obj_grade.calculated_grade_factory(row_tuple, 'no')
                if api_code == '213':
                    plus, high_risk, middle_risk, low_risk, warnings, complex_grade, len_dict = \
                        obj_grade.calculated_grade_api_213(row_tuple, 'no')
                if api_code == '736':
                    plus, high_risk, middle_risk, low_risk, warnings, complex_grade, len_dict = \
                        obj_grade.calculated_grade_api_736(row_tuple, 'no')
                if api_code == '930':
                    plus, high_risk, middle_risk, low_risk, warnings, complex_grade, len_dict = \
                        obj_grade.calculated_grade_api_930(row_tuple, 'no')
                if api_code == '1003':
                    plus, high_risk, middle_risk, low_risk, warnings, complex_grade, len_dict = \
                        obj_grade.calculated_grade_api_1003(row_tuple, 'no')
                if api_code == '701':
                    plus, high_risk, middle_risk, low_risk, warnings, complex_grade, len_dict = \
                        obj_grade.calculated_grade_api_701(row_tuple, 'no')
                if api_code == '410':
                    plus, high_risk, middle_risk, low_risk, warnings, complex_grade, len_dict = \
                        obj_grade.calculated_grade_api_410(row_tuple, 'no')

                msg = "{} The calculation of the grade for api code {}".format(
                    self.name_class + name_func, api_code)
                self.obj_print.out_msg_info(msg)

                # вывод в окно или запись в файл результатов
                result = {}
                if len(plus) > 0:
                    msg = "Plus {}".format(str(plus).encode('utf-8'))
                    result['plus'] = plus
                    self.obj_print.out_msg_info(msg)
                if len(high_risk) > 0:
                    msg = "High risk {}".format(str(high_risk).encode('utf-8'))
                    result['high_risk'] = high_risk
                    self.obj_print.out_msg_info(msg)
                if len(middle_risk) > 0:
                    msg = "Middle risk {}".format(str(middle_risk).encode('utf-8'))
                    result['middle_risk'] = middle_risk
                    self.obj_print.out_msg_info(msg)
                if len(low_risk) > 0:
                    msg = "Low risk {}".format(str(low_risk).encode('utf-8'))
                    result['low_risk'] = low_risk
                    self.obj_print.out_msg_info(msg)
                if len(warnings) > 0:
                    msg = "Warnings {}".format(str(warnings).encode('utf-8'))
                    result['warnings'] = warnings
                    self.obj_print.out_msg_info(msg)
                msg = "{} Total due diligence {}".format(self.name_class + name_func, str(complex_grade).encode('utf-8'))
                self.obj_print.out_msg_info(msg)
                result['complex_grade'] = complex_grade
                result['len_dict'] = len_dict
                # if flag_command_line:
                #    path_file = self.path_json_file + '\\' + self.name_json_file_decode
                #    try:
                #        with open(path_file, 'w') as f:
                #            json.dump(result, f)
                #            f.close()
                #    except Exception as exc:
                #        msg = "{} Failed to write json score calculation results to file {}. Error {}".format(
                #            self.name_class + name_func, path_file, exc)
                #        self.obj_print.out_msg_error(msg)
                #        return
                return result
            else:
                msg = "{} Failed to convert string to tuple for score calculation api code {}".format(
                    self.name_class + name_func, api_code)
                self.obj_print.out_msg_error(msg)
                return {}
        else:
            msg = "{} Failed to parse json for api code {}".format(self.name_class + name_func, api_code)
            self.obj_print.out_msg_error(msg)
            return {}


class API():
    def __init__(self, obj_main_class):
        self.conn = obj_main_class.conn
        self.cursor = obj_main_class.cursor
        self.path_db = obj_main_class.path_db
        self.message = obj_main_class.message
        self.table_name = obj_main_class.table_name
        self.table_properties = obj_main_class.table_properties
        self.table_files = obj_main_class.table_files
        self.right_api_code = obj_main_class.right_api_code
        self.not_right_api_code = obj_main_class.not_right_api_code
        self.name_main_table_api_code = obj_main_class.name_main_table_api_code
        self.name_eng = obj_main_class.name_eng;
        self.name_ch = obj_main_class.name_ch;
        self.credit_code = obj_main_class.credit_code
        self.obj_db = Work_db(self.conn, self.cursor)
        self.obj_print = Print_msg(self.message)
        self.name_class = 'API'

    def control_and_create_any_table(self, name_table):
        name_func = '.control_and_create_any_table'
        # проверка наличия таблицы table_name и ее создание
        if not self.obj_db.check_table_is_exists(name_table):
            msg = "{} table {} not found. Create table".format(self.name_class + name_func, name_table)
            self.obj_print.out_msg_warning(msg)
            # создаем таблицу
            self.obj_db.table_management(self.table_properties[name_table])

    def status_responce(self, var):
        # проверка кода ответа сервера
        if jmespath.search("Status", var) == '200':
            return True
        else:
            return False

    def load_multi_json(self, json_line):
        try:
            return json.loads(json_line)
        except JSONDecodeError as err:
            if err.msg == 'Extra data':
                head = json.loads(json_line[0:err.pos])
                return head
            else:
                return {}

    def get_filname_error_json_line(self, api_code):
        filename = "Error_json_line_" + api_code + "_" + datetime.now().ctime() + ".txt"
        filename_ = filename.replace(" ", "_")
        filename__ = filename_.replace(":", "_")
        return filename__

    def decrypt_json(self, api_code, json_line):
        name_func = '.decrypt_json'
        # функция разбирает один json из поля ввода с указанным кодом и возвращает оценку предприятия
        try:
            if isinstance(json_line, str):
                var = self.load_multi_json(json_line)
                if len(var) == 0:
                    return 'error'
            elif isinstance(json_line, dict):
                var = json_line
                if len(var) == 0:
                    return 'error'
            else:
                msg = "{} Invalid data type in json_line {}. type json_line {}".format(
                    self.name_class + name_func, str(json_line).encode('utf-8'), type(json_line))
                return 'error'
        except Exception as exc:
            msg = "{} Failed to parse json. Error {}. type json_line {}".format(self.name_class + name_func, 
                                                                                str(exc), type(json_line))
            self.obj_print.out_msg_error(msg)
            msg = "{} error json_line {}".format(self.name_class + name_func, str(json_line).encode('utf-8'))
            with open(self.get_filname_error_json_line(api_code), 'w', encoding="utf-8") as f:
                f.write(json_line)
                f.close()
            return 'error'

        if self.status_responce(var):
            obj_api = Work_api_code(self.obj_db)
            parsing_result = 0
            if api_code == '430':
                parsing_result = obj_api.parsing_api_430(var)
            if api_code == '213':
                parsing_result = obj_api.parsing_api_213(var)
            if api_code == '736':
                parsing_result = obj_api.parsing_api_736(var)
            if api_code == '930':
                parsing_result = obj_api.parsing_api_930(var, self.name_eng, self.name_ch)
            if api_code == '1003':
                parsing_result = obj_api.parsing_api_1003(var)
            if api_code == '701':
                parsing_result = obj_api.parsing_api_701(var, self.name_eng, self.name_ch, self.credit_code)
            if api_code == '410':
                parsing_result = obj_api.parsing_api_410(var)
            msg = "{} Information in json {}".format(self.name_class + name_func, 
                                                     str(parsing_result).encode('utf-8'))
            self.obj_print.out_msg_info(msg)
            return parsing_result
        else:
            msg = "{} Status code server !=200. Json has no useful information".format(
                self.name_class + name_func)
            self.obj_print.out_msg_error(msg)
            return 'error'

def get_value(d, k, index):
    if k in d:
        if len(d[k]) != 0:
            try:
                return d[k][index]
            except Exception as exc:
                return 0
        else:
            return 0
    else:
        return 0

def extract_last_substring(string):
    substrings = string.split('_')
    last_substring = substrings[-1]
    return int(last_substring)

def read_json_file(file_path):
    try:
        with open(file_path, 'rb') as f:
            encoding = chardet.detect(f.read())['encoding']
        with open(file_path, 'r', encoding=encoding) as f:
            #print(f)
            data = json.load(f)
            if isinstance(data, str):
                data = json.loads(data)
            if isinstance(data, dict):
                return data
            else:
                return {}
    except Exception as e:
        #print(str(e))
        return {}

def count_commas(input_data):
    if isinstance(input_data, str) and input_data is not None and input_data != '':
        return input_data.count(',')
    else:
        return 0

def get_row_description(k, d):
    result = []
    if k in d and len(d[k]) > 0:
        row = d[k].split(";")
        for p in row:
            try:
                value = p.split(":")[1].strip().capitalize()
                result.append(value + ".")
            except:
                pass
        return " ".join(result)
    else:
        return "нет."


if __name__ == "__main__":
    name_class = "_name_=_main_"
    name_func = " Calculated_grade"
    parser = argparse.ArgumentParser(description='Calculated_grade')
    parser.add_argument('--id', type=str, help='id operation', default='1')
    parser.add_argument('--filename', type=str, help='file name', default='736_good_factory.json')
    args = parser.parse_args()

    #dbname = "attis-test"
    #user = "testuser"
    #password = "NB6DhKOgGa9QLZS2"
    #host = "195.49.214.50"

    # база на restAPI server Казахстан
    #dbname = "postgres"
    #user = "postgres"
    #password = "S?6wTGk2xQK7J`V5"   # пароль на машине в KZ
    #host = "localhost"

    # база на restAPI server Москва
    #dbname = "postgres"
    #user = "postgres"
    #password = "S?6wTGk2xQK7J`V5"   # пароль на машине в Москва
    #host = "localhost"

    # база на моей машине для записи данных о предприятиях
    dbname = "postgres"
    user = "postgres"
    password = "123"   # пароль на моей машине
    host = "localhost"

    # управление логированием
    # control_print -> not_print = 0, console = 1, win_interface = 2, console_interface = 3
    control_print = 0
    shared.print_comment = control_print
    obj_print = Print_msg(0)
    obj_main = Main(dbname, user, password, host)
    # api коды для которых написана обработка
    right_api_code = ['430', '213', '736', '930', '1003', '701', '410']
    # api коды где нет названия предприятия или название только на китайском
    api_code_without_name = ['930', '701', '213', '1003', '410']

    #if args.filename is not None:
    #    data_js = read_json_file(args.filename)
    #    if True:
    #        grade = {}
    #        len_plus = 0
    #        len_high_risk = 0
    #        len_middle_risk = 0
    #        len_low_risk = 0
    #        len_warnings = 0

    #        # Формат запроса, который передается на сервер
    #        #{"message": {"service_code":"410", "search_text":"abcd", "log_data": {
    #        #"all_records": "1", "service_code_0": "410", "search_text_0": "abcd", "log_data_0": "ответ китайцев", 
    #        #"service_code_1": "", "search_text_1": "", "log_data_1": "", 
    #        #"service_code_2": "", "search_text_2": "", "log_data_2":"", 
    #        #"service_code_3":"", "search_text_3":"", "log_data_3":""}}}
    #        # на сервере извлекаются данные по ключу "message" и записываются в файл
    #        # ключи "service_code", "search_text", "log_data" появились на каком-то этапе и возможно есть не везде
    #        # проверка формата данных. проверяем в записи наличие ключей
    #        if  "all_records" in data_js and \
    #            "service_code_0" in data_js and \
    #            "log_data_0" in data_js and \
    #            "search_text_0" in data_js:
    #            good_data = True
    #        else:
    #            if  "all_records" in data_js["log_data"] and \
    #                "service_code_0" in data_js["log_data"] and \
    #                "log_data_0" in data_js["log_data"] and \
    #                "search_text_0" in data_js["log_data"]:
    #                good_data = True
    #                data_js = data_js["log_data"]
    #            else:
    #                good_data = False
    #                msg = "{} unknown data format. required keys not found".format(name_class + name_func)
    #                obj_print.out_msg_error(msg)


    #        if good_data:
    #            all_records_s = jmespath.search("all_records", data_js)
    #            try:
    #                all_records = int(all_records_s)
    #            except ValueError:
    #                all_records = 0

    #            # сначала обрабатываем коды у которых есть название
    #            for k,v in data_js.items():
    #                if k != "all_records":
    #                    if k.find("service_code") != -1:
    #                        service_code = jmespath.search(k, data_js)
    #                        i = extract_last_substring(k)
    #                        k_log_data = "log_data_" + str(i)
    #                        log_data = jmespath.search(k_log_data, data_js)
    #                        k_search_text = "search_text_" + str(i)
    #                        search_text = jmespath.search(k_search_text, data_js)
    #                        msg = "{} 1st cycle key service_code {} = {}".format(name_class + name_func, k, v)
    #                        obj_print.out_msg_info(msg)
    #                        msg = "{} 1st cycle key k_log_data {}".format(name_class + name_func, 
    #                                                        str(k_log_data).encode('utf-8'))
    #                        obj_print.out_msg_info(msg)
    #                        # проверим есть ли обработка для сервис кода
    #                        if service_code not in right_api_code:
    #                            continue
    #                        # проверим есть ли название предприятия в сервис коде
    #                        if all_records > 1 and service_code in api_code_without_name:
    #                            continue
    #                        if len(service_code) !=0 and len(log_data) != 0:
    #                            result = obj_main.decrypt_json_and_grade(service_code, log_data, search_text)
    #                            msg = "{} result decrypt_json_and_grade {}".format(name_class + name_func, 
    #                                                                        str(result).encode('utf-8'))
    #                            obj_print.out_msg_info(msg)
    #                            if len(result) != 0:
    #                                for k_, v_ in result.items():
    #                                    if k_ != 'len_dict':
    #                                        if k_ not in grade:
    #                                            grade[k_] = str(v_)
    #                                        else:
    #                                            grade[k_] = grade[k_] + ", " + str(v_)
    #                                len_plus += result['len_dict'][0]
    #                                len_high_risk += result['len_dict'][1]
    #                                len_middle_risk += result['len_dict'][2]
    #                                len_low_risk += result['len_dict'][3]
    #                                len_warnings += result['len_dict'][4]

    #            # обработка кодов без названия предприятия 2-ым циклом
    #            if all_records > 1:
    #                for k,v in data_js.items():
    #                    if k != "all_records":
    #                        if k.find("service_code") != -1:
    #                            i = extract_last_substring(k)
    #                            k_log_data = "log_data_" + str(i)
    #                            service_code = jmespath.search(k, data_js)
    #                            log_data = jmespath.search(k_log_data, data_js)
    #                            msg = "{} 2nd cycle key service_code {} = {}".format(name_class + name_func, k, v)
    #                            obj_print.out_msg_info(msg)
    #                            msg = "{} 2nd cycle k_log_data {}".format(name_class + name_func, 
    #                                                        str(k_log_data).encode('utf-8'))
    #                            obj_print.out_msg_info(msg)
    #                            # проверим есть ли обработка для сервис кода
    #                            if service_code not in right_api_code:
    #                                continue
    #                            # проверим есть ли название предприятия в сервис коде
    #                            if service_code not in api_code_without_name:
    #                                continue
    #                            if len(service_code) !=0 and len(log_data) != 0:
    #                                result = obj_main.decrypt_json_and_grade(service_code, log_data, search_text)
    #                                msg = "{} result decrypt_json_and_grade {}".format(name_class + name_func, 
    #                                                                        str(result).encode('utf-8'))
    #                                obj_print.out_msg_info(msg)
    #                                if len(result) != 0:
    #                                    for k_, v_ in result.items():
    #                                        if k_ != 'len_dict':
    #                                            if k_ not in grade:
    #                                                grade[k_] = str(v_)
    #                                            else:
    #                                                grade[k_] = grade[k_] + ", " + str(v_)
    #                                    len_plus += result['len_dict'][0]
    #                                    len_high_risk += result['len_dict'][1]
    #                                    len_middle_risk += result['len_dict'][2]
    #                                    len_low_risk += result['len_dict'][3]
    #                                    len_warnings += result['len_dict'][4]

    #            weight_plus = 10
    #            weight_high_risk = -10
    #            weight_middle_risk = -5
    #            weight_low_risk = -2
    #            weight_warnings = -1
    #            begin_grade = 100
    #            complex_grade = begin_grade + \
    #                            len_plus * weight_plus + \
    #                            len_high_risk * weight_high_risk + \
    #                            len_middle_risk * weight_middle_risk + \
    #                            len_low_risk * weight_low_risk + \
    #                            len_warnings * weight_warnings
    #            grade['complex_grade'] = complex_grade
    #            msg = "{} grade {}".format(name_class + name_func, str(grade).encode('utf-8'))
    #            obj_print.out_msg_info(msg)

    #        # словарь с нужной последовательностью ключей для печати
    #        grade_for_out = {}
    #        if "plus" in grade:
    #            grade_for_out["plus"] = grade["plus"]
    #        if "high_risk" in grade:
    #            grade_for_out["high_risk"] = grade["high_risk"]
    #        if "middle_risk" in grade:
    #            grade_for_out["middle_risk"] = grade["middle_risk"]
    #        if "low_risk" in grade:
    #            grade_for_out["low_risk"] = grade["low_risk"]
    #        if "warnings" in grade:
    #            grade_for_out["warnings"] = grade["warnings"]
    #        if "complex_grade" in grade:
    #            grade_for_out["complex_grade"] = grade["complex_grade"]
    #        # добавляем в словарь информацию: как рассчитывается оценка и краткую характеристику компании
    #        if len(grade_for_out) > 0:
    #            if grade_for_out["complex_grade"] != 100:
    #                grade_for_out["description"] = "Краткая характеристика компании.".encode('utf-8').decode('utf-8')
    #                grade_for_out["description"] += " К сильным сторонам предприятия можно отнести: {}".format(
    #                    get_row_description('plus', grade_for_out)).encode('utf-8').decode('utf-8')
    #                grade_for_out["description"] += " Факторы, относящиеся к высокому риску работы с предприятием: {}".format(
    #                    get_row_description('high_risk', grade_for_out)).encode('utf-8').decode('utf-8')
    #                grade_for_out["description"] += " Факторы, относящиеся к среднему риску работы с предприятием: {}".format(
    #                    get_row_description('middle_risk', grade_for_out)).encode('utf-8').decode('utf-8')
    #                grade_for_out["description"] += " Факторы, относящиеся к низкому риску работы с предприятием: {}".format(
    #                    get_row_description('low_risk', grade_for_out)).encode('utf-8').decode('utf-8')
    #                grade_for_out["description"] += " Информация которую стоит принять во внимание: {}".format(
    #                    get_row_description('warnings', grade_for_out)).encode('utf-8').decode('utf-8')
    #            else:
    #                grade_for_out["description"] = "По предприятию есть вся необходимая информация, чтобы рассматривать его в качестве потенциального делового партнера. Мы не нашли в ней отклонений от нормы.".encode('utf-8').decode('utf-8')

    #            grade_for_out["as_calculated_grade"] = "Как мы рассчитали оценку?".encode('utf-8').decode('utf-8')
    #            grade_for_out["as_calculated_grade"] += " Изначально всем компаниям присваивается оценка в {} баллов.".format(
    #                begin_grade).encode('utf-8').decode('utf-8')
    #            grade_for_out["as_calculated_grade"] += " Далее проводится факторный анализ. В зависимости от наличия или отсутствия информации формируется 5 групп рисков:".encode('utf-8').decode('utf-8')
    #            grade_for_out["as_calculated_grade"] += " сильные стороны предприятия (plus), +{} баллов за каждое преимущество;".format(
    #                weight_plus).encode('utf-8').decode('utf-8')
    #            grade_for_out["as_calculated_grade"] += " высокий риск (high_risk), отсутствие важной информации, необходимой для установления деловых связей с компанией или сильно негативная информация о предприятии, {} баллов за каждое;".format(
    #                weight_high_risk).encode('utf-8').decode('utf-8')
    #            grade_for_out["as_calculated_grade"] += " средний риск (middle_risk), отсутствие некоторой информации, желательной для установления деловых связей с компанией или негативная информация о предприятии, {} баллов за каждое;".format(
    #                weight_middle_risk).encode('utf-8').decode('utf-8')
    #            grade_for_out["as_calculated_grade"] += " слабый риск (low_risk), отсутствие вспомогательной информации, желательной для установления деловых связей с компанией или слабо негативная информация о предприятии, {} баллов за каждое;".format(
    #                weight_low_risk).encode('utf-8').decode('utf-8')
    #            grade_for_out["as_calculated_grade"] += " предупреждения (warnings), информация не являющаяся негативной, но могущая повлиять на принятие решения, {} баллов за каждое.".format(
    #                weight_warnings).encode('utf-8').decode('utf-8')
    #            grade_for_out["as_calculated_grade"] += " Мы используем данные поставщика информации, которые могут быть неполными.".encode('utf-8').decode('utf-8')
    #            grade_for_out["as_calculated_grade"] += " Например, отсутствие адреса оценивается в -10 баллов, но это не означает что его на самом деле нет".encode('utf-8').decode('utf-8')

    #        try:
    #            sys.stdout.write(json.dumps(grade_for_out))
    #            msg = "********** end **********"
    #            obj_print.out_msg_info(msg)
    #        except Exception as exc:
    #            msg = "{} grade_for_out {}".format(name_class + name_func, str(grade_for_out).encode('utf-8'))
    #            obj_print.out_msg_error(msg)
    #            sys.stdout.write(json.dumps({}))
    #            msg = "{} ********** end **********".format(str(exc))
    #            obj_print.out_msg_error(msg)

    try:
        data_js = read_json_file(args.filename)
        grade = {}
        len_plus = 0
        len_high_risk = 0
        len_middle_risk = 0
        len_low_risk = 0
        len_warnings = 0

        # Формат запроса, который передается на сервер
        # длинный словарь
        #{"message": {"service_code":"410", "search_text":"abcd", "log_data": {
        #"all_records": "1", "service_code_0": "410", "search_text_0": "abcd", "log_data_0": "ответ китайцев", 
        #"service_code_1": "", "search_text_1": "", "log_data_1": "", 
        #"service_code_2": "", "search_text_2": "", "log_data_2":"", 
        #"service_code_3":"", "search_text_3":"", "log_data_3":""}}}
        # на сервере извлекаются данные по ключу "message" и записываются в файл
        # ключи "service_code", "search_text", "log_data" появились на каком-то этапе и возможно есть не везде
        # поэтому предусмотрена возможность обработки короткого словаря
        #{"message": {
        #"all_records": "1", "service_code_0": "410", "search_text_0": "abcd", "log_data_0": "ответ китайцев", 
        #"service_code_1": "", "search_text_1": "", "log_data_1": "", 
        #"service_code_2": "", "search_text_2": "", "log_data_2":"", 
        #"service_code_3":"", "search_text_3":"", "log_data_3":""}}

        # проверка формата данных. проверяем в записи наличие ключей
        if wac.control_data(data_js):
            if wac.search_key(data_js, "all_records"):
                if wac.control_empty_val(wac.get_value(data_js, "all_records")):
                    all_records = int(wac.get_value(data_js, "all_records"))
                    key_list = [("service_code_"+str(i), "search_text_"+str(i), "log_data_"+str(i)) for i in range(all_records)]
                    for k in key_list:
                        for k_sub in k:
                            if not wac.search_key(data_js, k_sub):
                                raise KeyError("Not key '{}' in dict".format(k_sub))
                else:
                    raise ValueError("Empty value 'all_records' in dict")
            else:
                raise KeyError("Not key 'all_records' in dict")

        if True:
            # сначала обрабатываем коды у которых есть название
            for k,v in data_js.items():
                if k != "all_records":
                    if k.find("service_code") != -1:
                        service_code = jmespath.search(k, data_js)
                        i = extract_last_substring(k)
                        k_log_data = "log_data_" + str(i)
                        log_data = jmespath.search(k_log_data, data_js)
                        k_search_text = "search_text_" + str(i)
                        search_text = jmespath.search(k_search_text, data_js)
                        msg = "{} 1st cycle key service_code {} = {}".format(name_class + name_func, k, v)
                        obj_print.out_msg_info(msg)
                        msg = "{} 1st cycle key k_log_data {}".format(name_class + name_func, 
                                                        str(k_log_data).encode('utf-8'))
                        obj_print.out_msg_info(msg)
                        # проверим есть ли обработка для сервис кода
                        if service_code not in right_api_code:
                            continue
                        # проверим есть ли название предприятия в сервис коде
                        if all_records > 1 and service_code in api_code_without_name:
                            continue
                        if len(service_code) !=0 and len(log_data) != 0:
                            result = obj_main.decrypt_json_and_grade(service_code, log_data, search_text)
                            msg = "{} result decrypt_json_and_grade {}".format(name_class + name_func, 
                                                                        str(result).encode('utf-8'))
                            obj_print.out_msg_info(msg)
                            if len(result) != 0:
                                for k_, v_ in result.items():
                                    if k_ != 'len_dict':
                                        if k_ not in grade:
                                            grade[k_] = str(v_)
                                        else:
                                            grade[k_] = grade[k_] + ", " + str(v_)
                                len_plus += result['len_dict'][0]
                                len_high_risk += result['len_dict'][1]
                                len_middle_risk += result['len_dict'][2]
                                len_low_risk += result['len_dict'][3]
                                len_warnings += result['len_dict'][4]

            # обработка кодов без названия предприятия 2-ым циклом
            if all_records > 1:
                for k,v in data_js.items():
                    if k != "all_records":
                        if k.find("service_code") != -1:
                            i = extract_last_substring(k)
                            k_log_data = "log_data_" + str(i)
                            service_code = jmespath.search(k, data_js)
                            log_data = jmespath.search(k_log_data, data_js)
                            msg = "{} 2nd cycle key service_code {} = {}".format(name_class + name_func, k, v)
                            obj_print.out_msg_info(msg)
                            msg = "{} 2nd cycle k_log_data {}".format(name_class + name_func, 
                                                        str(k_log_data).encode('utf-8'))
                            obj_print.out_msg_info(msg)
                            # проверим есть ли обработка для сервис кода
                            if service_code not in right_api_code:
                                continue
                            # проверим есть ли название предприятия в сервис коде
                            if service_code not in api_code_without_name:
                                continue
                            if len(service_code) !=0 and len(log_data) != 0:
                                result = obj_main.decrypt_json_and_grade(service_code, log_data, search_text)
                                msg = "{} result decrypt_json_and_grade {}".format(name_class + name_func, 
                                                                        str(result).encode('utf-8'))
                                obj_print.out_msg_info(msg)
                                if len(result) != 0:
                                    for k_, v_ in result.items():
                                        if k_ != 'len_dict':
                                            if k_ not in grade:
                                                grade[k_] = str(v_)
                                            else:
                                                grade[k_] = grade[k_] + ", " + str(v_)
                                    len_plus += result['len_dict'][0]
                                    len_high_risk += result['len_dict'][1]
                                    len_middle_risk += result['len_dict'][2]
                                    len_low_risk += result['len_dict'][3]
                                    len_warnings += result['len_dict'][4]

            weight_plus = 10
            weight_high_risk = -10
            weight_middle_risk = -5
            weight_low_risk = -2
            weight_warnings = -1
            begin_grade = 100
            complex_grade = begin_grade + \
                            len_plus * weight_plus + \
                            len_high_risk * weight_high_risk + \
                            len_middle_risk * weight_middle_risk + \
                            len_low_risk * weight_low_risk + \
                            len_warnings * weight_warnings
            grade['complex_grade'] = complex_grade
            msg = "{} grade {}".format(name_class + name_func, str(grade).encode('utf-8'))
            obj_print.out_msg_info(msg)

        # словарь с нужной последовательностью ключей для печати
        grade_for_out = {}
        if "plus" in grade:
            grade_for_out["plus"] = grade["plus"]
        if "high_risk" in grade:
            grade_for_out["high_risk"] = grade["high_risk"]
        if "middle_risk" in grade:
            grade_for_out["middle_risk"] = grade["middle_risk"]
        if "low_risk" in grade:
            grade_for_out["low_risk"] = grade["low_risk"]
        if "warnings" in grade:
            grade_for_out["warnings"] = grade["warnings"]
        if "complex_grade" in grade:
            grade_for_out["complex_grade"] = grade["complex_grade"]
        # добавляем в словарь информацию: как рассчитывается оценка и краткую характеристику компании
        if len(grade_for_out) > 0:
            if grade_for_out["complex_grade"] != 100:
                grade_for_out["description"] = "Краткая характеристика компании.".encode('utf-8').decode('utf-8')
                grade_for_out["description"] += " К сильным сторонам предприятия можно отнести: {}".format(
                    get_row_description('plus', grade_for_out)).encode('utf-8').decode('utf-8')
                grade_for_out["description"] += " Факторы, относящиеся к высокому риску работы с предприятием: {}".format(
                    get_row_description('high_risk', grade_for_out)).encode('utf-8').decode('utf-8')
                grade_for_out["description"] += " Факторы, относящиеся к среднему риску работы с предприятием: {}".format(
                    get_row_description('middle_risk', grade_for_out)).encode('utf-8').decode('utf-8')
                grade_for_out["description"] += " Факторы, относящиеся к низкому риску работы с предприятием: {}".format(
                    get_row_description('low_risk', grade_for_out)).encode('utf-8').decode('utf-8')
                grade_for_out["description"] += " Информация которую стоит принять во внимание: {}".format(
                    get_row_description('warnings', grade_for_out)).encode('utf-8').decode('utf-8')
            else:
                grade_for_out["description"] = "По предприятию есть вся необходимая информация, чтобы рассматривать его в качестве потенциального делового партнера. Мы не нашли в ней отклонений от нормы.".encode('utf-8').decode('utf-8')

            grade_for_out["as_calculated_grade"] = "Как мы рассчитали оценку?".encode('utf-8').decode('utf-8')
            grade_for_out["as_calculated_grade"] += " Изначально всем компаниям присваивается оценка в {} баллов.".format(
                begin_grade).encode('utf-8').decode('utf-8')
            grade_for_out["as_calculated_grade"] += " Далее проводится факторный анализ. В зависимости от наличия или отсутствия информации формируется 5 групп рисков:".encode('utf-8').decode('utf-8')
            grade_for_out["as_calculated_grade"] += " сильные стороны предприятия (plus), +{} баллов за каждое преимущество;".format(
                weight_plus).encode('utf-8').decode('utf-8')
            grade_for_out["as_calculated_grade"] += " высокий риск (high_risk), отсутствие важной информации, необходимой для установления деловых связей с компанией или сильно негативная информация о предприятии, {} баллов за каждое;".format(
                weight_high_risk).encode('utf-8').decode('utf-8')
            grade_for_out["as_calculated_grade"] += " средний риск (middle_risk), отсутствие некоторой информации, желательной для установления деловых связей с компанией или негативная информация о предприятии, {} баллов за каждое;".format(
                weight_middle_risk).encode('utf-8').decode('utf-8')
            grade_for_out["as_calculated_grade"] += " слабый риск (low_risk), отсутствие вспомогательной информации, желательной для установления деловых связей с компанией или слабо негативная информация о предприятии, {} баллов за каждое;".format(
                weight_low_risk).encode('utf-8').decode('utf-8')
            grade_for_out["as_calculated_grade"] += " предупреждения (warnings), информация не являющаяся негативной, но могущая повлиять на принятие решения, {} баллов за каждое.".format(
                weight_warnings).encode('utf-8').decode('utf-8')
            grade_for_out["as_calculated_grade"] += " Мы используем данные поставщика информации, которые могут быть неполными.".encode('utf-8').decode('utf-8')
            grade_for_out["as_calculated_grade"] += " Например, отсутствие адреса оценивается в -10 баллов, но это не означает что его на самом деле нет".encode('utf-8').decode('utf-8')

        sys.stdout.write(json.dumps(grade_for_out))
        msg = "********** end **********"
        obj_print.out_msg_info(msg)

    except Exception as exc:
        grade_for_out = {}
        grade_for_out["Error"] = str(exc)
        msg = "{} grade_for_out {}".format(name_class + name_func, str(grade_for_out).encode('utf-8'))
        obj_print.out_msg_error(msg)
        sys.stdout.write(json.dumps(grade_for_out))
        msg = "{} ********** end **********".format(str(exc))
        obj_print.out_msg_info(msg)
        sms.send_email("Calculated_grade - error", str(exc))




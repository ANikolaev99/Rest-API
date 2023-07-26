# coding: utf-8

import jmespath
import re
from Work_translator import Work_translator
import sys
from Print_msg import Print_msg
from Work_any_class import Work_any_class as wac

NAME_CLASS = "Work_api_code"

class Work_api_code(object):
    def __init__(self, obj_db):
        self.obj_db = obj_db
        self.obj_print = Print_msg(0)

    def control_china_language(self, s):
        # проверка на китайские иероглифы в строке
        #try:
        #    if len(s) == 0:
        #        return False
        #except Exception as exc:
        #    return False
        #result = False
        #for x in s:
        #    if  u'\u4e00' <= x <= u'\u9fff':
        #        result = True
        #        break
        #return result
        if s is None or not isinstance(s, str):
            return False
        else:
            chinese_pattern = re.compile(r'[\u4e00-\u9fff]+')
            return bool(chinese_pattern.search(s))

    def delete_duplicated(self, s):
        # удаление дубликатов в строке
        if wac.len_c(s) == 0:
            return s
        lst = list(set(s.split(",")))
        new_s = ','.join(lst)
        return new_s

    def get_factory_name_eng(self, var):
        # извлечение английского названия из записи
        name = jmespath.search("Result.EnglishName", var)
        # вариант когда в китайском имени английское название, а 
        # наименования на английском нет
        if name == '':
            name_ch = jmespath.search("Result.Name", var)
            if name_ch != '' and self.control_china_language(name_ch) == False:
                name = name_ch
            elif name_ch != '' and self.control_china_language(name_ch):
                obj_translator = Work_translator(self.obj_db)
                name = obj_translator.translator_eng(name_ch)
        return name

    def parsing_api_430(self, var):
        name_func = ".parsing_api_430"
        # разбирает запись из search_log и формирует строку для записи в _factories
        # словарь соответствия ключей записи колонкам в _factories
        right_keys = {
                    'EnglishName': 'name_eng',              # название на английском - без перевода
                    'Name': 'name_ch',                      # название на китайском - без перевода
                    'CreditCode': 'creditcode',             # - без перевода
                    'OperName': 'Директор',                 # директор
                    'Status': 'Статус',                     # статус предприятия                
                    'StartDate': 'Дата_рег',                # дата регистрации - без перевода
                    'EconKind': 'Тип_собств',               # тип собственности
                    'BelongOrg': 'Надзор',                  # надзорный орган
                    'PersonScope': 'Персонал',              # кол-во сотрудников
                    'Industry.Industry': 'Индустрия',
                    'Industry.SubIndustry': 'Подиндустрия',
                    'Industry.MiddleCategory': 'Категория',                  
                    'Address': 'Адрес',                     # адрес
                    'PhoneNumber': 'Телефон',               # - без перевода
                    'Email': 'email',                       # - без перевода
                    'WebSiteUrl': 'website',                # - без перевода                 
                    'Scope': 'Род_деят',                    # роды деятельности
                    #'ParentInfo.Name': 'Главк',             # вышестоящая организация
                    'ParentInfo': 'Главк',                  # вышестоящая организация
                    'IsOnStock': 'isonstock',               # торгуется на бирже 0/1 (да/нет)
                    'StockNumber': 'stocknum',              # что-то относящееся к бирже ???
                    'StockType': 'stocktype'                # название биржи            
                }

        # собираем данные
        row = {}
        for key, column in right_keys.items():
            full_key = "Result." + key
            # получаем значение поля в записи
            value = jmespath.search(full_key, var)
            #print('full_key ', full_key, ' value ', str(value).encode('utf-8'), ' column ', column,
            #       file=sys.stdout, flush=True)
            if wac.control_empty_val(value):
                #if key == "PersonScope":
                #    print("{} value for key {} is {}".format(NAME_CLASS+name_func, key, value))
                # ключи которые нужно переводить
                if key != 'Name' and key != 'IsOnStock' and key != 'StockNumber' and \
                key != 'PhoneNumber' and key != 'Email' and key != 'WebSiteUrl' and \
                self.control_china_language(value):
                    obj_translator = Work_translator(self.obj_db)
                    if key != 'StockType':
                        # перевод китайский -> русский
                        translate_value = obj_translator.translator(value, key)
                        #if key == "PersonScope":
                        #    print("{} translate_value for key {} is {}".format(NAME_CLASS+name_func, key, translate_value))
                        if key == 'Address':
                            translate_value = self.delete_duplicated(translate_value)
                        row[column] = translate_value
                    else:
                        # перевод китайский -> английский
                        translate_value = obj_translator.translator_eng(value)
                        row[column] = translate_value
                else:
                    #if key == "PersonScope":
                    #    print("{} not china language for key {}".format(NAME_CLASS+name_func, key))
                    row[column] = value
            else:
                # пропуски
                if key == 'EnglishName':
                    # возможно англ имя в Name
                    row[column] = self.get_factory_name_eng(var)
        return row

    def control_null_field_factories(self, factory_name):
        # проверка на пустые поля в таблице _factories
        # список полей для проверки
        field_lst = ["creditcode", "Директор", "Статус", "Тип_собств", "Персонал", "Индустрия", 
                     "Адрес", "Телефон", "email", "website", "Род_деят"]
        len_result, result = self.obj_db.control_null_field('_factories', 'name_eng', factory_name, field_lst)
        return len_result

    def write_msg_system_log(self, name_proc, comment, code):
        # запись сообщения в _system_log
        msg_dict = {
            "name_process": name_proc,
            "comment": comment,
            "code_finish": code
            }
        # удаляем все старые
        self.obj_db.delete_row('_system_log', 'comment', comment)
        # записываем сообщение
        self.obj_db.insert_row_into_table('_system_log', msg_dict)

    def control_assets_data(self, var):
        # проверка раздела assets_data годовом отчете (213-ый код) на полезную информацию
        have_information = False
        result = jmespath.search("Result", var)
        assets_data = jmespath.search("AssetsData", result[0])
        unnecessary_keys = [
            'Year',
            'No',
            'PublishDate'
        ]
        if wac.len_c(assets_data) > 0:
            for k, v in assets_data.items():
                if k not in unnecessary_keys and v != '' and v != '企业选择不公示':
                    have_information = True
                    break
        return have_information

    def control_employee_list(self, var):
        # проверка раздела employee_list годовом отчете (213-ый код) на полезную информацию
        have_information = False
        result = jmespath.search("Result", var)
        assets_data = jmespath.search("EmployeeList", result[0])
        if wac.len_c(assets_data) > 0:
            for k, v in assets_data.items():
                if v != '' and v != '企业选择不公示':
                    have_information = True
                    break
        return have_information

    def control_social_insurance(self, var):
        # проверка раздела social_insurance годовом отчете (213-ый код) на полезную информацию
        have_information = False
        result = jmespath.search("Result", var)
        assets_data = jmespath.search("SocialInsurance", result[0])
        right_keys = [
            "OweUrbanBasicInsAmount", 
            "OweUnemploymentInsAmount",
            "OweEmployeeBasicInsAmount",
            "OweIndustrialInjuryInsAmount",
            "OweMaternityInsAmount"
        ]
        unnecessary_keys = [
            'Year',
            'No',
            'PublishDate'
        ]
        if wac.len_c(assets_data) > 0:
            for k, v in assets_data.items():
                if k in right_keys and k not in unnecessary_keys and v != '' and v != '企业选择不公示':
                    have_information = True
                    break
        return have_information

    def get_factory_from_factories(self, credit_code, email):
        # поиск предприятия в _factories по creditcode или email с возвратом строки 
        len_res, result = self.obj_db.select_simple('_factories', '*', 'creditcode', credit_code)
        if len_res != 0:
            return len_res, result
        else:
            # пробуем поиск по email
            len_res, result = self.obj_db.select_simple('_factories', '*', 'email', email)
            if len_res != 0:
                return len_res, result
            else:
                return 0, None

    def extract_numeric_value_from_string(self, value):
        name_func = '.extract_numeric_value_from_string'
        # конвертирует суммы вида "9755.54万元" (десятки тысяч юаней) в числовые данные
        # "6102.175万元人民币" (млн юаней)
        #'千': 1000,	# тысяча	
        #'千元': 1000, # тысяча юаней, тысяч юаней,
        #'万': 10000, # десять тысяч	
        #'万元': 10000, # десять тысяч юаней	
        #'几万元': 10000, # десятки тысяч юаней
        #'十万': 100000, # сто тысяч 
        #'十万元': 100000, # сто тысяч юаней		
        #'几十万元': 100000, # сотни тысяч юаней	 
        #'百万': 1000000, # миллион
        #'万元': 1000000, # миллион юаней, миллионы юаней
        # "万元人民币" десять тысяч юаней
        # value_correct = value.replace('万元', '')
        if isinstance(value, str) and wac.control_empty_val(value):
            digits = re.findall(r'[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?', value)
            try:
                if wac.len_c(digits) == 0:
                    return 0
                else:
                    return float(digits[0])
            except Exception as exc:
                msg = "{} error converting {} to number, error {}".format(
                       NAME_CLASS + name_func, str(value).encode('utf-8'), str(exc))
                self.obj_print.out_msg_error(msg)
                return 0
        else:
            return 0

    def parsing_api_213(self, var):
        # разбирает запись из search_log и формирует строку для записи в _factory_reports
        row = {}
        # получаем раздел BasicInfoData. Если предприятия есть в _factories, возможно нужно дополнить
        # информацию, если нет, новое преприятие, добавляем в _factories всю базовую информацию

        # словарь соответствия колонок в _factories ключам BasicInfoData
        right_column = {
                     'name_ch': 'CompanyName',                      # название на китайском - без перевода
                     'creditcode': 'CreditCode',                    # - без перевода
                     'Директор': 'OperatorName',                    # директор
                     'Статус': 'Status',                            # статус предприятия                
                     'Персонал': 'EmployeeCount',                   # кол-во сотрудников                 
                     'Адрес': 'Address',                            # адрес
                     'Телефон': 'ContactNo',                        # - без перевода
                     'email': 'EmailAddress',                       # - без перевода               
                     'Род_деят': 'GeneralOperationItem'             # роды деятельности           
                }

        result = jmespath.search("Result", var)
        if wac.control_empty_val(result):
            basic_info_data = jmespath.search("BasicInfoData", result[0])
            if wac.control_empty_val(basic_info_data):
                # basic_info_data  {'RegNo': '320282000163216', 'CompanyName': '江苏沛尔膜业股份有限公司', 
                # 'CreditCode': '91320200663800287B', 'OperatorName': '', 'ContactNo': '0510-87838770', 
                # 'PostCode': '214200', 'Address': '宜兴市高塍镇冯家桥', 'EmailAddress': '323831842@qq.com', 
                # 'IsStockRightTransfer': '否', 'Status': '开业', 'HasWebSite': '是', 'HasNewStockOrByStock': 
                # ' 否', 'EmployeeCount': '77人', 'BelongTo': '', 'CapitalAmount': '', 'HasProvideAssurance': '否', 
                # 'OperationPlaces': '', 'MainType': '', 'OperationDuration': '', 'IfContentSame': '', 
                # 'DifferentContent': '', 'GeneralOperationItem': '', 'ApprovedOperationItem': ''}
                # проверяем есть ли предприятие в _factories
                credit_code = jmespath.search("CreditCode", basic_info_data)
                email = jmespath.search("EmailAddress", basic_info_data)
                if wac.control_empty_val(credit_code) and wac.control_empty_val(email):
                    len_res, row_factory = self.get_factory_from_factories(credit_code, email)
                    if len_res != 0 and wac.control_empty_val(row_factory):
                        name_eng = ''
                        #print('row_factory ', str(row_factory).encode('utf-8'), file=sys.stdout, flush=True)
                        # [(67, 'Jiangsu Peier Membrane Corp.,Ltd.', '江苏沛尔膜业股份有限公司', 115, 'Индустрия: 
                        # производственная компания; isonstock: акции компании торгуются на бирже; ', '', 
                        # 'Персонал: малое предприятие; ', '', '', '91320200663800287B', 'Чжоу Каню', 'существующее', 
                        # datetime.date(2007, 6, 27), 'Co., Ltd. (не списковые, инвестиции в естественные лица или владение)', ...]
                        # получим список столбцов _factories
                        # (2, 'name_eng', 'character varying', None)
                        columns = self.obj_db.get_column_list('_factories')
                        for value_column, column in zip(row_factory[0], columns):
                            name_column = column[1]
                            #print(str(value_column).encode('utf-8'), '', name_column, file=sys.stdout, flush=True)
                            if name_column == 'name_eng' and wac.control_empty_val(value_column):
                                name_eng = value_column
                                # если есть английское название, добавляем в строку для записи в '_factory_reports'
                                row['name_eng'] = name_eng
                            if name_column in right_column:
                                # если в _factories поле не заполнено, а в basic_info_data есть значение, обновляем ячейку в _factories
                                if not wac.control_empty_val(value_column) and \
                                    wac.control_empty_val(basic_info_data[right_column[name_column]]):
                                    self.obj_db.set_value_cell('_factories', name_column, basic_info_data[right_column[name_column]],
                                                               'name_eng', name_eng)
                                    # устанавливаем флаг обновления оценки
                                    self.obj_db.set_value_cell('_factories', 'update_grade', 1, 'name_eng', name_eng, 'int')
                    else:
                        # предприятия нет в _factories.
                        obj_translator = Work_translator(self.obj_db)
                        value = jmespath.search("CompanyName", basic_info_data)
                        if wac.control_empty_val(value):
                            row['name_eng'] = obj_translator.translator_eng(value)
                    # добавляем данные из BasicInfoData в строку для записи
                    row['creditcode'] = credit_code
                    row['email'] = email
                    if wac.control_empty_val(jmespath.search("CompanyName", basic_info_data)):
                        row['name_ch'] = basic_info_data['CompanyName']

            # получаем раздел AssetsData
            assets_data = jmespath.search("AssetsData", result[0])
            if wac.control_empty_val(assets_data):
                #print('assets_data ', str(assets_data).encode('utf-8'), file=sys.stdout, flush=True)
                # словарь соответствия ключей раздела AssetsData колонкам в _factory_reports
                right_keys_assets_data = {
                             'TotalAssets': 'Итого_активы', 
                             'TotalAssets': 'Капитал_владельца',
                             'GrossTradingIncome': 'Доход',
                             'TotalProfit': 'Прибыль',
                             'MainBusinessIncome': 'Доход_осн_бизнес',
                             'NetProfit': 'Чистая_прибыль',
                             'TotalTaxAmount': 'Сумма_по_налогам',
                             'TotalLiabilities': 'Обязательства',
                             'BankingCredit': 'Кредиты',
                             'GovernmentSubsidy': 'Господдержка',
                        }
                if wac.len_c(assets_data) > 0:
                    for key, column in right_keys_assets_data.items():
                        # получаем значение поля в записи
                        if key in assets_data:
                            value = assets_data[key]
                            if wac.control_empty_val(value) and value != '企业选择不公示':
                                row[column] = self.extract_numeric_value_from_string(value)

            # получаем раздел SocialInsurance
            social_insurance = jmespath.search("SocialInsurance", result[0])
            if wac.control_empty_val(social_insurance):
                #print('social_insurance ', str(social_insurance).encode('utf-8'), file=sys.stdout, flush=True)
                # словарь соответствия ключей раздела SocialInsurance колонкам в _factory_reports
                right_keys_social_insurance = {
                             'OweUrbanBasicInsAmount': 'Долг_накоп_страх', 
                             'OweUnemploymentInsAmount': 'Долг_безраб_страх',
                             'OweEmployeeBasicInsAmount': 'Долг_мед_страх',
                             'OweIndustrialInjuryInsAmount': 'Долг_компенс_работникам'
                        }
                if wac.len_c(social_insurance) > 0:
                    for key, column in right_keys_social_insurance.items():
                        if key in social_insurance:
                            value = social_insurance[key]
                            if wac.control_empty_val(value) and value != '企业选择不公示':
                                row[column] = self.extract_numeric_value_from_string(value)

            # получаем дату публикации отчета
            publish_date = jmespath.search("PublishDate",result[0])
            if wac.control_empty_val(publish_date):
                row['Дата_отчета'] = publish_date

        return row

    def parsing_api_736(self, var):
        # разбирает запись из search_log и формирует строку для записи в _factory_risk
        # словарь соответствия ключей записи колонкам в _factories
        right_keys = {
                    'EnglishName': 'name_eng',              # название на английском - без перевода
                    'Name': 'name_ch',                      # название на китайском - без перевода
                    'CreditCode': 'creditcode',             # - без перевода
                    'ContactInfo.Email': 'email',           # - без перевода
                    'Penalty': ['Штрафы', 'Описание', 'Даты'],               
                    "RegistCapi": 'Уст_капитал'          
                }

        # собираем данные
        row = {}
        obj_translator = Work_translator(self.obj_db)
        for key, column in right_keys.items():
            full_key = "Result." + key
            # получаем значение поля в записи
            value = jmespath.search(full_key, var)
            #print('full_key ', full_key, ' value ', str(value).encode('utf-8'), ' column ', column, 
            #      file=sys.stdout, flush=True)
            if wac.control_empty_val(value):
                # ключи которые нужно переводить
                if key == 'Penalty':
                    # список словарей штрафов
                    if wac.len_c(value) > 0:
                        penalty_type = []
                        penalty_content = []
                        penalty_date = []
                        for p in value:
                            if wac.control_empty_val(p['PenaltyType']):
                                translate_value = obj_translator.translator(p['PenaltyType'])
                                penalty_type.append(translate_value)
                            if wac.control_empty_val(p['Content']):
                                translate_value = obj_translator.translator(p['Content'])
                                penalty_content.append(translate_value)
                            if wac.control_empty_val(p['PenaltyDate']):
                                penalty_date.append(p['PenaltyDate'])
                        row[column[0]] = ";".join(penalty_type)
                        row[column[1]] = ";".join(penalty_content)
                        row[column[2]] = ";".join(penalty_date)
                #elif key == 'RegistCapi' and self.control_china_language(value) and value != '企业选择不公示':
                #    row[column] = self.extract_numeric_value_from_string(value)
            else:
                # пропуски
                if key == 'EnglishName':
                    # возможно англ имя в Name
                    row[column] = self.get_factory_name_eng(var)
        return row

    def parsing_api_930(self, var, factory_name_eng='', factory_name_ch=''):
        # собираем данные
        row = {}
        obj_translator = Work_translator(self.obj_db)
        # проверяем наличие записей
        result = jmespath.search("Result", var)
        if wac.control_empty_val(result):
            verify_result = jmespath.search("VerifyResult",result)
            if wac.control_empty_val(verify_result):
                row['name_eng'] = factory_name_eng
                row['name_ch'] = factory_name_ch
                data = jmespath.search("Data",result)
                if wac.control_empty_val(data):
                    row['Всего_дел'] = wac.len_c(data)
                    as_prosecutor = 0       # как ответчик
                    as_defendant = 0        # как истец
                    reason = []
                    for rec in data:
                        if wac.len_c(rec['ProsecutorList']) != 0:
                            as_prosecutor += 1
                        if wac.len_c(rec['DefendantList']) != 0:
                            as_defendant += 1        
                        if wac.len_c(rec['CaseReason']) != 0:
                            reason.append(rec['CaseReason'])
                    row['Как_ответчик'] = as_prosecutor
                    row['Как_истец'] = as_defendant
                    reason_set = set(reason)
                    s = ''
                    if wac.len_c(reason_set) > 0:
                        for r in reason_set:
                            translate_value = obj_translator.translator(r)
                            s += translate_value + ','
                        row['Причины_судов'] = s[:-1]
        return row

    def parsing_api_1003(self, var, factory_name=''):
        name_func = ".parsing_api_1003"
        # собираем данные
        row = {}
        obj_translator = Work_translator(self.obj_db)
        result = jmespath.search("Result", var)

        if wac.control_empty_val(result):
            if wac.control_empty_val(jmespath.search("CompanyName",result)):
                row['name_ch'] = jmespath.search("CompanyName",result)
            if factory_name != '':
                row['name_eng'] = factory_name
            else:
                if 'name_ch' in row and wac.control_empty_val(jmespath.search("name_ch",row)):
                    # factory_name = 0, если name_eng не найдено
                    factory_name = self.obj_db.get_value_cell('_factories', 'name_eng', "name_ch", row['name_ch'])
                    if factory_name != 0:
                        row['name_eng'] = factory_name
                    else:
                        row['name_eng'] = obj_translator.translator_eng(row['name_ch'])

            if wac.control_empty_val(jmespath.search("FindMatched",result)) and \
                wac.control_empty_val(jmespath.search("BreakThroughList",result)):
                if jmespath.search("FindMatched", result) == 'N' or \
                    wac.len_c(jmespath.search("BreakThroughList", result)) == 0:
                    # нет информации о бенефициарах, извлекаем примечание
                    row['Бенефициары'] = "неизвестны"
                    if wac.control_empty_val(jmespath.search("Remark", result)):
                        row['Примечание'] = obj_translator.translator(str(jmespath.search("Remark", result)))
                    row['Бенефициар'] = ''
                    row['Доля_бенефициара'] = ''
                    row['Тип_владения'] = ''
                    row['Позиция'] = ''
                else:
                    # есть информация о бенефициарах
                    row['Бенефициары'] = "известны"
                    if wac.control_empty_val(jmespath.search("BreakThroughList", result)):
                        try:
                            benefit_list = jmespath.search("BreakThroughList", result)[0]
                            row['Бенефициар'] = obj_translator.translator(str(benefit_list['Name']))
                            row['Доля_бенефициара'] = str(benefit_list['TotalStockPercent'])
                            row['Тип_владения'] = obj_translator.translator(str(benefit_list['BenifitType']))
                            row['Позиция'] = obj_translator.translator(str(benefit_list['Position']))
                        except Exception as exc:
                            msg = "{} error {}".format(NAME_CLASS + name_func, str(exc))
                            self.obj_print.out_msg_error(msg)
        return row

    def parsing_api_701(self, var, factory_name_eng='', factory_name_ch='', credit_code=''):
        name_func = ".parsing_api_701"
        # собираем данные
        row = {}
        result = jmespath.search("GroupItems", var)
        if wac.control_empty_val(result):
            #msg = "{} result = jmespath.search(\"GroupItems\", var) {}".format(NAME_CLASS + name_func, result)
            #self.obj_print.out_msg_info(msg)
            row['name_eng'] = factory_name_eng
            row['name_ch'] = factory_name_ch
            row['CreditCode'] = credit_code
            neutral = 0
            positive = 0
            negative = 0
            for d in result:
                if 'Key' in d and d['Key'] == 'impact':
                    if 'Items' in d:
                        for type_news in d['Items']:
                            if 'Value' in type_news:
                                if type_news['Value'] == 'negative':
                                    if 'Count' in type_news:
                                        negative = int(type_news['Count'])
                                if type_news['Value'] == 'positive':
                                    if 'Count' in type_news:
                                        positive = int(type_news['Count'])
                                if type_news['Value'] == 'none':
                                    if 'Count' in type_news:
                                        neutral = int(type_news['Count'])
            row["нейтральные"] = neutral
            row["позитивные"] = positive
            row["негативные"] = negative
            if (neutral + positive + negative) != 0:
                row["процент_положительных"] = round(
                    ((neutral + positive) / (neutral + positive + negative)) * 100, 0)
            else:
                row["процент_положительных"] = 0
        return row

    def parsing_api_410(self, var, factory_name=''):
        # собираем данные
        row = {}
        obj_translator = Work_translator(self.obj_db)

        result = jmespath.search("Result", var)
        if wac.control_empty_val(result):
            if wac.control_empty_val(jmespath.search("Name",result)):
                row['name_ch'] = jmespath.search("Name",result)
            if factory_name != '':
                row['name_eng'] = factory_name
            else:
                if 'name_ch' in row and wac.control_empty_val(jmespath.search("name_ch",row)):
                    factory_name = self.obj_db.get_value_cell('_factories', 'name_eng', "name_ch", row['name_ch'])
                    if factory_name != 0:
                        row['name_eng'] = factory_name
                    else:
                        row['name_eng'] = obj_translator.translator_eng(row['name_ch'])

            if wac.control_empty_val(jmespath.search("CreditCode",result)):
                row['creditcode'] = jmespath.search("CreditCode",result)
            if wac.control_empty_val(jmespath.search("OperName",result)):
                row['Директор'] = obj_translator.translator(jmespath.search("OperName",result))
            if wac.control_empty_val(jmespath.search("Status",result)):
                row['Статус'] = obj_translator.translator(jmespath.search("Status",result), "Status")
            if wac.control_empty_val(jmespath.search("StartDate",result)):
                row['Дата_рег'] = jmespath.search("StartDate",result)
            if wac.control_empty_val(jmespath.search("EconKind",result)):
                row['Тип_собств'] = obj_translator.translator(jmespath.search("EconKind",result))
            if wac.control_empty_val(jmespath.search("BelongOrg",result)):
                row['Надзор'] = obj_translator.translator(jmespath.search("BelongOrg",result))
            if wac.control_empty_val(jmespath.search("Address",result)):
                row['Адрес'] = obj_translator.translator(jmespath.search("Address",result))
            if wac.control_empty_val(jmespath.search("Scope",result)):
                row['Род_деят'] = obj_translator.translator(jmespath.search("Scope",result))
            # RegistCapi не участвует в оценке, отключаю
            #if control_empty_val(jmespath.search("RegistCapi",result)):
            #    row['Уст_капитал'] = obj_translator.translator(jmespath.search("RegistCapi",result))
            if wac.control_empty_val(jmespath.search("IsOnStock",result)):
                row['isonstock'] = jmespath.search("IsOnStock",result)
            if wac.control_empty_val(jmespath.search("StockNumber",result)):
                row['stocknum'] = jmespath.search("StockNumber",result)
            if wac.control_empty_val(jmespath.search("StockType",result)):
                row['stocktype'] = obj_translator.translator(jmespath.search("StockType",result))
        return row
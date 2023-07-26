# coding: utf-8

from datetime import datetime
from dateutil.parser import parse
import re
import sys
from Print_msg import Print_msg
from Work_any_class import Work_any_class as wac

NAME_CLASS = "Work_grade"

class Work_grade(object):
    def __init__(self, obj_db, columns={}, name_main_table_api_code={}):
        self.obj_db = obj_db
        self.columns = columns      # список колонок в таблице
        self.plus = {}              # словарь с плюсами
        self.high_risk = {}         # словарь высокий риск
        self.middle_risk = {}       # словарь средний риск
        self.low_risk = {}          # словарь низкий риск
        self.warnings = {}          # словарь с предупреждениями
        self.name_main_table_api_code = name_main_table_api_code
        self.obj_print = Print_msg(0)
        self.name_key = {
                'creditcode': 'Кредитный код',
	            'director': 'Директор', 
	            'status': 'Статус', 
	            'property': 'Тип собственности', 
	            'personal': 'Количество работающих',
                'industry': 'Вид деятельности',
                'address': 'Адрес',
		        'phone': 'Телефон',
		        'email': 'Электронная почта',
		        'website': 'Сайт',
		        'occupation': 'Род деятельности',
		        'affiliated': 'Материнская компания',
		        'stock': 'Фондовый рынок',
                'date_reg': 'Дата регистрации предприятия',
                'report': 'Годовой отчет',
                'report_param_1': 'Финансовые показатели. Коэффициент ликвидности',
                'report_param_2': 'Финансовые показатели. Чистая прибыль',
                'report_param_3': 'Финансовые показатели. Господдержка',
                'report_param_4': 'Финансовые показатели. Иностранные_инвесторы',
                'report_param_5': 'Финансовые показатели. Долг накопительное страхование',
                'report_param_6': 'Финансовые показатели. Долг страхование от безработицы',
                'report_param_7': 'Финансовые показатели. Долг медицинское страхование',
                'report_param_8': 'Финансовые показатели. Долг перед работниками',
                'risk': 'Корпоративные риски',
                'penalty': 'Штрафы',
                'description_penalty': 'Описание',
                'date_penalty': 'Даты',
                'court': 'Судебная практика',
                'court_param_1': 'Количество судебных разбирательств',
                'court_param_2': 'Как ответчик',
                'court_param_3': 'Как истец',
                'court_param_4': 'Причины судебных разбирательств',
                'benefit': 'Бенефициары',
                'public_opinion': 'Общественное мнение'
            }

    def calculated_grade_factory(self, row, write='yes'):
        name_func = '.calculated_grade_factory'
        # write='yes' комплексная оценка по всем таблицам с записью в _factories
        # write='no' анализ api_code 430 из поля json
        # row - строка из _factories

        # (69, 'Industrial and Commercial Bank of China', 'КИТАЙСКОЕ НАЗВАНИЕ', 
        # None, None, None, None, None, None, None, 'Спросите Вэй Сянь', 'неизвестен', 
        # datetime.date(2023, 2, 10), None, 'Янчжоу Баоринг Бюро Управления промышленности и торговли', 
        # None, 'Финансовая индустрия', 'Денежные финансовые услуги', None, None, None, None, None, None, 
        # None, '0', None, None, datetime.datetime(2023, 2, 10, 7, 53, 53, 792705), 1)

        # [(1, 'f_id', 'integer', "nextval('_factories_f_id_seq1'::regclass)"), (2, 'name_eng', 'character varying', None), 
        # (3, 'name_ch', 'character varying', None), (4, 'Оценка', 'character varying', None), 
        # (5, 'Плюсы', 'character varying', None), (6, 'Риски_высокие', 'character varying', None), 
        # (7, 'Риски_средние', 'character varying', None), (8, 'Риски_низкие', 'character varying', None), 
        # (9, 'Предупреждения', 'character varying', None), (10, 'creditcode', 'character varying', None), 
        # (11, 'Директор', 'character varying', None), (12, 'Статус', 'character varying', None), 
        # (13, 'Дата_рег', 'date', None), (14, 'Тип_собств', 'character varying', None), (15, 'Надзор', 'character varying', None), 
        # (16, 'Персонал', 'character varying', None), (17, 'Индустрия', 'character varying', None), 
        # (18, 'Подиндустрия', 'character varying', None), (19, 'Категория', 'character varying', None), 
        # (20, 'Адрес', 'character varying', None), (21, 'Телефон', 'character varying', None), 
        # (22, 'email', 'character varying', None), (23, 'website', 'character varying', None), 
        # (24, 'Род_деят', 'character varying', None), (25, 'Главк', 'character varying', None), 
        # (26, 'isonstock', 'character varying', None), (27, 'stocktype', 'character varying', None), 
        # (28, 'stocknum', 'character varying', None), (29, 'date_rec', 'timestamp without time zone', None), 
        # (30, 'update_grade', 'integer', None)]

        # список столбцов, которые не нужно обрабатывать
        columns_without_analysis = [
            'f_id', 'name_eng', 'name_ch', 'Оценка', 'Плюсы', 'Риски_высокие', 'Риски_средние', 'Риски_низкие',
            'Предупреждения', 'Надзор', 'Подиндустрия', 'Категория', 'stocktype', 'stocknum',   
            'date_rec', 'update_grade'
            ]
        for column, field_row in zip(self.columns, row):
            if column[1] not in columns_without_analysis:
                # столбец для обработки
                if column[1] == 'creditcode':
                    self.creditcode_analysis(field_row, row, write)
                if column[1] == 'Директор':
                    self.director_analysis(field_row, write)
                if column[1] == 'Статус':
                    self.status_analysis(field_row, write)
                if column[1] == 'Дата_рег':
                    self.start_date_analysis(field_row, write)
                if column[1] == 'Тип_собств':
                    self.property_type_analysis(field_row, write)
                if column[1] == 'Персонал':
                    self.personal_analysis(field_row, write)
                if column[1] == 'Индустрия':
                    self.industry_analysis(field_row, write)
                if column[1] == 'Адрес':
                    self.address_analysis(field_row, write)
                if column[1] == 'Телефон':
                    self.phone_analysis(field_row, write)
                if column[1] == 'email':
                    self.email_analysis(field_row, write)
                if column[1] == 'website':
                    self.website_analysis(field_row, write)
                if column[1] == 'Род_деят':
                    self.occupation_analysis(field_row, write)
                if column[1] == 'Главк':
                    self.affiliated_analysis(field_row, write)
                if column[1] == 'isonstock':
                    self.stock_analysis(field_row, write)

        # расчет оценки для других api_code. данные берутся из соответствующих таблиц
        if write == 'yes':
            if self.obj_db.check_table_is_exists(self.name_main_table_api_code['213']):
                self.calculated_grade_api_213(row)
            if self.obj_db.check_table_is_exists(self.name_main_table_api_code['736']):
                self.calculated_grade_api_736(row)
            if self.obj_db.check_table_is_exists(self.name_main_table_api_code['930']):
                self.calculated_grade_api_930(row)

            # запись данных в _factories
            #msg = "{} self.plus {}".format(NAME_CLASS + name_func, str(self.plus).encode('utf-8'))
            #self.obj_print.out_msg_error(msg)
            self.obj_db.set_value_cell('_factories', 'Плюсы', self.convert_dict_str(self.plus), 'name_eng', row[1])
            #msg = "{} self.high_risk {}".format(NAME_CLASS + name_func, str(self.high_risk).encode('utf-8'))
            #self.obj_print.out_msg_error(msg)
            self.obj_db.set_value_cell('_factories', 'Риски_высокие', self.convert_dict_str(self.high_risk), 'name_eng', row[1])
            #msg = "{} self.middle_risk {}".format(NAME_CLASS + name_func, str(self.self.middle_risk).encode('utf-8'))
            #self.obj_print.out_msg_error(msg)
            self.obj_db.set_value_cell('_factories', 'Риски_средние', self.convert_dict_str(self.middle_risk), 'name_eng', row[1])
            #msg = "{} self.low_risk {}".format(NAME_CLASS + name_func, str(self.self.low_risk).encode('utf-8'))
            #self.obj_print.out_msg_error(msg)
            self.obj_db.set_value_cell('_factories', 'Риски_низкие', self.convert_dict_str(self.low_risk), 'name_eng', row[1])
            #msg = "{} self.warnings {}".format(NAME_CLASS + name_func, str(self.self.warnings).encode('utf-8'))
            #self.obj_print.out_msg_error(msg)
            self.obj_db.set_value_cell('_factories', 'Предупреждения', self.convert_dict_str(self.warnings), 'name_eng', row[1])

            # получим комплексную оценку и занесем в _factories
            self.obj_db.set_value_cell('_factories', 'Оценка', self.get_complex_grade(), 'name_eng', row[1], 'int')

        else:
            # возврат данных для случая с расчетом оценки по данным в поле json для api_code 430
            plus = self.convert_dict_str(self.plus)
            high_risk = self.convert_dict_str(self.high_risk)
            middle_risk = self.convert_dict_str(self.middle_risk)
            low_risk = self.convert_dict_str(self.low_risk)
            warnings = self.convert_dict_str(self.warnings)
            complex_grade = self.get_complex_grade()
            len_dict = [len(self.plus), len(self.high_risk), len(self.middle_risk), len(self.low_risk), len(self.warnings)]
            return plus, high_risk, middle_risk, low_risk, warnings, complex_grade, len_dict

    def get_complex_grade(self):
        # веса
        weight_plus = 10
        weight_high_risk = -10
        weight_middle_risk = -5
        weight_low_risk = -2
        weight_warnings = -1
        begin_grade = 100
        complex_grade = begin_grade + \
                        len(self.plus) * weight_plus + \
                        len(self.high_risk) * weight_high_risk + \
                        len(self.middle_risk) * weight_middle_risk + \
                        len(self.low_risk) * weight_low_risk + \
                        len(self.warnings) * weight_warnings
        return (complex_grade)

    def creditcode_analysis(self, value, row, write='yes'):
        if not wac.control_empty_val(value):
            status = self.get_value_from_row(row, 'Статус')
            if not wac.control_empty_val(status) or status == 'неизвестен':
                self.high_risk[self.name_key['creditcode']] = 'Неизвестна причина отсутствия кредитного кода'
            elif wac.control_empty_val(status) and status == 'существующее, на этапе регистрации':
                self.middle_risk[self.name_key['creditcode']] = 'Предприятие на этапе регистрации'
            else:
                self.high_risk[self.name_key['creditcode']] = 'Неизвестна причина отсутствия кредитного кода'

    def director_analysis(self, value, write='yes'):
        if not wac.control_empty_val(value):
            self.high_risk[self.name_key['director']] = 'Неизвестен директор предприятия'
        else:
            # проверка на массового директора
            if write == 'yes':
                len_result, result = self.obj_db.select_simple('_factories', 'name_eng', 'Директор', value)
                if len_result > 1:
                    self.warnings[self.name_key['director']] = \
                        """{} является директором в {} компаниях: {}""".format(value, str(len_result), str(result))

    def status_analysis(self, value, write='yes'):
        if not wac.control_empty_val(value) or value == 'неизвестен':
            self.high_risk[self.name_key['status']] = 'Неизвестен статус предприятия (работающее или нет)'

    def property_type_analysis(self, value, write='yes'):
        if wac.control_empty_val(value) and wac.len_c(value) > 0:
            if re.search('госуд', value):
                self.plus[self.name_key['property']] = 'Компания с участием государства'

    def personal_analysis(self, value, write='yes'):
        name_func = '.personal_analysis'
        if not wac.control_empty_val(value):
            self.high_risk[self.name_key['personal']] = 'Неизвестно количество работающих на предприятии'
        else:
            # определим категорию предприятия по численности
            try:
                category, personal, prefix = self.personal_category(value)
            except Exception as exc:
                msg = "{} calling personal_category, value {}, error {}, ".format(NAME_CLASS+name_func, value, str(exc))
                self.obj_print.out_msg_error(msg)
                category = 'не удалось определить категорию'

            if category == 'не удалось определить категорию' or personal == -1:
                msg = "{} failed to determine the number for line {}".format(NAME_CLASS+name_func, value)
                self.obj_print.out_msg_error(msg)
            else:
                personal_description = "Количество работающих {} {} человек".format(prefix, str(personal)) 
                if category == 'micro':
                    self.middle_risk[self.name_key['personal']] = 'Компания относится к категории микро предприятий. ' +\
                    personal_description
                if category == 'small':
                    self.middle_risk[self.name_key['personal']] = 'Компания относится к категории малых предприятий. ' +\
                    personal_description
                if category == 'big':
                    self.plus[self.name_key['personal']] = 'Крупная компания. ' +\
                    personal_description

    def personal_category(self, s):
        name_func = "personal_category"
        def grades(pers):
            # критерии оценки размера бизнеса 
            # https://cyberleninka.ru/article/n/kriterii-malogo-i-srednego-biznesa-v-raznyh-stranah-i-ego-masshtaby-sravnitelnoe-issledovanie/viewer
            # < 10 - микро
            # >= 10 и < 50 - малый
            # >= 50 и < 250 - средний
            # >= 250 крупный
        
            micro = 9
            small_max = 49
            middle_max = 249

            if pers <= micro:
                return 'micro'
            elif pers > micro and pers <= small_max:
                return 'small'
            elif pers > small_max and pers <= middle_max:
                return 'middle'
            else:
                return 'big'
        
        msg_error = 'не удалось определить категорию'
        new_s = re.sub(' человек', '', s)
        if re.match('[0-9]', new_s):
            result = re.split('-', new_s)
            if wac.len_c(result) == 2:
                try:
                    max_personal = int(result[1]) - 1
                    return(grades(max_personal), max_personal, "до")
                except Exception as exc:
                    msg = "{} failed to determine the number for line {}, error {}".format(NAME_CLASS+name_func, s, str(exc))
                    self.obj_print.out_msg_error(msg)                    
                    return(msg_error, -1, "")
            else:
                msg = "{} failed to determine the number for line {}".format(NAME_CLASS+name_func, new_s)
                self.obj_print.out_msg_error(msg) 
                return(msg_error, -1, "")
        else:
            if re.match('Более', new_s):
                new_s_1 = re.sub('Более', '', new_s)
                new_s_2 = re.sub(' ', '', new_s_1)
                try:
                    max_personal = int(new_s_2) + 1
                    return(grades(max_personal), max_personal, "более")
                except Exception as exc:
                    msg = "{} failed to determine the number for line {}, error {}".format(NAME_CLASS+name_func, new_s, str(exc))
                    self.obj_print.out_msg_error(msg) 
                    return(msg_error, -1, "")
            elif re.match('Менее', new_s):
                new_s_1 = re.sub('Менее', '', new_s)
                new_s_2 = re.sub(' ', '', new_s_1)
                try:
                    min_personal = int(new_s_2) - 1
                    return(grades(min_personal), min_personal, "не менее")
                except Exception as exc:
                    msg = "{} failed to determine the number for line {}, error {}".format(NAME_CLASS+name_func, new_s, str(exc))
                    self.obj_print.out_msg_error(msg) 
                    return(msg_error, -1, "")
            else:
                return(msg_error, -1, "")

    def industry_analysis(self, value, write='yes'):
        if not wac.control_empty_val(value):
            self.high_risk[self.name_key['industry']] = 'Неизвестна индустрия'
        else:
            if re.search('Производство', value) or re.search('производство', value):
                self.plus[self.name_key['industry']] = 'Производственная компания'
            if re.search('научных исследований', value):
                self.plus[self.name_key['industry']] = 'Компания из индустрии научных исследований и технических услуг'
            if re.search('программного обеспечения', value):
                self.plus[self.name_key['industry']] = 'Компания из индустрии передачи информации, программного обеспечения и информационных технологий'
            if re.search('Оптовая и розничная индустрия', value):
                self.middle_risk[self.name_key['industry']] = 'Оптовая и розничная торговля'

    def address_analysis(self, value, write='yes'):
        if not wac.control_empty_val(value):
            self.high_risk[self.name_key['address']] = 'Неизвестен адрес предприятия'
        else:
            # проверка на массовый адрес
            if write == 'yes':
                len_result, result = self.obj_db.select_simple('_factories', 'name_eng', 'Адрес', value)
                if len_result > 1:
                    self.warnings[self.name_key['address']] = """{} является адресом для {} компаний: {}""".format(
                        value, str(len_result), str(result))

    def phone_analysis(self, value, write='yes'):
        if not wac.control_empty_val(value):
            self.low_risk[self.name_key['phone']] = 'Неизвестен телефон предприятия'
        else:
            # проверка на массовый телефон
            if write == 'yes':
                len_result, result = self.obj_db.select_simple('_factories', 'name_eng', 'Телефон', value)
                if len_result > 1:
                    self.warnings[self.name_key['phone']] = """{} является телефоном для {} компаний: {}""".format(
                        value, str(len_result), str(result))

    def email_analysis(self, value, write='yes'):
        if not wac.control_empty_val(value):
            self.high_risk[self.name_key['email']] = 'Неизвестна электронная почта предприятия'

    def website_analysis(self, value, write='yes'):
        if not wac.control_empty_val(value):
            self.warnings[self.name_key['website']] = 'Неизвестен сайт предприятия'

    def occupation_analysis(self, value, write='yes'):
        if not wac.control_empty_val(value):
            self.high_risk[self.name_key['occupation']] = 'Неизвестен род деятельности предприятия'
        else:
            if (re.search('Производство', value) or re.search('производство', value)) and \
                self.name_key['industry'] not in self.plus:
                self.plus[self.name_key['occupation']] = 'производственная компания'

    def affiliated_analysis(self, value, write='yes'):
        if wac.control_empty_val(value):
            self.warnings[self.name_key['affiliated']] = 'Компания является дочерним предприятием {}'.format(value)

    def stock_analysis(self, value, write='yes'):
        if wac.control_empty_val(value) and value == '1':
            self.plus[self.name_key['stock']] = 'Акции компании торгуются на бирже'

    def days_since_date(self, date_start):
        name_func = "days_since_date"
        if isinstance(date_start, datetime):
            today = datetime.now()
            delta = today - date_start
            return delta.days
        if isinstance(date_start, str):
            try:
                # date_obj = datetime.strptime(date_start, '%Y-%m-%d %H:%M:%S')
                date = parse(date_start)
                date_obj = datetime(date.year, date.month, date.day)
                time_diff = datetime.now() - date_obj
                return time_diff.days
            except Exception as exc:
                msg = "{} error convert date {} with type{} from str. code error {}".format(NAME_CLASS + name_func, 
                    str(date_start), type(date_start), str(exc))
                self.obj_print.out_msg_error(msg)
                return -1
        return -1

    def start_date_analysis(self, value, write='yes'):
        name_func = ".start_date_analysis"
        if wac.control_empty_val(value):
            days = self.days_since_date(value)
            msg = "{} date {} days {}".format(NAME_CLASS + name_func, value, days)
            self.obj_print.out_msg_info(msg)
            if days != -1:
                if days > 900:
                    self.plus[self.name_key['date_reg']] = 'Компания существует более трех лет'
                if days < 360:
                    self.high_risk[self.name_key['date_reg']] = 'Компания существует менее одного года'

    def get_value_from_row(self, row, name_column):
        name_func = 'get_value_from_row'
        # получить значение столбца из строки по названию столбца
        index = -1
        for i, column in enumerate(self.columns):
            if column[1] == name_column:
                index = i
                break
        if index != -1:
            try:
                return row[i]
            except Exception as exc:
                msg = "{} get column value from string by column name, error {}".format(NAME_CLASS+name_func, str(exc))
                self.obj_print.out_msg_error(msg) 
                return 0
        else:
            return 0

    def convert_dict_str(self, d):
        # конвертация словаря в строку 
        def clear_str(s):
            new_s = s
            if re.findall('\[\(\'', s):
                new_s = re.sub('\[\(\'', '', s)
                if re.findall('\'\,\)', new_s):
                    new_s = re.sub('\'\,\)', '', new_s)
                    if re.findall('\(\'', new_s):
                        new_s = re.sub('\(\'', '', new_s)
                        if re.findall('\]', new_s):
                            new_s = re.sub('\]', '', new_s)
            return new_s
        
        if wac.len_c(d) > 0:
            s = ''
            delimeter = '; '
            for k, v in d.items():
                s += k + ': '+ clear_str(v) + delimeter
            return s
        else:
            return ''

    def calculated_grade_api_213(self, row, write='yes'):
        name_factory = ''
        len_result = 0
        result = ()
        if write == 'yes': 
            # получаем строку из _factory_reports
            name_factory = self.get_value_from_row(row, 'name_eng')
            len_result, result = self.obj_db.select_simple('_factory_reports', '*', 'name_eng', name_factory)
        else:
            # строка в аргументе
            len_result = 1
            result = row

        if len_result:
            #result = result[0]
            # получим список колонок _factory_reports
            self.columns = self.obj_db.get_column_list("_factory_reports")
            all_assets = self.get_value_from_row(result, 'Итого_активы')            # активы
            owners_capital = self.get_value_from_row(result, 'Капитал_владельца')   # капитал владельца
            obligation = self.get_value_from_row(result, 'Обязательства')           # обязательства
            if (all_assets != 0 or owners_capital != 0) and obligation != 0:
                self.plus[self.name_key['report']] = 'Есть информация о годовом отчете предприятия'
                # расчет коэффициента текущей ликвидности
                if all_assets != 0 and obligation != 0:
                    K = all_assets / obligation
                else:
                    K = owners_capital / obligation
                if K < 1:
                    self.high_risk[self.name_key['report_param_1']] = \
                        'Коэффициент текущей ликвидности меньше 1. Обязательства предприятия превышают активы'
                elif K >= 1 and K < 2.8:
                    self.plus[self.name_key['report_param_1']] = \
                        'Коэффициент текущей ликвидности в диапазоне 1-2.8. У предприятия достаточный размер капитала'
                else:
                    self.middle_risk[self.name_key['report_param_1']] = \
                        'Коэффициент текущей ликвидности больше 2.8. У предприятия слишком высокий уровень накопленных запасов'

                netto_profit = self.get_value_from_row(result, 'Чистая_прибыль')
                if netto_profit < 0:
                    self.warnings[self.name_key['report_param_2']] = 'У предприятия убыток в годовом отчете ' + str(netto_profit)
            
                gov_support = self.get_value_from_row(result, 'Господдержка')
                if gov_support != 0:
                    self.plus[self.name_key['report_param_3']] = 'Предприятие пользуется господдержкой'

                if wac.control_empty_val(self.get_value_from_row(result, 'Иностранные_инвесторы')):
                    self.plus[self.name_key['report_param_4']] = 'У предприятия есть иностранные инвестиции'

                obligation_gov = self.get_value_from_row(result, 'Долг_накоп_страх')
                if obligation_gov != 0:
                    self.high_risk[self.name_key['report_param_5']] = \
                        'У предприятия есть долги по накопительному страхованию перед государством ' + str(obligation_gov)

                obligation_gov = self.get_value_from_row(result, 'Долг_безраб_страх')
                if obligation_gov != 0:
                    self.high_risk[self.name_key['report_param_6']] = \
                        'У предприятия есть долги по страхованию от безработицы перед государством ' + str(obligation_gov)

                obligation_gov = self.get_value_from_row(result, 'Долг_мед_страх')
                if obligation_gov != 0:
                    self.high_risk[self.name_key['report_param_7']] = \
                        'У предприятия есть долги по медицинскому страхованию перед государством ' + str(obligation_gov)

                obligation_gov = self.get_value_from_row(result, 'Долг_компенс_работникам')
                if obligation_gov != 0:
                    self.high_risk[self.name_key['report_param_8']] = \
                        'У предприятия есть долги по зарплате перед персоналом ' + str(obligation_gov)
            else:
                self.warnings[self.name_key['report']] = 'Есть информация о годовом отчете предприятия, но в нем недостаточно данных для расчета финансовых показателей'
        else:
            self.high_risk[self.name_key['report']] = 'Нет информации о годовом отчете предприятия'

        if write == 'no':
            plus = self.convert_dict_str(self.plus)
            high_risk = self.convert_dict_str(self.high_risk)
            middle_risk = self.convert_dict_str(self.middle_risk)
            low_risk = self.convert_dict_str(self.low_risk)
            warnings = self.convert_dict_str(self.warnings)
            complex_grade = self.get_complex_grade()
            len_dict = [len(self.plus), len(self.high_risk), len(self.middle_risk), len(self.low_risk), len(self.warnings)]
            return plus, high_risk, middle_risk, low_risk, warnings, complex_grade, len_dict

    def calculated_grade_api_736(self, row, write='yes'):
        name_factory = ''
        len_result = 0
        result = ()
        if write == 'yes': 
            # получаем строку из _factory_risk
            name_factory = self.get_value_from_row(row, 'name_eng')
            len_result, result = self.obj_db.select_simple('_factory_risk', '*', 'name_eng', name_factory)
        else:
            # строка в аргументе
            len_result = 1
            result = row

        if len_result:
            #result = result[0]
            # получим список колонок _factory_risk
            self.columns = self.obj_db.get_column_list("_factory_risk")
            # если есть информация по штрафам, добавим в warnings
            penalty = self.get_value_from_row(result, 'Штрафы')
            if wac.control_empty_val(penalty):
                self.warnings[self.name_key['penalty']] = "На компанию были наложены штрафы. {}".format(penalty)
            description_penalty = self.get_value_from_row(result, 'Описание')
            if wac.control_empty_val(description_penalty):
                self.warnings[self.name_key['description_penalty']] = "Подробности. {}".format(description_penalty)
            date_penalty = self.get_value_from_row(result, 'Даты')
            if wac.control_empty_val(date_penalty):
                self.warnings[self.name_key['date_penalty']] = "Компания была оштрафована. {}".format(date_penalty)
        else:
            self.warnings[self.name_key['risk']] = 'нет информации по корпоративным рискам'

        if write == 'no':
            plus = self.convert_dict_str(self.plus)
            high_risk = self.convert_dict_str(self.high_risk)
            middle_risk = self.convert_dict_str(self.middle_risk)
            low_risk = self.convert_dict_str(self.low_risk)
            warnings = self.convert_dict_str(self.warnings)
            complex_grade = self.get_complex_grade()
            len_dict = [len(self.plus), len(self.high_risk), len(self.middle_risk), len(self.low_risk), len(self.warnings)]
            return plus, high_risk, middle_risk, low_risk, warnings, complex_grade, len_dict

    def calculated_grade_api_930(self, row, write='yes'):
        name_factory = ''
        len_result = 0
        result = ()
        if write == 'yes': 
            # получаем строку из _factory_court_cases
            name_factory = self.get_value_from_row(row, 'name_eng')
            len_result, result = self.obj_db.select_simple('_factory_court_cases', '*', 'name_eng', name_factory)
        else:
            # строка в аргументе
            len_result = 1
            result = row

        if len_result:
            #result = result[0]
            # получим список колонок _factory_court_cases
            self.columns = self.obj_db.get_column_list("_factory_court_cases")
            # если есть информация по судебным делам, добавим в warnings
            court_deals = self.get_value_from_row(result, 'Всего_дел')
            if court_deals != 0:
                self.warnings[self.name_key['court_param_1']] = \
                    'Есть информация о {} судебных разбирательствах с участием компании'.format(
                        str(int(court_deals)))

            as_defendant = self.get_value_from_row(result, 'Как_ответчик')
            if as_defendant != 0:
                self.warnings[self.name_key['court_param_2']] = \
                    'В роли ответчика {}'.format(
                        str(int(as_defendant)))

            as_complainant = self.get_value_from_row(result, 'Как_истец')
            if as_complainant != 0:
                self.warnings[self.name_key['court_param_3']] = \
                    'В роли истца {}'.format(
                        str(int(as_complainant)))

            reasons = self.get_value_from_row(result, 'Причины_судов')
            if wac.control_empty_val(reasons):
                self.warnings[self.name_key['court_param_4']] = reasons
        else:
            self.warnings[self.name_key['court']] = 'нет информации по судебной практике предприятия'

        if write == 'no':
            plus = self.convert_dict_str(self.plus)
            high_risk = self.convert_dict_str(self.high_risk)
            middle_risk = self.convert_dict_str(self.middle_risk)
            low_risk = self.convert_dict_str(self.low_risk)
            warnings = self.convert_dict_str(self.warnings)
            complex_grade = self.get_complex_grade()
            len_dict = [len(self.plus), len(self.high_risk), len(self.middle_risk), len(self.low_risk), len(self.warnings)]
            return plus, high_risk, middle_risk, low_risk, warnings, complex_grade, len_dict

    def calculated_grade_api_1003(self, row, write='yes'):
        name_factory = ''
        len_result = 0
        result = ()
        if write == 'yes': 
            # получаем строку из _factory_beneficial
            name_factory = self.get_value_from_row(row, 'name_eng')
            len_result, result = self.obj_db.select_simple('_factory_beneficial', '*', 'name_eng', name_factory)
        else:
            # строка в аргументе
            len_result = 1
            result = row

        if len_result:
            #result = result[0]
            # получим список колонок _factory_beneficial
            self.columns = self.obj_db.get_column_list("_factory_beneficial")
            # если нет информации о бенефициарах, добавим в warnings
            have_benefit = self.get_value_from_row(result, 'Бенефициары')
            if have_benefit == "неизвестны":
                remark = self.get_value_from_row(result, 'Примечание')
                if remark is not None and remark != '':
                    self.warnings[self.name_key['benefit']] = \
                        "Неизвестны бенефициары предприятия по следующим причинам: {}".format(remark)
                else:
                    self.warnings[self.name_key['benefit']] = "Неизвестны бенефициары предприятия"
            else:
                # список бенефициаров
                benefit_list = self.get_value_from_row(result, 'Бенефициар')
                # доли бенефициаров
                stock_percent = self.get_value_from_row(result, 'Доля_бенефициара')
                # тип владения
                benefit_type = self.get_value_from_row(result, 'Тип_владения')
                # позиция в компании
                position = self.get_value_from_row(result, 'Позиция')
                data = ''
                if wac.len_c(benefit_list) != 0:
                    data += 'Бенефициар' + ': ' + benefit_list
                if wac.len_c(stock_percent) != 0:
                    data += ', ' + 'Доля_бенефициара' + ': ' + stock_percent
                if wac.len_c(benefit_type) != 0:
                    data += ', ' + 'Тип_владения' + ': ' + benefit_type
                if wac.len_c(position) != 0:
                    data += ', ' + 'Позиция' + ': ' + position
                if wac.len_c(data) != 0:
                    self.plus[self.name_key['benefit']] = \
                        'Известна информация о бенефициарах предприятия: {}'.format(data)

        if write == 'no':
            plus = self.convert_dict_str(self.plus)
            high_risk = self.convert_dict_str(self.high_risk)
            middle_risk = self.convert_dict_str(self.middle_risk)
            low_risk = self.convert_dict_str(self.low_risk)
            warnings = self.convert_dict_str(self.warnings)
            complex_grade = self.get_complex_grade()
            len_dict = [len(self.plus), len(self.high_risk), len(self.middle_risk), len(self.low_risk), len(self.warnings)]
            return plus, high_risk, middle_risk, low_risk, warnings, complex_grade, len_dict

    def calculated_grade_api_701(self, row, write='yes'):
            name_factory = ''
            len_result = 0
            result = ()
            if write == 'yes': 
                # получаем строку из _factory_news
                name_factory = self.get_value_from_row(row, 'name_eng')
                len_result, result = self.obj_db.select_simple('_factory_news', '*', 'name_eng', name_factory)
            else:
                # строка в аргументе
                len_result = 1
                result = row

            if len_result:
                #result = result[0]
                # получим список колонок _factory_news
                self.columns = self.obj_db.get_column_list("_factory_news")
                # если есть данные 
                percent_positive = self.get_value_from_row(result, 'процент_положительных')
                if percent_positive:
                    if percent_positive > 50:
                        self.plus[self.name_key['public_opinion']] = \
                            "Процент позитивных новостей о компании - {}".format(str(percent_positive))
                    else:
                        self.high_risk[self.name_key['public_opinion']] = \
                            "Процент позитивных новостей о компании меньше 50 и составляет {}".format(str(percent_positive))

            if write == 'no':
                plus = self.convert_dict_str(self.plus)
                high_risk = self.convert_dict_str(self.high_risk)
                middle_risk = self.convert_dict_str(self.middle_risk)
                low_risk = self.convert_dict_str(self.low_risk)
                warnings = self.convert_dict_str(self.warnings)
                complex_grade = self.get_complex_grade()
                len_dict = [len(self.plus), len(self.high_risk), len(self.middle_risk), len(self.low_risk), len(self.warnings)]
                return plus, high_risk, middle_risk, low_risk, warnings, complex_grade, len_dict

    def calculated_grade_api_410(self, row, write='yes'):
            name_factory = ''
            len_result = 0
            result = ()
            if write == 'yes': 
                # получаем строку из _factory_short
                name_factory = self.get_value_from_row(row, 'name_eng')
                len_result, result = self.obj_db.select_simple('_factories_short', '*', 'name_eng', name_factory)
            else:
                # строка в аргументе
                len_result = 1
                result = row

            if len_result:
                #result = result[0]
                # получим список колонок _factory_short
                self.columns = self.obj_db.get_column_list("_factories_short")

            for column, field_row in zip(self.columns, row):
                if column[1] == 'creditcode':
                    self.creditcode_analysis(field_row, row, write)
                if column[1] == 'Директор':
                    self.director_analysis(field_row, write)
                if column[1] == 'Статус':
                    self.status_analysis(field_row, write)
                if column[1] == 'Тип_собств':
                    self.property_type_analysis(field_row, write)
                if column[1] == 'Адрес':
                    self.address_analysis(field_row, write)
                if column[1] == 'Дата_рег':
                    self.start_date_analysis(field_row, write)
                if column[1] == 'Род_деят':
                    self.occupation_analysis(field_row, write)
                if column[1] == 'isonstock':
                    self.stock_analysis(field_row, write)

            if write == 'no':
                plus = self.convert_dict_str(self.plus)
                high_risk = self.convert_dict_str(self.high_risk)
                middle_risk = self.convert_dict_str(self.middle_risk)
                low_risk = self.convert_dict_str(self.low_risk)
                warnings = self.convert_dict_str(self.warnings)
                complex_grade = self.get_complex_grade()
                len_dict = [len(self.plus), len(self.high_risk), len(self.middle_risk), len(self.low_risk), len(self.warnings)]
                return plus, high_risk, middle_risk, low_risk, warnings, complex_grade, len_dict
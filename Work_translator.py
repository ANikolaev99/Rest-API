# coding: utf-8

import json
import requests
from requests.exceptions import InvalidURL
from requests.exceptions import HTTPError
from requests.exceptions import Timeout
from requests.exceptions import ConnectionError
from requests.exceptions import TooManyRedirects
import time
import sys
from Print_msg import Print_msg
from Send_message import SendMessage as sms

NAME_CLASS = "Work_api_code"

class Work_translator(object):
    def __init__(self, obj_db):
        self.obj_db = obj_db
        # параметры url lingva-translate
        self.url_api = "https://lingva.ml/api/v1/"
        self.lang_from = "auto"
        self.obj_print = Print_msg(0)

    def get_url_api(self, lang_to:str, text:str):
        name_func = "get_url_api"
        try:
            url = self.url_api +  self.lang_from + "/" + lang_to + "/" + text
            return url
        except Exception as exc:
            msg = "{} Error create url {}".format(NAME_CLASS + name_func, str(exc))
            self.obj_print.out_msg_error(msg)
            return ''

    def translator(self, sentence: str, key: str = ""):
        name_func = ".translator"

        def get_sentence_translation(s):
            name_sub_func = ".get_word_translation"
            # кол-во попыток на 1 запрос
            max_count_req = 3
            # минимальная задержка между запросами
            delay_ = 5
            timeout = 120
            # формирование url для запроса
            url_requests = self.get_url_api("ru", s)
            if url_requests == '':
                return s

            try:
                response = requests.get(url_requests, timeout=timeout)
                if response.status_code == 200:
                    result = response.json()['translation']
                    # убираем кавычки из текста
                    result_correct = result.replace('\'', '')
                    # правка перевода
                    result_after_correct = self.correct_translator(result_correct)
                    return result_after_correct
                else:
                    msg = "{} Error for request {}. Status server response code {}".format(
                        NAME_CLASS + name_func + name_sub_func, url_requests, response.status_code)
                    self.obj_print.out_msg_error(msg)
                    # пробуем сделать еще count_req запросов
                    i = 1
                    for _ in range(max_count_req):
                        # на каждом новом запросе будем увеличивать задержку
                        time.sleep(delay_ * i)
                        i += 1
                        response = requests.get(url_requests, timeout=timeout)
                        if response.status_code == 200:
                            result = response.json()['translation']
                            # убираем кавычки из текста
                            result_correct = result.replace('\'', '')
                            return result_correct
                        else:
                            msg = "{} {}. Error. Status server response code {}, waiting for {} sec".format(
                                NAME_CLASS + name_func + name_sub_func, i-1, response.status_code, (i-1)*delay_)
                            self.obj_print.out_msg_error(msg)
                    # если дошли сюда, ответ 200 так и не был получен
                    return s
            except InvalidURL as exc:
                msg = "{} Exception {} for request {}".format(NAME_CLASS + name_func + name_sub_func, str(exc), url_requests)
                self.obj_print.out_msg_error(msg)
                return s
            except Timeout as exc:
                msg = "{} Exception {} for request {}".format(NAME_CLASS + name_func + name_sub_func, str(exc), url_requests)
                self.obj_print.out_msg_error(msg)
                return s
            except ConnectionError as exc:
                msg = "{} Exception {} for request {}".format(NAME_CLASS + name_func + name_sub_func, str(exc), url_requests)
                self.obj_print.out_msg_error(msg)
                return s
            except HTTPError as exc:
                msg = "{} Exception {} for request {}".format(NAME_CLASS + name_func + name_sub_func, str(exc), url_requests)
                self.obj_print.out_msg_error(msg)
                return s
            except TooManyRedirects as exc:
                msg = "{} Exception {} for request {}".format(NAME_CLASS + name_func + name_sub_func, str(exc), url_requests)
                self.obj_print.out_msg_error(msg)
                return s
            except Exception as exc:
                msg = "{} Exception {} for request {}".format(NAME_CLASS + name_func + name_sub_func, str(exc), url_requests)
                self.obj_print.out_msg_error(msg)
                return s

        name_translate_table = "_dict_ch_ru"
        if key == "Status" and self.obj_db.check_table_is_exists(name_translate_table):
            #print("key = {}, sentence = {}".format(key, sentence.encode('utf-8')))
            # пробуем получить перевод из таблицы
            result = self.obj_db.get_value_cell(name_translate_table, "word_ru_correct", "word_ch", sentence)
            if result != 0:
                #print("translate from table {}".format(result.encode('utf-8')))
                return result
            else:
                # новое предложение. делаем перевод, заносим в таблицу, отправляем сообщение
                result = get_sentence_translation(sentence)
                new_sentence = {}
                new_sentence['word_ch'] = sentence
                new_sentence['word_ru'] = result
                if key == "Status":
                    new_sentence['word_ru_correct'] = "неизвестен"
                else:
                    new_sentence['word_ru_correct'] = ""
                self.obj_db.insert_row_into_table(name_translate_table, new_sentence)
                msg_subj = "{} new sentence"
                msg = "{} new sentence {} for key {}".format(NAME_CLASS + name_func, sentence, key) 
                sms.send_email(msg_subj, msg) 
                self.obj_print.out_msg_error(msg)
                if key == "Status":
                    return "неизвестен"
                else:
                    return ""
        else:
            return get_sentence_translation(sentence)


        ## кол-во попыток на 1 запрос
        #max_count_req = 3
        ## минимальная задержка между запросами
        #delay_ = 5
        #timeout = 120
        ## формирование url для запроса
        #url_requests = self.get_url_api("ru", s)
        #if url_requests == '':
        #    return s

        #try:
        #    response = requests.get(url_requests, timeout=timeout)
        #    if response.status_code == 200:
        #        result = response.json()['translation']
        #        # убираем кавычки из текста
        #        result_correct = result.replace('\'', '')
        #        # правка перевода
        #        result_after_correct = self.correct_translator(result_correct)
        #        return result_after_correct
        #    else:
        #        msg = "{} Error for request {}. Status server response code {}".format(
        #            NAME_CLASS + name_func, url_requests, response.status_code)
        #        self.obj_print.out_msg_error(msg)
        #        # пробуем сделать еще count_req запросов
        #        i = 1
        #        for _ in range(max_count_req):
        #            # на каждом новом запросе будем увеличивать задержку
        #            time.sleep(delay_ * i)
        #            i += 1
        #            response = requests.get(url_requests, timeout=timeout)
        #            if response.status_code == 200:
        #                result = response.json()['translation']
        #                # убираем кавычки из текста
        #                result_correct = result.replace('\'', '')
        #                # правка перевода
        #                result_after_correct = self.correct_translator(result_correct)
        #                return result_after_correct
        #            else:
        #                msg = "{} {}. Error. Status server response code {}, waiting for {} sec".format(
        #                    NAME_CLASS + name_func, i-1, response.status_code, (i-1)*delay_)
        #                self.obj_print.out_msg_error(msg)
        #        # если дошли сюда, ответ 200 так и не был получен
        #        return s
        #except InvalidURL as exc:
        #    msg = "{} Exception {} for request {}".format(NAME_CLASS + name_func, str(exc), url_requests)
        #    self.obj_print.out_msg_error(msg)
        #    return s
        #except Timeout as exc:
        #    msg = "{} Exception {} for request {}".format(NAME_CLASS + name_func, str(exc), url_requests)
        #    self.obj_print.out_msg_error(msg)
        #    return s
        #except ConnectionError as exc:
        #    msg = "{} Exception {} for request {}".format(NAME_CLASS + name_func, str(exc), url_requests)
        #    self.obj_print.out_msg_error(msg)
        #    return s
        #except HTTPError as exc:
        #    msg = "{} Exception {} for request {}".format(NAME_CLASS + name_func, str(exc), url_requests)
        #    self.obj_print.out_msg_error(msg)
        #    return s
        #except TooManyRedirects as exc:
        #    msg = "{} Exception {} for request {}".format(NAME_CLASS + name_func, str(exc), url_requests)
        #    self.obj_print.out_msg_error(msg)
        #    return s
        #except Exception as exc:
        #    msg = "{} Exception {} for request {}".format(NAME_CLASS + name_func, str(exc), url_requests)
        #    self.obj_print.out_msg_error(msg)
        #    return s

    def translator_eng(self, s: str):
        name_func = ".translator"
        # кол-во попыток на 1 запрос
        max_count_req = 3
        # минимальная задержка между запросами
        delay_ = 5
        timeout = 120
        # формирование url для запроса
        url_requests = self.get_url_api("en", s)
        if url_requests == '':
            return s

        try:
            response = requests.get(url_requests, timeout=timeout)
            if response.status_code == 200:
                result = response.json()['translation']
                # убираем кавычки из текста
                result_correct = result.replace('\'', '')
                # правка английского перевода не предусмотрена
                return result_correct
            else:
                msg = "{} Error for request {}. Status server response code {}".format(
                    NAME_CLASS + name_func, url_requests, response.status_code)
                self.obj_print.out_msg_error(msg)
                # пробуем сделать еще count_req запросов
                i = 1
                for _ in range(max_count_req):
                    # на каждом новом запросе будем увеличивать задержку
                    time.sleep(delay_ * i)
                    i += 1
                    response = requests.get(url_requests, timeout=timeout)
                    if response.status_code == 200:
                        result = response.json()['translation']
                        # убираем кавычки из текста
                        result_correct = result.replace('\'', '')
                        # правка английского перевода не предусмотрена
                        return result_correct
                    else:
                        msg = "{} {}. Error. Status server response code {}, waiting for {} sec".format(
                            NAME_CLASS + name_func, i-1, response.status_code, (i-1)*delay_)
                        self.obj_print.out_msg_error(msg)
                # если дошли сюда, ответ 200 так и не был получен
                return s
        except InvalidURL as exc:
            msg = "{} Exception {} for request {}".format(NAME_CLASS + name_func, str(exc), url_requests)
            self.obj_print.out_msg_error(msg)
            return s
        except Timeout as exc:
            msg = "{} Exception {} for request {}".format(NAME_CLASS + name_func, str(exc), url_requests)
            self.obj_print.out_msg_error(msg)
            return s
        except ConnectionError as exc:
            msg = "{} Exception {} for request {}".format(NAME_CLASS + name_func, str(exc), url_requests)
            self.obj_print.out_msg_error(msg)
            return s
        except HTTPError as exc:
            msg = "{} Exception {} for request {}".format(NAME_CLASS + name_func, str(exc), url_requests)
            self.obj_print.out_msg_error(msg)
            return s
        except TooManyRedirects as exc:
            msg = "{} Exception {} for request {}".format(NAME_CLASS + name_func, str(exc), url_requests)
            self.obj_print.out_msg_error(msg)
            return s
        except Exception as exc:
            msg = "{} Exception {} for request {}".format(NAME_CLASS + name_func, str(exc), url_requests)
            self.obj_print.out_msg_error(msg)
            return s

    def correct_translator(self, s):
        # корректировка перевода. 
        # если есть фраза в таблице _correct_translator, используем новое значение из таблицы
        name_table = '_correct_translator'
        if self.obj_db.check_table_is_exists(name_table):
            s_correct = self.obj_db.get_value_cell(name_table, 'перевод_коррект', 'перевод', s)
            if s_correct == 0:
                return s
            else:
                return s_correct
        else:
            return s
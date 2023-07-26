# coding: utf-8

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText  
from Print_msg import Print_msg

NAME_CLASS = "SendMessage"

class SendMessage:
    addr_from: str = "admin@guru-net.biz"  # Отправитель
    addr_to = addr_from  # Получатель
    password: str = "Hteriandr_77"  # Пароль ящика отправителя
    server_name: str = "smtp.yandex.ru" # Имя сервера
    port_name: int = 465 # Порт сервера
    obj_print = Print_msg(0)

    @staticmethod
    def send_email(msg_subj: str, msg_text: str):
        name_func = '.send_email'
        msg = MIMEMultipart()  # Создаем сообщение
        msg['From'] = SendMessage.addr_from
        msg['To'] = SendMessage.addr_to
        msg['Subject'] = msg_subj  # Тема сообщения
        body = msg_text  # Текст сообщения
        msg.attach(MIMEText(body, 'plain'))  # Добавляем в сообщение текст
        try:
            server = smtplib.SMTP_SSL(SendMessage.server_name, SendMessage.port_name)  # Создаем объект SMTP (Yandex)
            server.login(SendMessage.addr_from, SendMessage.password)  # Получаем доступ
            server.send_message(msg)  # Отправляем сообщение
            server.quit() 
        except Exception as exc:
            msg = "{} Failed to send message {}. Error: {}".format(NAME_CLASS + name_func, msg_text.encode('utf-8'), str(exc))
            SendMessage.obj_print.out_msg_error(msg)


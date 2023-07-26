# coding: utf-8

import sys
import shared

NAME_CLASS = "Print_msg"

class Print_msg():
    def __init__(self, message):
        self.print_comment = shared.print_comment
        self.message = message  # обьект поле вывода интерфейса

    def print_msg(self, msg):
        if self.print_comment == 1 or self.print_comment == 3:
            print(str(msg).encode('utf-8'), file=sys.stdout, flush=True)

    def out_msg_info(self, msg):
        msg_ = 'Info: ' + msg
        self.print_msg(msg_)
        # if self.print_comment >= 2 and self.message != 0:
        #    self.message.insert(END, msg_ + '\n')

    def out_msg_warning(self, msg):
        msg_ = 'Warning: ' + msg
        self.print_msg(msg_)
        # if self.print_comment >= 2 and self.message != 0:
        #    self.message.insert(END, msg_ + '\n')

    def out_msg_error(self, msg):
        msg_ = 'Error: ' + msg
        self.print_msg(msg_)
        # if self.print_comment >= 2 and self.message != 0:
        #    self.message.insert(END, msg_ + '\n')

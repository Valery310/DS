from datetime import time
import smtplib
import telebot
import logging
from airflow.models import Variable

log = logging.getLogger(__name__)

bot = telebot.TeleBot(
    Variable.get("TeleBot_access_token"), threaded=False)
# bot = telebot.TeleBot(
#    '5283108704:AAEu24lGphVLmEkfmeK1zTqGUgLQI1OFluA', threaded=False)
telegramm_chat_ids = list()
telegramm_chat_ids.append(225303319)

"""Надо будет токен и логопасы перенести в переменные в airflow https://marclamberti.com/blog/variables-with-apache-airflow/#How_to_set_a_variable_in_Airflow"""


def send_alert(message, head):
    message = "Ошибка в работе скрипта! " + str(message)
    send_telegram(message, head)
    #send_email(message, head)


def send_telegram(message, head):
    for chat in telegramm_chat_ids:
        try:
            bot.send_message(
                chat, head + " : " + message)
        except Exception as e:
            log.error("Ошибка отправки сообщения в телеграмм: " + str(e))
    """while True:
        try:
            bot.polling(none_stop=True)

        except Exception as e:
            log.error(e)
            time.sleep(15)"""


@ bot.message_handler(commands=["start"])
def start(m, res=False):
    bot.send_message(
        m.chat.id, 'Подписка на рассылку создана. ID чата: ' + str(m.chat.id))
    telegramm_chat_ids.append(m.chat.id)
    telegramm_chat_ids = set(telegramm_chat_ids)


# bot.polling()


def send_email(message, head):
    try:
        server = smtplib.SMTP_SSL('smtp.yandex.com', 465)
        # server.ehlo("v.kryukov@agro-man.ru")
        sender = Variable.get("mail_login_secret")
        sender = 'v.kryukov@agro-man.ru'
        msg = 'From: %s\r\nTo: %s\r\nContent-Type: text/plain; charset="utf-8"\r\nSubject: %s\r\n\r\n' % (
            sender, sender, head)
        msg += message
        password = Variable.get("mail_password")
        #password = "3sQCDV6V"
        server.login(sender, password)
        server.sendmail(sender,
                        sender, msg.encode('utf8'))
        server.quit()
    except smtplib.SMTPException:
        log.error("Error: unable to send email")

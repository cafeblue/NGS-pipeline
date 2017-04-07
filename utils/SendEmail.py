#! /bin/env python3
import smtplib
import socket
from email.mime.text import MIMEText

def SendEmail(Subject, Receiptors, Content):
    msg= MIMEText(Content)
    msg['Subject'] = Subject
    msg['From'] = 'notice@' + socket.gethostname()
    msg['To'] = Receiptors

    s = smtplib.SMTP('127.0.0.1')
    s.send_message(msg)
    s.quit()

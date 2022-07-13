import os
import json
# for email
import smtplib
import mimetypes
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
import email.encoders as Encoders
import sys
import traceback


sender_username =  os.getenv('SENDER_EMAIL','')
sender_password =  os.getenv('SENDER_PASSWORD','')
receiver_emails =   os.getenv('RECEIVER_EMAIL', '').split(',')
host = os.getenv('smtp_host', 'smtp.gmail.com')
smtp_port  = os.getenv('smtp_port', 587)
useconnect = True if os.getenv('smtp_useconnect', 'false') == 'true' else False

def send_email(targets = receiver_emails,
               username = sender_username,
               password = sender_password,
               SUBJECT = "ทดสอบส่งจดหมาย", 
               host = host,
               body = None,
               port = smtp_port,
               useconnect = useconnect,
               zipfile = None):
    # https://gist.github.com/blazindragon/7781087

    assert len(targets) != 0, 'please define RECEIVER_EMAIL= '
    assert host is not None and host != '', 'please define smtp_host= ' 
    assert username != '', 'please define sender_username'

    msg = MIMEMultipart()
    msg['Subject'] = SUBJECT
    msg['From'] = username
    msg['To'] = ', '.join(targets)

    if zipfile is not None:
      part = MIMEBase("application", "octet-stream")
      part.set_payload(open(zipfile, "rb").read())
      Encoders.encode_base64(part)
      part.add_header('Content-Disposition', 'attachment; filename=\" {} \"'.format(zipfile))
      msg.attach(part)

    if body is not None:
      msg.attach(MIMEText(body, "plain"))

    
    mailserver = smtplib.SMTP(host, port)
    if useconnect:
      print('connecting...')
      mailserver.connect(host, port)
  
    mailserver.ehlo()
    mailserver.starttls()
    mailserver.ehlo()

    if password != '':
      mailserver.login(username, password)
    mailserver.sendmail(username, targets, msg.as_string())
    mailserver.quit()

if __name__ == '__main__':
  sender_username = os.getenv('SENDER_EMAIL','')
  sender_password = os.getenv('SENDER_PASSWORD','')
  receiver_emails = os.getenv('RECEIVER_EMAIL', '')
  smtp_port  = os.getenv('smtp_port', 587)
  useconnect = True if os.getenv('smtp_useconnect', 'false') == 'true' else False
  receiver_emails = receiver_emails.split(',')
 
  host = os.getenv('smtp_email')
  assert len(receiver_emails) != 0, 'please define RECEIVER_EMAIL = '
  assert host is not None, 'please define host = ?'

  print(f'Send email to {receiver_emails}')
  print(f'host: {host}:{smtp_port}')
  print(f'Connect SMTP bool: {useconnect}')
  try:
    send_email( 
                targets = receiver_emails, 
                username = sender_username,
                password = sender_password,
                host = host, 
                port = smtp_port,
                SUBJECT = 'หัวเรื่องทดสอบส่งจดหมาย',
                body = 'ทดสอบส่ง', 
                useconnect = useconnect
              )
    print('Send Email: Pass')
    sys.exit(0)
  except Exception as err:
    print(err)
    raise Exception(traceback.format_exc())




  

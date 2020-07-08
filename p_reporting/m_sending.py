import re
import os
import smtplib
import MimeWriter
import mimetools
from io import StringIO

from dotenv import load_dotenv
load_dotenv()

### UNFINISHED

"""def send(archive_to_send, period):

    filename = archive_to_send

    # Checking email and pass in environment
    if not 'emailPassword' in os.environ:
        raise ValueError ('You should pass a email password')

    gmail_user = os.environ['email']
    gmail_password = os.environ['emailPassword']

    # Connecting to gmail servers
    try:
        server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        server.ehlo()
        server.login(gmail_user, gmail_password)
        print('· Connected to gmail servers')

    except:
        print('Someting went wrong..')

    # Getting CC mail
    to = input('Receiver mail CC: ')
    while not re.match(r'[^@]+@[^@]+\.[^@]+', to):
        print('Invalid email. Try again...')
        to = input('Receiver mail CC: ')

    html = f'Automated email. Pipeline results'
    msg = ,IMEultipart('mixed')

    msg['Subject'] = f'Pipelines Project Results {period}'
    msg['From'] = gmail_user
    msg['To'] = to 

    HTML_Contents = MIMEText(html, 'html')
    fo =open(filename, 'rb')"""
    attach = email.mime.application.MIMEApplication(filename)

    msg.attach(attach)
    msg.attach(HTML_Contents)
    server.sendmail(msg['From'], msg['To'], msg.as_string())
    print(f'· Email sent')


def create_mail_content(zip_file_to_attach):

    zipFile_path= f'./../data/results/{zip_file_to_attach}.zip'

    # Getting destinary credentials
    to = input('Receiver mail CC: ')
    while not re.match(r'[^@]+@[^@]+\.[^@]+', to):
        print('Invalid email. Try again...')
        to = input('Receiver mail CC: ')

    # Getting sender credentials
    if not 'emailPassword' in os.environ:
        raise ValueError ('You should pass a email password')

    gmail_user = os.environ['email']
    gmail_password = os.environ['emailPassword']

    # Writing content in the email
    subject = f'Pipelines Project Results'

    #Constructing email
    message = StringIO.StringIO()
    email_msg = MimeWriter.MimeWriter(message)
    email_msg.addheader('To', to)
    email_msg.addheader('From', sender)
    email_msg.addheader('Subject', subject)
    email_msg.addheader('MIME-Version', '1.0')

    email_msg.startmultipartbody('mixed')

    part = email_msg.nextpart()
    body = part.startbody('text/plain')
    part.flushheaders()
    body.write(text)

    filename = os.path.basename(zipFile_path)
    ftype, encoding = 'application/zip', None

    part = email_msg.nextpart()
    part.addheader('Content-Transfer-Encoding', encoding)
    body = part.startbody("%s; name=%s" % (ftype, filename))
    mimetools.encode(open(zipFile_path, 'rb'), body, encoding)

    email_msg.lastpart()

    email_text = message.getvalue()

    #sending email
    smtp = smtplib.SMTP(SERVER, PORT)
    smtp.login(USER, PASSWORD)
    smtp.sendmail(sender, to, email_text)
    smtp.quit()


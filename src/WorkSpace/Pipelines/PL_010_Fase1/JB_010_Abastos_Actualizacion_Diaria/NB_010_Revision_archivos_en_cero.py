

import smtplib
from email.mime.text import MIMEText
import re


source_dir = "/mnt/adlsabastosprscus/abastos/raw/"

zero_byte_files = []

def find_zero_byte_files(src):
    for f in dbutils.fs.ls(src):
        if f.isDir():
            find_zero_byte_files(f.path)  # Recursividad en subdirectorios
        elif f.size == 0:
            local_path = f.path.replace("dbfs:", "/dbfs")
            mod_time = os.path.getmtime(local_path)
            current_time = time.time()
            if current_time - mod_time <= 2 * 24 * 60 * 60:
                match = re.search(r'[^/]+$', f.path)
                if match:
                    filename = match.group(0)
                    zero_byte_files.append(filename)

find_zero_byte_files(source_dir)


email_body = "Lista de archivos de 0 bytes:\n\n" + "\n".join(zero_byte_files)

smtp_server = "smtp-mail.outlook.com"
smtp_port = 587
smtp_username = "power.support@desc.com"
smtp_password = "15.Que-1346."
sender = "power.support@desc.com"
receiver = ["framirez@seidoranalytics.com","jasilva@seidoranalytics.com","genaro.luna.navarro@accenture.com", "alejandra.rocha@desc.com", "yesenia.hrodriguez@desc.com"]
subject = "Lista de archivos de 0 bytes SAP - Abastos"


msg = MIMEText(email_body)
msg['Subject'] = subject
msg['From'] = sender
for r in receiver:
    msg['To'] = r
    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        server.login(smtp_username, smtp_password)
        server.sendmail(sender, r, msg.as_string())


source_dir = "/mnt/adlsabastosprscus/abastos/raw/"

zero_byte_files = []

def find_zero_byte_files(src):
    for f in dbutils.fs.ls(src):
        if f.isDir():
            find_zero_byte_files(f.path)  # Recursividad en subdirectorios
        elif f.size == 0:
            match = re.search(r'[^/]+$', f.path)
            if match:
                filename = match.group(0)
                zero_byte_files.append(filename)

find_zero_byte_files(source_dir)


(zero_byte_files)
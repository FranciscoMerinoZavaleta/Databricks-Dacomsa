# Databricks notebook source
# MAGIC %run /Shared/FASE1_INTEGRACION/includes

# COMMAND ----------

import smtplib
from email.mime.text import MIMEText
import re

# COMMAND ----------

# Directorio de origen
source_dir = "/mnt/adls-dac-data-bi-pr-scus/raw/"

# Lista para almacenar los archivos de 0 bytes
zero_byte_files = []

# Función para encontrar archivos de 0 bytes
def find_zero_byte_files(src):
    for f in dbutils.fs.ls(src):
        if f.isDir():
            find_zero_byte_files(f.path)  # Recursividad en subdirectorios
        elif f.size == 0:
            # Convertir la ruta de 'dbfs:' a '/dbfs' para usar con os.path
            local_path = f.path.replace("dbfs:", "/dbfs")
            # Obtener el tiempo de modificación del archivo
            mod_time = os.path.getmtime(local_path)
            # Obtener el tiempo actual
            current_time = time.time()
            # Verificar si el archivo fue modificado en los últimos dos días (en segundos)
            if current_time - mod_time <= 2 * 24 * 60 * 60:
                match = re.search(r'[^/]+$', f.path)
                if match:
                    filename = match.group(0)
                    zero_byte_files.append(filename)

# Ejecutar la función
find_zero_byte_files(source_dir)

# COMMAND ----------

zero_byte_files

# COMMAND ----------

if len(zero_byte_files) > 0:    
    # Crear el cuerpo del correo electrónico
    email_body = "Lista de archivos de 0 bytes:\n\n" + "\n".join(zero_byte_files)

    # Configuración del correo electrónico
    smtp_server = "smtp-mail.outlook.com"
    smtp_port = 587
    smtp_username = "power.support@desc.com"
    smtp_password = "15.Que-1346."
    sender = "power.support@desc.com"
    receiver = ["framirez@seidoranalytics.com","jasilva@seidoranalytics.com","genaro.luna.navarro@accenture.com", "alejandra.rocha@desc.com", "yesenia.hrodriguez@desc.com"]
    subject = "Lista de archivos de 0 bytes SAP - Refacciones"

    # Crear el mensaje MIME

    msg = MIMEText(email_body)
    msg['Subject'] = subject
    msg['From'] = sender
    # Enviar el correo electrónico
    for r in receiver:
        msg['To'] = r
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(smtp_username, smtp_password)
            server.sendmail(sender, r, msg.as_string())
else:
    dbutils.notebook.exit("No hay archivos de 0 bytes")
import mysql.connector
import datetime, time
import os, shutil
import glob

date_today = datetime.datetime.today().strftime('%Y%m%d')
base_dir = os.path.dirname(os.path.dirname(__file__))
script_dir = base_dir + '/scripts/'
S3_raw_dir = base_dir + '/data/s3/' + date_today + '/'
bill_data_dir = base_dir + '/data/Billing/' + date_today + '/'
log_dir = base_dir + '/logs/' + date_today
S3_region_lookup = base_dir + '/data/Lookup/' + date_today + '/' + 'S3_bucket_properties.csv'
S3_access_lookup = base_dir + '/data/Lookup/' + date_today + '/' + 'Public_access.csv'
file_suffix = time.strftime("%d%m%y%H%M%S")


# Create Today's LOG directory
try:
    if not os.path.exists(log_dir):
        os.mkdir(log_dir)
    else:
        logfile = open(log_dir + "/" + 'DB_LOAD_' + file_suffix + ".log", "a")
        logfile.write(
            time.strftime("%d-%m-%y  %H:%M:%S") + " " + "INFO : Today's folder already exists")
        logfile.write("\n")
        logfile.close()
except Exception as e:
    logfile = open(log_dir + file_suffix + ".log", "a")
    logfile.write(time.strftime("%d-%m-%y  %H:%M:%S") + " " + "ERROR : ")
    logfile.write(str(e))
    logfile.write("\n")
    logfile.close()
    exit()

# Define Logging Function

try:
    def logging(input_text, input_value):
        logfile = open(log_dir + "/" + 'DB_LOAD_' + file_suffix + ".log", "a")
        logfile.write(time.strftime("%d-%m-%y  %H:%M:%S") + " " + "INFO : " + input_text)
        logfile.write(input_value)
        logfile.write("\n")
        logfile.close()
except Exception as e:
    logfile = open(log_dir + "/" + 'DB_LOAD_' + file_suffix + ".log", "a")
    logfile.write(time.strftime("%d-%m-%y  %H:%M:%S") + " " + "ERROR : ")
    logfile.write(str(e))
    logfile.write("\n")
    logfile.close()
    exit()

# Connecting to DB

db_connection = {
    'user': 'root',
    'password': 'InfoSolution@999',
    'host': '10.10.5.165',
    'database': 'Cloud_Monitor',
    'allow_local_infile': 'True'}
try:
    connection = mysql.connector.connect(**db_connection)
    logging("DB Connection: ", str(connection))
    cursor = connection.cursor()
    logging("DB cursor: ", str(cursor))
except Exception as e:
    logfile = open(log_dir + "/" + 'DB_LOAD_' + file_suffix + ".log", "a")
    logfile.write(time.strftime("%d-%m-%y  %H:%M:%S") + " " + "ERROR : ")
    logfile.write(str(e))
    logfile.write("\n")
    logfile.close()
    exit()


# Load Region and Versioning Lookup Function
def load_reg_lkp():
    logging("********** Loading S3 Region and Versioning lookup data into DB ************", '')
    try:
        filename = S3_region_lookup
        logging("Reading File: ", str(S3_region_lookup))
        new_fname = filename.replace("\\", '/')
        logging("FULL PATH: ", str(new_fname))
        query_load_met = r""" LOAD DATA LOCAL INFILE """ + "'" + str(new_fname) + "'" + r""" REPLACE INTO TABLE CM_S3_BUCKET_PROP_LKP FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '"' LINES TERMINATED BY '\n' IGNORE 1 LINES; """
        logging("SQL QUERY: ", str(query_load_met))
        cursor.execute(query_load_met)
        connection.commit()
    except Exception as e:
        logfile = open(log_dir + "/" + 'DB_LOAD_' + file_suffix + ".log", "a")
        logfile.write(time.strftime("%d-%m-%y  %H:%M:%S") + " " + "ERROR : ")
        logfile.write(str(e))
        logfile.write("\n")
        logfile.close()
        exit()


# Load Metrics Function
def load_access_lkp():
    logging("********** Loading S3 public access lookup data into DB ************", '')
    try:
        filename = S3_access_lookup
        logging("Reading File: ", str(S3_access_lookup))
        new_fname = filename.replace("\\", '/')
        logging("FULL PATH: ", str(new_fname))
        query_load_met = r""" LOAD DATA LOCAL INFILE """ + "'" + str(new_fname) + "'" + r""" REPLACE INTO TABLE CM_S3_PUBLIC_ACCESS_LKP FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '"' LINES TERMINATED BY '\n' IGNORE 1 LINES; """
        logging("SQL QUERY: ", str(query_load_met))
        cursor.execute(query_load_met)
        connection.commit()
    except Exception as e:
        logfile = open(log_dir + "/" + 'DB_LOAD_' + file_suffix + ".log", "a")
        logfile.write(time.strftime("%d-%m-%y  %H:%M:%S") + " " + "ERROR : ")
        logfile.write(str(e))
        logfile.write("\n")
        logfile.close()
        exit()


# Load Metrics Function
def load_metrics():
    logging("********** Loading S3 metrics data into DB ************", '')
    try:
        for dirpath, _, filenames in os.walk(S3_raw_dir):
            for f in filenames:
                filename = os.path.abspath(os.path.join(dirpath, f))
                logging("Reading File: ", str(filename))
                new_fname = filename.replace("\\", '/')
                logging("FULL PATH: ", str(new_fname))
                query_load_met = r""" LOAD DATA LOCAL INFILE """ + "'" + str(
                    new_fname) + "'" + r""" REPLACE INTO TABLE raw_s3_metrics FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '"' LINES TERMINATED BY '\n' IGNORE 1 LINES; """
                logging("SQL QUERY: ", str(query_load_met))
                cursor.execute(query_load_met)
        connection.commit()
    except Exception as e:
        logfile = open(log_dir + "/" + 'DB_LOAD_' + file_suffix + ".log", "a")
        logfile.write(time.strftime("%d-%m-%y  %H:%M:%S") + " " + "ERROR : ")
        logfile.write(str(e))
        logfile.write("\n")
        logfile.close()
        exit()


# Load Billing Function
def load_billing():
    logging("********** Loading S3 Billing data into DB ************", '')
    try:
        for dirpath, _, filenames in os.walk(bill_data_dir):
            for f in filenames:
                filename = os.path.abspath(os.path.join(dirpath, f))
                logging("Reading File: ", str(filename))
                new_fname = filename.replace("\\", '/')
                logging("FULL PATH: ", str(new_fname))
                query_load_bill = r""" LOAD DATA LOCAL INFILE """ + "'" + str(
                    new_fname) + "'" + r""" REPLACE INTO TABLE raw_s3_billing FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '"' LINES TERMINATED BY '\n' IGNORE 1 LINES; """
                logging("SQL QUERY: ", str(query_load_bill))
                cursor.execute(query_load_bill)
        connection.commit()
    except Exception as e:
        logfile = open(log_dir + "/" + 'DB_LOAD_' + file_suffix + ".log", "a")
        logfile.write(time.strftime("%d-%m-%y  %H:%M:%S") + " " + "ERROR : ")
        logfile.write(str(e))
        logfile.write("\n")
        logfile.close()
        exit()


# Load Lookup data
try:
    logging("START Loading regional lookup data into DB ", '')
    load_reg_lkp()
    logging("END Loading regional lookup data ", '')
    logging("START Loading Public Access lookup data into DB ", '')
    load_access_lkp()
    logging("END Loading Public Access lookup data ", '')
except Exception as e:
    logfile = open(log_dir + "/" + 'DB_LOAD_' + file_suffix + ".log", "a")
    logfile.write(time.strftime("%d-%m-%y  %H:%M:%S") + " " + "ERROR : ")
    logfile.write(str(e))
    logfile.write("\n")
    logfile.close()
    exit()


# Archiving old Billing data:

try:
    logging("START Archiving Billing Data ", '')
    query_archive_bil = r""" INSERT IGNORE INTO archive_raw_s3_billing (select * from raw_s3_billing); """
    logging("SQL QUERY: ", str(query_archive_bil))
    cursor.execute(query_archive_bil)
    connection.commit()
    logging("END Archiving Billing Data ", '')
    logging("START Truncating raw_s3_billing ", '')
    truncate_bil = r""" TRUNCATE TABLE raw_s3_billing; """
    logging("SQL QUERY: ", str(truncate_bil))
    cursor.execute(truncate_bil)
    connection.commit()
    logging("TRUNCATED TABLE: ", 'raw_s3_billing')
    load_billing()
except Exception as e:
    logfile = open(log_dir + "/" + 'DB_LOAD_' + file_suffix + ".log", "a")
    logfile.write(time.strftime("%d-%m-%y  %H:%M:%S") + " " + "ERROR : ")
    logfile.write(str(e))
    logfile.write("\n")
    logfile.close()
    exit()

# Archiving old Metrics data:

try:
    logging("START Archiving Metrics Data ", '')
    query_archive_met = r""" INSERT IGNORE INTO archive_raw_s3_metrics (select * from raw_s3_metrics); """
    logging("SQL QUERY: ", str(query_archive_met))
    cursor.execute(query_archive_met)
    connection.commit()
    logging("END Archiving Billing Data ", '')
    logging("START Truncating raw_s3_metrics ", '')
    truncate_met = r""" TRUNCATE TABLE raw_s3_metrics; """
    logging("SQL QUERY: ", str(truncate_met))
    cursor.execute(truncate_met)
    connection.commit()
    logging("TRUNCATED TABLE: ", 'raw_s3_metrics')
    load_metrics()
except Exception as e:
    logfile = open(log_dir + "/" + 'DB_LOAD_' + file_suffix + ".log", "a")
    logfile.write(time.strftime("%d-%m-%y  %H:%M:%S") + " " + "ERROR : ")
    logfile.write(str(e))
    logfile.write("\n")
    logfile.close()
    exit()
	

# Loading final data into the facts table

try:
    logging("START Loading final Data ", '')
    query = r""" INSERT IGNORE INTO CM_S3_STATS (select * from VW_FINAL); """
    logging("SQL QUERY: ", str(query))
    cursor.execute(query)
    connection.commit()
    logging("Loading final Data ", '')
except Exception as e:
    logfile = open(log_dir + "/" + 'DB_LOAD_' + file_suffix + ".log", "a")
    logfile.write(time.strftime("%d-%m-%y  %H:%M:%S") + " " + "ERROR : ")
    logfile.write(str(e))
    logfile.write("\n")
    logfile.close()
    exit()
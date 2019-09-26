import boto3
import time
import os
import datetime

session = boto3.session.Session()
s3_res = session.resource('s3')
s3_cli = session.client('s3')

file_suffix=time.strftime("%d%m%y%H%M%S")
date_today = datetime.datetime.today().strftime('%Y%m%d')
base_dir = os.path.dirname(os.path.dirname(__file__))
script_dir = base_dir+'/scripts/'
S3_lookup_dir = base_dir+'/data/Lookup/'+date_today
log_dir = base_dir+'/logs/'



logfile = open(log_dir+"Public_access_"+file_suffix+".log","a")
logfile.write(time.strftime("%d-%m-%y  %H:%M:%S")+" "+"INFO : Starting to GET PUBLIC ACCESS for all buckets")
logfile.write("\n")
logfile.close()

try:
    if not os.path.exists(S3_lookup_dir):
        os.mkdir(S3_lookup_dir)
    else:
        logfile = open(log_dir+"bucket_properties_"+file_suffix+".log","a")
        logfile.write(
            time.strftime("%d-%m-%y  %H:%M:%S") + " " + "INFO : Today's lookup folder already exists")
        logfile.write("\n")
        logfile.close()

except Exception as e:
    logfile = open(log_dir+"bucket_properties_"+file_suffix+".log","a")
    logfile.write(time.strftime("%d-%m-%y  %H:%M:%S")+" "+"ERROR : ")
    logfile.write(str(e))
    logfile.write("\n")
    logfile.close()
    exit()

S3_lookup_dir=S3_lookup_dir+'/'
#########################################################################
## create session
########################################################################
Upgrade_logfile=open(log_dir+"Public_access_"+file_suffix+".log","a")
Upgrade_logfile.write(time.strftime("%d-%m-%y  %H:%M:%S")+" "+"INFO : creating Session")
Upgrade_logfile.write("\n")
Upgrade_logfile.close()

try:
    session = boto3.session.Session()
except Exception as e:
            Upgrade_logfile=open(log_dir+"Public_access_"+file_suffix+".log","a")
            Upgrade_logfile.write(time.strftime("%d-%m-%y  %H:%M:%S")+" "+"ERROR : ")
            Upgrade_logfile.write(str(e))
            Upgrade_logfile.write("\n")
            Upgrade_logfile.close()
            exit()


#########################################################################
## create resource object
########################################################################
Upgrade_logfile=open(log_dir+"Public_access_"+file_suffix+".log","a")
Upgrade_logfile.write(time.strftime("%d-%m-%y  %H:%M:%S")+" "+"INFO : creating Resource")
Upgrade_logfile.write("\n")
Upgrade_logfile.close()


try:
    s3_res = session.resource('s3')
except Exception as e:
            Upgrade_logfile=open(log_dir+"Public_access_"+file_suffix+".log","a")
            Upgrade_logfile.write(time.strftime("%d-%m-%y  %H:%M:%S")+" "+"ERROR : ")
            Upgrade_logfile.write(str(e))
            Upgrade_logfile.write("\n")
            Upgrade_logfile.close()
            exit()


#########################################################################
## create Client object
########################################################################
Upgrade_logfile=open(log_dir+"Public_access_"+file_suffix+".log","a")
Upgrade_logfile.write(time.strftime("%d-%m-%y  %H:%M:%S")+" "+"INFO : creating Client")
Upgrade_logfile.write("\n")
Upgrade_logfile.close()

try:
    s3_cli = session.client('s3')
except Exception as e:
            Upgrade_logfile=open(log_dir+"Public_access_"+file_suffix+".log","a")
            Upgrade_logfile.write(time.strftime("%d-%m-%y  %H:%M:%S")+" "+"ERROR : ")
            Upgrade_logfile.write(str(e))
            Upgrade_logfile.write("\n")
            Upgrade_logfile.close()
            exit()


#########################################################################
## Start Job for S3
########################################################################
Upgrade_logfile=open(log_dir+"get_region_"+file_suffix+".log","a")
Upgrade_logfile.write(time.strftime("%d-%m-%y  %H:%M:%S")+" "+"INFO : Start Get PUBLIC ACCESS Job for S3")
Upgrade_logfile.write("\n")
Upgrade_logfile.close()

#########################################################################
## Creating header row
########################################################################
Upgrade_logfile=open(log_dir+"Public_access_"+file_suffix+".log","a")
Upgrade_logfile.write(time.strftime("%d-%m-%y  %H:%M:%S")+" "+"INFO : creating Header row")
Upgrade_logfile.write("\n")
Upgrade_logfile.close()


header = "Bucket,Public_access"
region_data_file = open(S3_lookup_dir+"Public_access"+".csv", "a")
region_data_file.write(header)
region_data_file.write("\n")
region_data_file.close()


try:
    for each_bucket_r in s3_res.buckets.all():
        try:
            access_block=s3_cli.get_public_access_block(Bucket=each_bucket_r.name)
            if str(access_block['PublicAccessBlockConfiguration']['IgnorePublicAcls'])=='False' and str(access_block['PublicAccessBlockConfiguration']['BlockPublicPolicy'])=='False' and str(access_block['PublicAccessBlockConfiguration']['BlockPublicAcls'])=='False' and str(access_block['PublicAccessBlockConfiguration']['BlockPublicAcls'])=='False':
                output = each_bucket_r.name + "," + 'ENABLED'
                region_data_file = open(S3_lookup_dir + "Public_access" + ".csv", "a")
                region_data_file.write(output)
                region_data_file.write("\n")
            else:
                output = each_bucket_r.name + "," + 'DISABLED'
                region_data_file = open(S3_lookup_dir+"Public_access"+".csv", "a")
                region_data_file.write(output)
                region_data_file.write("\n")
        except Exception as e:
            if str(e).find('NoSuchPublicAccessBlockConfiguration'):
                Upgrade_logfile = open(log_dir + "Public_access" + file_suffix + ".log", "a")
                Upgrade_logfile.write(time.strftime("%d-%m-%y  %H:%M:%S") + " " + "THIS IS A DEFAULT SYSTEM BUCKET - NO ACCESS INFORMATION AVAILABLE ")
                Upgrade_logfile.write("\n")
                Upgrade_logfile.write(time.strftime("%d-%m-%y  %H:%M:%S") + " " + "ERROR : ")
                Upgrade_logfile.write(str(e))
                Upgrade_logfile.write("\n")
                Upgrade_logfile.close()
                continue

except Exception as err:
    Upgrade_logfile = open(log_dir + "bucket_properties_" + file_suffix + ".log", "a")
    Upgrade_logfile.write(time.strftime("%d-%m-%y  %H:%M:%S") + " " + "ERROR : ")
    Upgrade_logfile.write(str(err))
    Upgrade_logfile.write("\n")
    Upgrade_logfile.close()
    exit()


#########################################################################
## Completed
########################################################################
Upgrade_logfile=open(log_dir+"bucket_properties_"+file_suffix+".log","a")
Upgrade_logfile.write(time.strftime("%d-%m-%y  %H:%M:%S")+" "+"INFO : COMPLETED")
Upgrade_logfile.write("\n")
Upgrade_logfile.close()

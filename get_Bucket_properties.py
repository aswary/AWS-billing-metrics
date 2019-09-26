import boto3
import time
import os
import datetime

file_suffix=time.strftime("%d%m%y%H%M%S")
date_today = datetime.datetime.today().strftime('%Y%m%d')
base_dir = os.path.dirname(os.path.dirname(__file__))
script_dir = base_dir+'/scripts/'
S3_lookup_dir = base_dir+'/data/Lookup/'+date_today
log_dir = base_dir+'/logs/'

logfile = open(log_dir+"bucket_properties_"+file_suffix+".log","a")
logfile.write(time.strftime("%d-%m-%y  %H:%M:%S")+" "+"INFO : Starting to GET Region and versioning for all buckets")
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
Upgrade_logfile=open(log_dir+"bucket_properties_"+file_suffix+".log","a")
Upgrade_logfile.write(time.strftime("%d-%m-%y  %H:%M:%S")+" "+"INFO : creating Session")
Upgrade_logfile.write("\n")
Upgrade_logfile.close()

try:
    session = boto3.session.Session()
except Exception as e:
            Upgrade_logfile=open(log_dir+"bucket_properties_"+file_suffix+".log","a")
            Upgrade_logfile.write(time.strftime("%d-%m-%y  %H:%M:%S")+" "+"ERROR : ")
            Upgrade_logfile.write(str(e))
            Upgrade_logfile.write("\n")
            Upgrade_logfile.close()
            exit()


#########################################################################
## create resource object
########################################################################
Upgrade_logfile=open(log_dir+"bucket_properties_"+file_suffix+".log","a")
Upgrade_logfile.write(time.strftime("%d-%m-%y  %H:%M:%S")+" "+"INFO : creating Resource")
Upgrade_logfile.write("\n")
Upgrade_logfile.close()


try:
    s3_res = session.resource('s3')
except Exception as e:
            Upgrade_logfile=open(log_dir+"bucket_properties_"+file_suffix+".log","a")
            Upgrade_logfile.write(time.strftime("%d-%m-%y  %H:%M:%S")+" "+"ERROR : ")
            Upgrade_logfile.write(str(e))
            Upgrade_logfile.write("\n")
            Upgrade_logfile.close()
            exit()


#########################################################################
## create Client object
########################################################################
Upgrade_logfile=open(log_dir+"bucket_properties_"+file_suffix+".log","a")
Upgrade_logfile.write(time.strftime("%d-%m-%y  %H:%M:%S")+" "+"INFO : creating Client")
Upgrade_logfile.write("\n")
Upgrade_logfile.close()

try:
    s3_cli = session.client('s3')
except Exception as e:
            Upgrade_logfile=open(log_dir+"bucket_properties_"+file_suffix+".log","a")
            Upgrade_logfile.write(time.strftime("%d-%m-%y  %H:%M:%S")+" "+"ERROR : ")
            Upgrade_logfile.write(str(e))
            Upgrade_logfile.write("\n")
            Upgrade_logfile.close()
            exit()


#########################################################################
## Start Job for S3
########################################################################
Upgrade_logfile=open(log_dir+"get_region_"+file_suffix+".log","a")
Upgrade_logfile.write(time.strftime("%d-%m-%y  %H:%M:%S")+" "+"INFO : Start Get Region and versioning Job for S3")
Upgrade_logfile.write("\n")
Upgrade_logfile.close()

#########################################################################
## Creating header row
########################################################################
Upgrade_logfile=open(log_dir+"bucket_properties_"+file_suffix+".log","a")
Upgrade_logfile.write(time.strftime("%d-%m-%y  %H:%M:%S")+" "+"INFO : creating Header row")
Upgrade_logfile.write("\n")
Upgrade_logfile.close()


header = "Bucket,versioning,region"
region_data_file = open(S3_lookup_dir+"S3_bucket_properties"+".csv", "a")
region_data_file.write(header)
region_data_file.write("\n")
region_data_file.close()


try:
    for each_bucket_r in s3_res.buckets.all():
        try:
            resp_vers = s3_cli.get_bucket_versioning(Bucket=each_bucket_r.name)['Status']
            resp_reg = s3_cli.head_bucket(Bucket=each_bucket_r.name)['ResponseMetadata']['HTTPHeaders']['x-amz-bucket-region']
            output = each_bucket_r.name + "," + str(resp_vers) + "," + str(resp_reg)
            region_data_file = open(S3_lookup_dir+"S3_bucket_properties"+".csv", "a")
            region_data_file.write(output)
            region_data_file.write("\n")
        except KeyError:
            resp_reg = s3_cli.head_bucket(Bucket=each_bucket_r.name)['ResponseMetadata']['HTTPHeaders']['x-amz-bucket-region']
            output = each_bucket_r.name + "," + "Disabled" + "," + str(resp_reg)
            region_data_file = open(S3_lookup_dir + "S3_bucket_properties" + ".csv", "a")
            region_data_file.write(output)
            region_data_file.write("\n")
        except Exception as e:
            Upgrade_logfile = open(log_dir + "bucket_properties_" + file_suffix + ".log", "a")
            Upgrade_logfile.write(time.strftime("%d-%m-%y  %H:%M:%S") + " " + "ERROR : ")
            Upgrade_logfile.write(str(e))
            Upgrade_logfile.write("\n")
            Upgrade_logfile.close()
            exit()

except Exception as e:
            Upgrade_logfile=open(log_dir+"bucket_properties_"+file_suffix+".log","a")
            Upgrade_logfile.write(time.strftime("%d-%m-%y  %H:%M:%S")+" "+"ERROR : ")
            Upgrade_logfile.write(str(e))
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

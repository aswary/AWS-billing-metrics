import boto3
import datetime
import time
import pandas as pd
from datetime import date
import os


date_today = datetime.datetime.today().strftime('%Y%m%d')
base_dir = os.path.dirname(os.path.dirname(__file__))
script_dir = base_dir+'/scripts/'
S3_data_dir = base_dir+'/data/S3/'+date_today
bill_data_dir = base_dir+'/data/Billing/'+date_today
log_dir = base_dir+'/logs/'
file_suffix = time.strftime("%d%m%y%H%M%S")


logfile = open(log_dir + file_suffix+".log","a")
logfile.write(time.strftime("%d-%m-%y  %H:%M:%S")+" "+"INFO : Creating Today's Data directory for S3 metrics")
logfile.write("\n")
logfile.close()

try:
    if not os.path.exists(S3_data_dir):
        os.mkdir(S3_data_dir)
    else:
        logfile = open(log_dir + file_suffix + ".log", "a")
        logfile.write(
            time.strftime("%d-%m-%y  %H:%M:%S") + " " + "INFO : Today's folder already exists")
        logfile.write("\n")
        logfile.close()

except Exception as e:
    logfile = open(log_dir + file_suffix+".log","a")
    logfile.write(time.strftime("%d-%m-%y  %H:%M:%S")+" "+"ERROR : ")
    logfile.write(str(e))
    logfile.write("\n")
    logfile.close()
    exit()

logfile = open(log_dir + file_suffix+".log","a")
logfile.write(time.strftime("%d-%m-%y  %H:%M:%S")+" "+"INFO : Creating Today's Data directory for billing metrics")
logfile.write("\n")
logfile.close()

try:
    if not os.path.exists(bill_data_dir):
        os.mkdir(bill_data_dir)
    else:
        logfile = open(log_dir + file_suffix + ".log", "a")
        logfile.write(
            time.strftime("%d-%m-%y  %H:%M:%S") + " " + "INFO : Today's folder already exists")
        logfile.write("\n")
        logfile.close()
except Exception as e:
    logfile = open(log_dir + file_suffix+".log","a")
    logfile.write(time.strftime("%d-%m-%y  %H:%M:%S")+" "+"ERROR : ")
    logfile.write(str(e))
    logfile.write("\n")
    logfile.close()
    exit()
# job_cloudwatch_s3 = 'S3_couldwatch_metrics_'
# job_cloudwatch_billing = 'billing_couldwatch_metrics_'


#########################################################################
# create session
########################################################################
# create session for root user OR "default" profile
try:
    logfile = open(log_dir + file_suffix + ".log", "a")
    logfile.write(time.strftime("%d-%m-%y  %H:%M:%S") + " " + "INFO : creating Session")
    logfile.write("\n")
    logfile.close()

    session = boto3.session.Session()
except Exception as e:
    logfile = open(log_dir + file_suffix+".log","a")
    logfile.write(time.strftime("%d-%m-%y  %H:%M:%S")+" "+"ERROR : ")
    logfile.write(str(e))
    logfile.write("\n")
    logfile.close()
    exit()


#########################################################################
# create Resource
########################################################################

# create S3 resource object
try:
    logfile = open(log_dir + file_suffix + ".log", "a")
    logfile.write(time.strftime("%d-%m-%y  %H:%M:%S") + " " + "INFO : creating S3 Resource")
    logfile.write("\n")
    logfile.close()

    s3_res = session.resource('s3')
except Exception as e:
    logfile = open(log_dir + file_suffix + ".log","a")
    logfile.write(time.strftime("%d-%m-%y  %H:%M:%S")+" "+"ERROR : ")
    logfile.write(str(e))
    logfile.write("\n")
    logfile.close()
    exit()



#########################################################################
# create S3 Client
########################################################################
logfile=open(log_dir + file_suffix + ".log","a")
logfile.write(time.strftime("%d-%m-%y  %H:%M:%S")+" "+"INFO : creating S3 Client")
logfile.write("\n")
logfile.close()

# create S3 client object
try:
    s3_cli = session.client('s3')
except Exception as e:
    logfile = open(log_dir+file_suffix+".log","a")
    logfile.write(time.strftime("%d-%m-%y  %H:%M:%S")+" "+"ERROR : ")
    logfile.write(str(e))
    logfile.write("\n")
    logfile.close()
    exit()


#########################################################################
# create Cloudwatch Client
########################################################################

logfile = open(log_dir+file_suffix+".log","a")
logfile.write(time.strftime("%d-%m-%y  %H:%M:%S")+" "+"INFO : creating Cloudwatch Client")
logfile.write("\n")
logfile.close()

# create IAM user client object
try:
    cloudwatch_cli = boto3.client('cloudwatch')
except Exception as e:
    logfile = open(log_dir+file_suffix+".log","a")
    logfile.write(time.strftime("%d-%m-%y  %H:%M:%S")+" "+"ERROR : ")
    logfile.write(str(e))
    logfile.write("\n")
    logfile.close()
    exit()

# #####################################################


try:
    today = date.today()
    day_brf_yesterday = today + datetime.timedelta(-1)
    midnight_today = datetime.datetime.combine(today, datetime.datetime.min.time())
    midnight_yesterday= datetime.datetime.combine(day_brf_yesterday, datetime.datetime.min.time())

    logfile=open(log_dir+file_suffix+".log","a")
    logfile.write(time.strftime("%d-%m-%y  %H:%M:%S")+" "+"INFO : START DATE: " + str(midnight_yesterday))
    logfile.write("\n")
    logfile.write(time.strftime("%d-%m-%y  %H:%M:%S")+" "+"INFO : END DATE: " + str(midnight_today))
    logfile.write("\n")
    logfile.close()
except Exception as e:
    logfile = open(log_dir+file_suffix+".log","a")
    logfile.write(time.strftime("%d-%m-%y  %H:%M:%S")+" "+"ERROR : ")
    logfile.write(str(e))
    logfile.write("\n")
    logfile.close()
    exit()


# #####################################################


try:
    def logging(input_text, input_value):
        logfile = open(log_dir + file_suffix + ".log", "a")
        logfile.write(time.strftime("%d-%m-%y  %H:%M:%S") + " " + "INFO : " + input_text)
        logfile.write(input_value)
        logfile.write("\n")
        logfile.close()
except Exception as e:
    logfile = open(log_dir+file_suffix+".log","a")
    logfile.write(time.strftime("%d-%m-%y  %H:%M:%S")+" "+"ERROR : ")
    logfile.write(str(e))
    logfile.write("\n")
    logfile.close()
    exit()

# #####################################################
# #####################################################


try:
    def get_metrics_stats_s3(name_space, metric_name, dimension, s3_dimension_arg):
        stats = cloudwatch_cli.get_metric_statistics(
            Namespace=name_space,
            MetricName=metric_name,
            Dimensions=dimension,
            StartTime=midnight_yesterday,
            EndTime=midnight_today,
            Period=86400,
            Statistics=['Average', 'Sum', 'Minimum', 'Maximum']
        )
        logging("This is the get_metrics_statistics_response: ", str(stats))
        metricname = stats['Label']
        logging("This is Metric Name: ", str(metricname))
        count = 0
        for list_datapoint in stats['Datapoints']:
            timestamp = list_datapoint['Timestamp']
            avg = list_datapoint['Average']
            maximum = list_datapoint['Maximum']
            minimum = list_datapoint['Minimum']
            summation = list_datapoint['Sum']
            unit = list_datapoint['Unit']
            s3_datapoint_values = timestamp, avg, maximum, minimum, summation, unit
            output = metricname, ','.join(map(str, s3_dimension_arg)), ','.join(map(str, s3_datapoint_values))
            final_output=','.join(map(str, output))
            s3_data_file = open(S3_data_dir + "/" + file_suffix + ".csv", "a")
            s3_data_file.write("\n")
            s3_data_file.write(final_output)

            count = count + 1
            logfile = open(log_dir + file_suffix + ".log", "a")
            logfile.write(
                time.strftime("%d-%m-%y  %H:%M:%S") + " " + "INFO : Dumping data Line # " + str(count) + " into DATA FILE")
            logfile.write("\n")
            logfile.close()

        logfile = open(log_dir + file_suffix + ".log", "a")
        logfile.write(time.strftime("%d-%m-%y  %H:%M:%S") + " " + "INFO : *********** Data Dump completed for metric : " + metricname + "  ***********")
        logfile.write("\n")
        logfile.close()

except Exception as e:
    logfile = open(log_dir+file_suffix+".log","a")
    logfile.write(time.strftime("%d-%m-%y  %H:%M:%S")+" "+"ERROR : ")
    logfile.write(str(e))
    logfile.write("\n")
    logfile.close()
    exit()

# #####################################################
# #####################################################


try:
    def get_metrics_stats_billing(name_space, metric_name, dimension, billing_dimension_arg):
        stats = cloudwatch_cli.get_metric_statistics(
            Namespace=name_space,
            MetricName=metric_name,
            Dimensions=dimension,
            StartTime=midnight_yesterday,
            EndTime=midnight_today,
            Period=86400,
            Statistics=['Average', 'Sum', 'Minimum', 'Maximum']
        )
        metricname = stats['Label']
        logging("This is the get_metrics_statistics_response: ", str(stats))
        logging("This is Metric Name: ", str(metricname))
        count = 0
        for list_datapoint in stats['Datapoints']:
            timestamp = list_datapoint['Timestamp']
            avg = list_datapoint['Average']
            maximum = list_datapoint['Maximum']
            minimum = list_datapoint['Minimum']
            summation = list_datapoint['Sum']
            unit = list_datapoint['Unit']
            bill_datapoint_values = timestamp, avg, maximum, minimum, summation, unit
            output = metricname, ','.join(map(str, billing_dimension_arg)), ','.join(map(str, bill_datapoint_values))
            final_output = ','.join(map(str, output))
            bill_data_file = open(bill_data_dir + "/" + file_suffix + ".csv", "a")
            bill_data_file.write("\n")
            bill_data_file.write(final_output)
            count = count + 1
            logfile = open(log_dir + file_suffix + ".log", "a")
            logfile.write(
                time.strftime("%d-%m-%y  %H:%M:%S") + " " + "INFO : Dumping data Line # " + str(count) + " into DATA FILE")
            logfile.write("\n")
            logfile.close()

        logfile = open(log_dir + file_suffix + ".log", "a")
        logfile.write(time.strftime("%d-%m-%y  %H:%M:%S") + " " + "INFO : *********** Data Dump completed for metric : " + metricname + "  ***********")
        logfile.write("\n")
        logfile.close()
except Exception as e:
    logfile = open(log_dir+file_suffix+".log","a")
    logfile.write(time.strftime("%d-%m-%y  %H:%M:%S")+" "+"ERROR : ")
    logfile.write(str(e))
    logfile.write("\n")
    logfile.close()
    exit()

# #####################################################
# #####################################################


try:
    def get_dimensions_s3(dimension_list):
        logging("Inside get_dimensions_s3() function with argument : ", str(dimension_list))

        df_s3 = pd.DataFrame(dimension_list)

        if df_s3.at[0, 'Name'] == 'StorageType':
            if df_s3.at[1, 'Name'] == 'BucketName':
                storage_type = df_s3.at[0, 'Value']
                bkt_name = df_s3.at[1, 'Value']
                dimension_array=[bkt_name, storage_type, '']
                logging("Returning", str(dimension_array))
                return dimension_array

        if df_s3.at[0, 'Name'] == 'BucketName':
            if df_s3.at[1, 'Name'] == 'FilterId':
                bkt_name = df_s3.at[0, 'Value']
                filter_name = df_s3.at[1, 'Value']
                dimension_array=[bkt_name, '', filter_name]
                logging("Returning", str(dimension_array))
                return dimension_array
except Exception as e:
    logfile = open(log_dir+file_suffix+".log","a")
    logfile.write(time.strftime("%d-%m-%y  %H:%M:%S")+" "+"ERROR : ")
    logfile.write(str(e))
    logfile.write("\n")
    logfile.close()
    exit()
# #####################################################
# #####################################################


try:
    def get_dimensions_billing(dimension_list):
        logging("Inside get_dimensions_billing() function with argument : ", str(dimension_list))

        df_s3 = pd.DataFrame(dimension_list)

        if df_s3.at[0, 'Name'] == 'ServiceName':
            if df_s3.at[1, 'Name'] == 'Currency':
                billing_service = df_s3.at[0, 'Value']
                billing_currency = df_s3.at[1, 'Value']
                dimension_array=[billing_service, billing_currency]
                logging("Returning", str(dimension_array))
                return dimension_array

        if df_s3.at[0, 'Name'] == 'Currency':
            billing_service = ''
            billing_currency = df_s3.at[0, 'Value']
            dimension_array=[billing_service, billing_currency]
            logging("Returning", str(dimension_array))
            return dimension_array
except Exception as e:
    logfile = open(log_dir+file_suffix+".log","a")
    logfile.write(time.strftime("%d-%m-%y  %H:%M:%S")+" "+"ERROR : ")
    logfile.write(str(e))
    logfile.write("\n")
    logfile.close()
    exit()

# ####################################################


# calling statistics function
try:
    logfile=open(log_dir+file_suffix+".log","a")
    logfile.write(time.strftime("%d-%m-%y  %H:%M:%S")+" "+"INFO : ***********STARTING STATISTICS EXTRACTION**********")
    logfile.write("\n")
    logfile.close()


# ##########################################################

    if __name__ == '__main__':

        logfile = open(log_dir + file_suffix + ".log", "a")
        logfile.write(
            time.strftime("%d-%m-%y  %H:%M:%S") + " " + "INFO : EXECUTING __name__ = " + __name__)
        logfile.write("\n")
        logfile.close()

    #########################################################################
    ## Creating header row for S3 metrics data
    ########################################################################

        logfile = open(log_dir + file_suffix + ".log", "a")
        logfile.write(time.strftime("%d-%m-%y  %H:%M:%S") + " " + "INFO : creating header row for S3 metrics data")
        logfile.write("\n")
        logfile.write(time.strftime(
            "%d-%m-%y  %H:%M:%S") + " " + "INFO : HEADER = {Metric_name,Bucket_name,Storage_type,Filter,Timestamp,Average,Maximum,Minimum,Sum,unit}")
        logfile.write("\n")
        logfile.close()

        header_s3 = "Metric_name,Bucket_name,Storage_type,Filter,Timestamp,Average,Maximum,Minimum,Sum,unit"
        s3_data_file = open(S3_data_dir + "/" + file_suffix + ".csv", "a")
        s3_data_file.write(header_s3)
        s3_data_file.close()

    #########################################################################
    ## Creating header row for Billing metrics data
    ########################################################################

        logfile = open(log_dir + file_suffix + ".log", "a")
        logfile.write(
            time.strftime("%d-%m-%y  %H:%M:%S") + " " + "INFO : creating header row for billing metrics data")
        logfile.write("\n")
        logfile.write(time.strftime(
            "%d-%m-%y  %H:%M:%S") + " " + "INFO : HEADER = {Metric_name,Service_name,Currency,Timestamp,Average,Maximum,Minimum,Sum,unit}")
        logfile.write("\n")
        logfile.close()

        header_bill = "Metric_name,Service_name,Currency,Timestamp,Average,Maximum,Minimum,Sum,unit"
        bill_data_file = open(bill_data_dir + "/" + file_suffix + ".csv", "a")
        bill_data_file.write(header_bill)
        bill_data_file.close()

        logfile = open(log_dir + file_suffix + ".log", "a")
        logfile.write(time.strftime("%d-%m-%y  %H:%M:%S") + " " + "INFO : Calling list_metrics() function")
        logfile.write("\n")
        logfile.close()

        Metric_dict = cloudwatch_cli.list_metrics()

        logging("This is Metric dictionary", str(Metric_dict))

        for metric_list in Metric_dict['Metrics']:

            Name_space = metric_list['Namespace']

            Metric_name = metric_list['MetricName']

            Dimension = metric_list['Dimensions']

            if Name_space == 'AWS/S3':

                logfile = open(log_dir + file_suffix + ".log", "a")
                logfile.write(time.strftime(
                    "%d-%m-%y  %H:%M:%S") + " " + "INFO : **********STARTING for new metrics in the S3 list**********")
                logfile.write("\n")
                logfile.close()

                logging("For Name Space: ", str(Name_space))

                logging("AND Metric: ", str(Metric_name))

                logging("Calling get_dimensions_s3() function with argument: ", str(Dimension))

                dimension_array_s3 = get_dimensions_s3(Dimension)

                argument_list = [Name_space, Metric_name, Dimension, dimension_array_s3]

                logging("Calling get_metrics_stats_s3() function with argument: ", str(argument_list))

                get_metrics_stats_s3(Name_space, Metric_name, Dimension, dimension_array_s3)

            elif Name_space == 'AWS/Billing':

                logfile = open(log_dir + file_suffix + ".log", "a")
                logfile.write(time.strftime(
                    "%d-%m-%y  %H:%M:%S") + " " + "INFO : **********STARTING for new metrics in the BILLING list**********")
                logfile.write("\n")
                logfile.close()
                logging("For Name Space: ", str(Name_space))
                logging("AND Metric: ", str(Metric_name))
                logging("Calling get_dimensions_billing() function with argument: ", str(Dimension))

                dimension_array_billing = get_dimensions_billing(Dimension)

                argument_list = [Name_space, Metric_name, Dimension, dimension_array_billing]

                logging("Calling get_metrics_stats_billing() function with argument: ", str(argument_list))

                get_metrics_stats_billing(Name_space, Metric_name, Dimension, dimension_array_billing)

            else:

                continue
except Exception as e:
    logfile = open(log_dir+file_suffix+".log","a")
    logfile.write(time.strftime("%d-%m-%y  %H:%M:%S")+" "+"ERROR : ")
    logfile.write(str(e))
    logfile.write("\n")
    logfile.close()
    exit()

# #############################################################


from flask import Flask, request, send_file
from botocore.config import Config
import os
import time
from logging.handlers import  RotatingFileHandler
import logging
from datetime import datetime
from time import strftime
from logging.handlers import TimedRotatingFileHandler
import os
import yaml
import traceback
from azure.storage.blob import ContainerClient, BlobServiceClient
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient,BlobSasPermissions, generate_account_sas, ResourceTypes, AccountSasPermissions,generate_blob_sas

username = "acss"
password = "acss"
def check_files(files):
    files_dict = files.to_dict(flat=True)
    if 'data_file' not in files_dict:
        files_dict['data_file'] = ""
    return files_dict
#log_path = os.getcwd()
app = Flask(__name__)
log_file = r"./logs/app.log"
#Need to change the log file at the clients machine
#handler = RotatingFileHandler( log_file, maxBytes=50*1024*1024, backupCount=50)
handler = TimedRotatingFileHandler(log_file, when='D', interval=1,backupCount=10, encoding=None, delay=False,utc=False, atTime=None)
logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)
logger.addHandler(handler)

def load_config():
    with open('./config.yaml',"r") as ymlfile:
        return yaml.load(ymlfile,Loader=yaml.FullLoader)

@app.route('/', methods=["POST", "GET"])
def welcome():
    return "Welcome to the Storage"

@app.route('/upload_file', methods=["POST", "GET"])
def upload_file():
    data = request.form
    try:
        file = request.files['file']
        user = data['username']
        passw = data['password']
        service_type = data['service_type']
    except:
        return 'Please check whether you have provided file, username, password and service_type in the form'
    config = load_config()
    
    if len(user) == 0 or len(passw) == 0:
        return f'Username and password are mandatory'
    if user != username or passw!=password:
        return f'You are not authorized as Credentials are wrong'
    else:
        if service_type == "Azure":
            try:
                connection_string = config["azure_storage_connectionstring"]
                container_name = config["files_container"]
                blob_service_client = BlobServiceClient.from_connection_string(config["azure_storage_connectionstring"])
                container_client = blob_service_client.get_container_client(container_name)
                blob_list = container_client.list_blobs()
                b_list = [blob.name for blob in blob_list]
                filename = file.filename
                if filename in b_list:
                    return f'{filename} is already in blob storage. Please check again'
                else:
                # return "Uploaded {}".format(file.filename)
                    container_client = ContainerClient.from_connection_string(connection_string,container_name)
                    print("Uploading files to the blob storage...")
                    filename = file.filename
                    blob_client = container_client.get_blob_client(filename)
                    # with open(file,"rb") as data:
                    blob_client.upload_blob(file.read())
                    print(f'{filename} uploaded to Azure storage')
                    return f'{filename} uploaded to Azure storage'
            except Exception as e:
                return f'Unable to append {filename} to the storage. The reason is {e}'
        elif service_type == "AWS":
            try:
                filename = file.filename
                bucket = config["aws_bucket"]
                s3 = boto3.client('s3',aws_access_key_id = config["aws_key_id"],aws_secret_access_key=config["aws_access_key"])
                contents = []
                filenames = []
                for item in s3.list_objects(Bucket=bucket)['Contents']:
                    contents.append(item)
                    filenames.append(item['Key'])
                if filename in filenames:
                    return f'{filename} is already in blob storage. Please check again'
                else:
                    # s3.upload_file(filename,bucket,file.read())
                    s3.upload_fileobj(file,bucket,file.filename)
                    return f'{filename} uploaded to AWS'
            except ClientError as e:
                print('Credential is Incorrect')
                print(e)
                return f'Unable to append {filename} to the storage. The reason is {e}'
            except Exception as e:
                print(e)
                return f'Unable to append {filename} to the storage. The reason is {e}'
        else:
            return '''As of now only Azure and AWS storage is supported... Meanwhile parameter is case sensitive if you are
            looking for azure or aws. Type [Azure for azure or AWS for aws]'''


@app.route('/get_file_name', methods=["POST", "GET"])
def get_files():
    try:
        data = request.form
        service_type = data['service_type']
    
        user = data['username']
        passw = data['password']
    except:
        return 'Please check whether you have provided file, username, password and service_type in the form'
    config = load_config()
    if len(user) == 0 or len(passw) == 0:
        return f'Username and password are mandatory'
    if user != username or passw!=password:
        return f'You are not authorized as Credentials are wrong'
    else:
        if service_type == "Azure":
            connection_string = config["azure_storage_connectionstring"]
            container_name = config["files_container"]
            blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            container_client = blob_service_client.get_container_client(container_name)
            blob_list = container_client.list_blobs()
            b_list = [blob.name for blob in blob_list]
            return f'The files present in Azure are {b_list}'
        elif service_type == "AWS":
            bucket = config["aws_bucket"]
            s3 = boto3.client('s3',aws_access_key_id = config["aws_key_id"],aws_secret_access_key=config["aws_access_key"])
            contents = []
            filenames = []
            for item in s3.list_objects(Bucket=bucket)['Contents']:
                contents.append(item)
                filenames.append(item['Key'])

            # return contents,filenames
            return f'The files present in AWS are {filenames}'
        else:
            return '''As of now only Azure and AWS storage is supported... Meanwhile parameter is case sensitive if you are
            looking for azure or aws. Type [Azure for azure or AWS for aws]'''

@app.route('/retrieve_file', methods=["POST", "GET"])
def retrieve_files():
    try:
        data = request.form
        file_name = data['file_name']
        service_type = data['service_type']
        user = data['username']
        passw = data['password']
    except Exception as e:
        return 'Please check whether you have provided file, username, password and service_type in the form'
    if len(user) == 0 or len(passw) == 0:
        return f'Username and password are mandatory'
    if user != username or passw!=password:
        return f'You are not authorized as Credentials are wrong'
    else:
        
        local_path = "./download"
        config = load_config()
        if not os.path.exists(local_path):
            os.mkdir(local_path)
                
        if service_type == "Azure":
            connection_string = config["azure_storage_connectionstring"]
            container_name = config["files_container"]
            blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        
            container_client = blob_service_client.get_container_client(container_name)
            blob_list = container_client.list_blobs()
            b_list = [blob.name for blob in blob_list]
            # filename = file_name.filename
            if file_name not in b_list:
                return f'{file_name} is not in blob storage. The available files are {b_list}'
            else:
                try:
                    blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)
                    download_path = os.path.join(local_path, file_name)

                    with open(download_path, "wb") as download_file:
                        download_file.write(blob_client.download_blob().readall())
                    print("Blob downloaded.")
                    return send_file(download_path, as_attachment=True)
                    
                except Exception as e:
                    return f'Unable to download from azure. The reason is {e}'

        elif service_type == "AWS":
            try:
                bucket = config["aws_bucket"]
                download_path = os.path.join(local_path, file_name)
                s3c = boto3.client('s3',aws_access_key_id = config["aws_key_id"],aws_secret_access_key=config["aws_access_key"])
                s3 = boto3.resource('s3',aws_access_key_id = config["aws_key_id"],aws_secret_access_key=config["aws_access_key"])
                contents = []
                filenames = []
                for item in s3c.list_objects(Bucket=bucket)['Contents']:
                    contents.append(item)
                    filenames.append(item['Key'])
                # filename = file_name.filename
                if file_name not in filenames:
                    return f'{file_name} is not in blob storage. The available files are {filenames}'
                else:
                    s3.Bucket(bucket).download_file(file_name, download_path)
                    return send_file(download_path, as_attachment=True)
            except Exception as e:
                return f'Unable to download from AWS. The reason is {e}'
        else:
            return '''As of now only Azure and AWS storage is supported... Meanwhile parameter is case sensitive if you are
            looking for azure or aws. Type [Azure for azure or AWS for aws]'''

@app.route('/retrieve_temp_file', methods=["POST", "GET"])
def retrieve_temp_files():
    try:
        data = request.form
        file_name = data['file_name']
        mins = int(data['minutes'])
        service_type = data['service_type']
        
        user = data['username']
        passw = data['password']
    except:
        return 'Please check whether you have provided file, username, password and service_type in the form'
    local_path = "./download"
    config = load_config()
    if not os.path.exists(local_path):
        os.mkdir(local_path)
    if len(user) == 0 or len(passw) == 0:
        return f'Username and password are mandatory'
    if user != username or passw!=password:
        return f'You are not authorized as Credentials are wrong'
    else:     
        if service_type == "Azure":
            connection_string = config["azure_storage_connectionstring"]
            container_name = config["files_container"]
            blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            container_client = blob_service_client.get_container_client(container_name)
            blob_list = container_client.list_blobs()
            b_list = [blob.name for blob in blob_list]
            # filename = file_name.filename
            if file_name not in b_list:
                return f'{file_name} is not in blob storage. The available files are {b_list}'
            else:
                try:
                    AZURE_ACC_NAME = config["az_account"]
                    AZURE_PRIMARY_KEY = config["key"] 
                    AZURE_CONTAINER = config["files_container"] 
                    sas = generate_blob_sas(account_name=AZURE_ACC_NAME,
                                            account_key=AZURE_PRIMARY_KEY,
                                            container_name=AZURE_CONTAINER,
                                            blob_name=file_name,
                                            permission=BlobSasPermissions(read=True),                                        
                                            expiry= datetime.utcnow() + timedelta(minutes=mins))

                    # start = datetime.utcnow(),
                    sas_url ='https://'+AZURE_ACC_NAME+'.blob.core.windows.net/'+AZURE_CONTAINER+'/'+file_name+'?'+sas
                    # sas_url ="https://{AZURE_ACC_NAME}.blob.core.windows.net/{AZURE_CONTAINER}/{file_name}?{sas}"
                    
                    return sas_url
                except Exception as e:
                    return f'Unable to create temp url from azure. The reason is {e}'

        elif service_type == "AWS":
            try:
                bucket = config["aws_bucket"]
                download_path = os.path.join(local_path, file_name)
                s3c = boto3.client('s3',aws_access_key_id = config["aws_key_id"],aws_secret_access_key=config["aws_access_key"],region_name="ap-south-1",config=Config(signature_version='s3v4'))
                s3 = boto3.resource('s3',aws_access_key_id = config["aws_key_id"],aws_secret_access_key=config["aws_access_key"])
                contents = []
                filenames = []
                for item in s3c.list_objects(Bucket=bucket)['Contents']:
                    contents.append(item)
                    filenames.append(item['Key'])
                # filename = file_name.filename
                if file_name not in filenames:
                    return f'{file_name} is not in S3 Bucket. The available files are {filenames}'
                else:
                    response = s3c.generate_presigned_url('get_object',
                                                        Params={'Bucket': bucket,
                                                                'Key': file_name},
                                                        ExpiresIn=mins*60)
                    return response
            except Exception as e:
                return f'Unable to download from AWS. The reason is {e}'
        else:
            return '''As of now only Azure and AWS storage is supported... Meanwhile parameter is case sensitive if you are
            looking for azure or aws. Type [Azure for azure or AWS for aws]'''

@app.errorhandler(Exception)
def exceptions(e):
    """ Logging after every Exception. """
    ts = strftime('[%Y-%b-%d %H:%M]')
    tb = traceback.format_exc()
    logger.error('%s %s %s %s %s 5xx INTERNAL SERVER ERROR\nRequest json: %s \n%s',
                  ts,
                  request.remote_addr,
                  request.method,
                  request.scheme,
                  request.full_path,
                  request.get_json(),
                  tb)
    return "No Data Found!!!", 500
    
# path ="./firstdoc.txt"
# # config = load_config()

# folders = "./doc/"
# upload_file(path)

if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=5000)


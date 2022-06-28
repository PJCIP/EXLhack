
from flask import Flask, request, send_file

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
    file = request.files['file']
    config = load_config()
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
        print(f'{filename} uploaded to blob storage')
        return f'{filename} uploaded to blob storage'


@app.route('/get_file_name', methods=["POST", "GET"])
def get_files():
    
    config = load_config()
    connection_string = config["azure_storage_connectionstring"]
    container_name = config["files_container"]
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    blob_list = container_client.list_blobs()
    b_list = [blob.name for blob in blob_list]
    return f'The files present in the blob are {b_list}'

@app.route('/retrieve_file', methods=["POST", "GET"])
def retrieve_files():
    data = request.form
    blob_name = data['blob_name']
    
    # return "test"
    
    local_path = "./download"
    config = load_config()
    connection_string = config["azure_storage_connectionstring"]
    container_name = config["files_container"]
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    # container_client = blob_service_client.get_container_client(container_name)
    try:
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

        # local_path = "/home/rachel/Documents/Etihad/blob-quickstart-v12/data"
        if not os.path.exists(local_path):
            os.mkdir(local_path)
        download_path = os.path.join(local_path, blob_name)

        with open(download_path, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())

        # with open(blobf_name, "wb") as download_file:
        #     download_file.write(blob_client.download_blob().readall())

        print("Blob downloaded.")
        # return f'Blob downloaded'
        return send_file(download_path, as_attachment=True)
        # return send_from_directory(directory='pdf', filename=filename)
    except Exception as e:
        print("Unable to download.")
        print(e)
        return f'Unable to download. The reason is {e}'


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


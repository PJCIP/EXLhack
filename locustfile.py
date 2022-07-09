from locust import HttpUser, TaskSet, task
import uuid
import random
import string
import logging

class UserBehaviour(TaskSet):
    @task
    def launch_Url(self):
        self.client.get("/")
        logging.info("Starting page works fine")
    @task
    def getfileaz(self):
        resp = self.client.post("/get_file_name",{"username":"acss","password":"acss","service_type":"Azure"})
        logging.info("/get_file_name[{}]:{}".format(resp.status_code,resp.text))
    @task
    def retrievefile(self):
        resp = self.client.post("/retrieve_file",{"file_name":"firstdoc.txt","username":"acss","password":"acss","service_type":"AWS"})
        logging.info("/retrieve_file[{}]:{}".format(resp.status_code,resp.text))
        # print(resp.text)
    @task
    def retrievetfile(self):
        resp = self.client.post("/retrieve_temp_file",{"file_name":"firstdoc.txt","minutes":1,"username":"acss","password":"acss","service_type":"AWS"})
        logging.info("/retrieve_temp_file[{}]:{}".format(resp.status_code,resp.text))
        # print(resp.text)
    @task
    def uploads(self):
        url = '/upload_file'
        letters = string.ascii_lowercase
        result_str = ''.join(random.choice(letters) for i in range(4))
        filename = './testfiles/{}.txt'.format(result_str)
        with open(filename, 'w') as f:
            f.write(''.join(random.choice(letters) for i in range(400)))

        headers = {
                'User-Agent': 'curl/7.58.0',
                'Cookie': 'authsomething=datastring; othercookie=datastring',
        }
        files = [
            ('file', (filename, open(filename, 'rb'), 'application/x-gtar')),
            ('guid', (None, str(uuid.uuid4()))),
        ]

        resp = self.client.post(url, data = {"username":"acss","password":"acss","service_type":"AWS"},headers=headers ,files=files)
        logging.info("/upload_file:{}".format(resp.status_code))
        
    

class User(HttpUser):
    tasks=[UserBehaviour]
    min_wait = 5000
    max_wait = 15000
    host = "https://srsexl.azurewebsites.net"
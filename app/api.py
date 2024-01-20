from flask import Flask, request
from flask_restful import Resource, Api, reqparse
from flask_autoindex import AutoIndex
import json
from sys import platform
import os
from pathlib import Path
from threading import Thread
from src.utils2 import download_pdf
from process_pdf2 import start_process
import uuid
import datetime
from src.utils2 import mongo_db_connection, ai_to_pdf, AwsBackNFro
import shutil

app = Flask(__name__)
api = Api(app)

BASE_DATA_FOLDER = os.path.join(Path(os.getcwd()).parent.absolute(),'data')
if not os.path.exists(BASE_DATA_FOLDER):
    os.mkdir(BASE_DATA_FOLDER)
BASE_FONT_FOLDER = os.path.join(Path(os.getcwd()).parent.absolute(),'fonts')
status_url_table = mongo_db_connection('effy-ai-translate')

class TextToTextPDF(Resource):
    def post(self):
        req_json = json.loads(request.data.decode('utf-8'))

        if 's3_link' not in req_json or \
            'src_lang' not in req_json or \
            'dest_lang' not in req_json or \
            'user_id' not in req_json or \
            'project_id' not in req_json:
            return {'error': 'missing parameters'}, 400
        
        s3_link = req_json['s3_link']
        src_lang = req_json['src_lang']
        dest_lang = req_json['dest_lang']
        user_id = req_json['user_id']
        project_id = req_json['project_id']

        ## dest lang check
        with open('src/lang_list.json', 'r') as f:
            lang_list = json.load(f)
        if dest_lang not in lang_list['languages']:
            return {'status': 'error',
                    'description': "destination language not supported"}, 400

        BASE_PROJECT_FOLDER = os.path.join(BASE_DATA_FOLDER, user_id, project_id)
        
        if os.path.exists(BASE_PROJECT_FOLDER):
            shutil.rmtree(BASE_PROJECT_FOLDER)

        if not os.path.exists(BASE_PROJECT_FOLDER):
            os.makedirs(BASE_PROJECT_FOLDER)
        

        try:
            # download the file
            BASE_PDF_FILE = os.path.join(BASE_PROJECT_FOLDER, 'file.pdf')

            download_pdf(s3_link=s3_link,
                         save_path=BASE_PDF_FILE) # download the pdf and save it in location

            unq_id = str(uuid.uuid4())

            Thread(target=start_process, args=(BASE_PDF_FILE, 
                                             BASE_PROJECT_FOLDER,
                                             BASE_FONT_FOLDER,
                                             unq_id, 
                                             src_lang, 
                                             dest_lang,
                                             status_url_table,)).start()

            return_data = {"status": "success",
                           "status_id": unq_id}
            return (return_data, 200)
        
        except Exception as e:
            # base_error
            error_log = os.path.join(BASE_PROJECT_FOLDER, 'error_log.txt')
            with open(error_log, 'a') as f:
                f.write(str(datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')) + '\n')
                f.write(str(req_json) + '\n')
                f.write(str(e) + '\n' + '------------------------------------------------------------------\n')
                f.write('\n\n')

            # return data
            return_data = {"status": "error"}
            return ({'status': 'error'}, 400)

class PDFTranslateStatus(Resource):
    def get(self):
        req_json = json.loads(request.data.decode('utf-8'))
        print(req_json)
        if 'status_id' not in req_json:
            return ({"Error": "Parameters Missing in Request"}, 400)

        status_id = str(req_json['status_id'])
        collection_name = 'status_url'
        status_obj = status_url_table.find_one_by_uiqu_id(collection_name, status_id)

        if status_obj is None:
            return ({"Error": "No Log Found for this ID"}, 400)
        
        ## delete if s3 link is present
        if 's3_link' in status_obj[status_id]:
            progress_status = status_obj[status_id] 
            mongo_id = status_obj['_id']
            status_url_table.delete_item(collection_name, {"_id": mongo_id})
            return progress_status

        return status_obj[status_id]

class AIToPDF(Resource):
    def get(self):
        req_json = json.loads(request.data.decode('utf-8'))
        if 's3_link' not in req_json or \
            'user_id' not in req_json or \
            'project_id' not in req_json:
            return {'error': 'missing parameters'}, 400
        
        s3_link = req_json['s3_link']
        user_id = req_json['user_id']
        project_id = req_json['project_id']

        BASE_PROJECT_FOLDER = os.path.join(BASE_DATA_FOLDER, user_id, project_id)
        
        if os.path.exists(BASE_PROJECT_FOLDER):
            shutil.rmtree(BASE_PROJECT_FOLDER)
        
        if not os.path.exists(BASE_PROJECT_FOLDER):
            os.makedirs(BASE_PROJECT_FOLDER)
        
        try:
            BASE_AI_FILE = os.path.join(BASE_PROJECT_FOLDER, 'file.ai')

            download_pdf(s3_link=s3_link,
                            save_path=BASE_AI_FILE) # download the pdf and save it in location
            
            unq_id = str(uuid.uuid4())
            OUT_PDF = os.path.join(BASE_PROJECT_FOLDER, unq_id+'.pdf')
            ai_to_pdf(BASE_AI_FILE, OUT_PDF)
            aws = AwsBackNFro()

            with open(OUT_PDF, 'rb') as f:
                server_file = aws.upload(f, OUT_PDF.split('/')[-1])
            print(server_file)
            return_data = {"status": "success",
                           "s3_link": server_file}
            return (return_data, 200)
        except Exception as e:
            return_data = {"status": "error"}
            return ({'status': 'error'}, 400)


api.add_resource(TextToTextPDF, '/translate')
api.add_resource(PDFTranslateStatus, '/status')
api.add_resource(AIToPDF, '/ai_to_pdf')

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8000)
from src.utils2 import pdf_text_remover, AwsBackNFro
from src.utils2 import pdf_data_extractor_html
import os
import datetime
import uuid

def init_stats_log(mongo_db_object, uinq_id, total_progress):
    collection = 'status_url'
    status_json = {uinq_id: {'status':0,'current_progress':0 ,'total_progress': total_progress, }}
    
    mongo_id = mongo_db_object.insert_one(collection, status_json)
    return mongo_id

def update_log(mongo_db_object, mongo_id, uinq_id, status, current_progress, total_progress, s3_link=None, blank_pdf_link=None):
    collection = 'status_url'

    status_json = {}
    status_json[uinq_id] = {'status':status}
    status_json[uinq_id]['current_progress'] = current_progress
    status_json[uinq_id]['total_progress'] = total_progress
    if s3_link:
        status_json[uinq_id]['s3_link'] = s3_link
    if blank_pdf_link:
        status_json[uinq_id]['blank_pdf_link'] = blank_pdf_link
    mongo_db_object.update_by_mongo_id(collection, mongo_id, status_json)

def start_process(BASE_PDF_FILE, BASE_PROJECT_FOLDER, BASE_FONT_FOLDER, uinq_id, src_lang, dest_lang, mongo_db_object):

    try:

        # init the status log
        # logging
        total_progress_log = 5
        mongo_id = init_stats_log(mongo_db_object, uinq_id=uinq_id, total_progress=total_progress_log)

        # saving pdf data in json
        uuid_key = str(uuid.uuid4())[:8]
        pdf_data_file = os.path.join(BASE_PROJECT_FOLDER, uuid_key+"_"+datetime.datetime.now().strftime("%d_%m_%Y_%H_%M_%S") + '.json')
        pdf_data_extractor_html(pdf_path=BASE_PDF_FILE, 
                           save_path=os.path.join(BASE_PROJECT_FOLDER, pdf_data_file), src_lang=src_lang, dest_lang=dest_lang)
        
        
        #update log - PDF data extracted
        update_log(mongo_db_object,
                mongo_id = mongo_id,
                uinq_id=uinq_id,
                status=0,
                current_progress=1,
                total_progress=total_progress_log)
        
        
        # removing the text from the pdf
        uuid_key = str(uuid.uuid4())[:8]
        BLANK_PDF = os.path.join(BASE_PROJECT_FOLDER, uuid_key+"_"+datetime.datetime.now().strftime("%d_%m_%Y_%H_%M_%S") + '_blank.pdf')
        # BLANK_PDF = os.path.join(BASE_PROJECT_FOLDER, 'blank.pdf')
        pdf_text_remover(pdf_path=BASE_PDF_FILE, 
                         save_path=BLANK_PDF)
        
        #update log - PDF text removed
        update_log(mongo_db_object,
                mongo_id = mongo_id,
                uinq_id=uinq_id,
                status=0,
                current_progress=3,
                total_progress=total_progress_log)



        ## uploading to s3
        aws = AwsBackNFro()
        with open(pdf_data_file, 'rb') as f:
            server_file = aws.upload(f, pdf_data_file.split('/')[-1])
        print(server_file)

        with open(BLANK_PDF, 'rb') as f:
            blnk_pdf = aws.upload(f, BLANK_PDF.split('/')[-1])
        print(blnk_pdf)

        #update log - uploading to s3 done
        update_log(mongo_db_object,
                mongo_id = mongo_id,
                uinq_id=uinq_id,
                    status=1,
                    current_progress=5,
                    total_progress=total_progress_log,
                    s3_link=server_file,
                    blank_pdf_link=blnk_pdf)

    
    except Exception as e:
        # log the error
        error_log = os.path.join(BASE_PROJECT_FOLDER, 'thread_error.txt')
        with open(error_log, 'a') as f:
                f.write(str(datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')) + '\n')
                f.write(str(e) + '\n' + '------------------------------------------------------------------')
                f.write('\n\n')

        update_log(mongo_db_object,
                mongo_id = mongo_id,
                uinq_id=uinq_id,
                    status=-1,
                    current_progress=0,
                    total_progress=total_progress_log)
        
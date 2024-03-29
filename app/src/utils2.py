import requests
import fitz  # PyMuPDF
from tqdm import tqdm
import json
from googletrans import Translator
from threading import Thread
import os
import boto3
from pymongo.mongo_client import MongoClient
import certifi
import random
import html_to_json
from bs4 import BeautifulSoup
import subprocess
import uuid


BASE_SRC_FOLDER = os.path.dirname(os.path.abspath(__file__))

def download_pdf(s3_link, save_path):
    """
    This function downloads a pdf file from a s3 link and saves it in the save_folder
    
    params:
        s3_link: str: s3 link to the pdf file
        save_path: str: save path of the pdf file. this should include the file name
    """
    r = requests.get(s3_link, stream=True)
    with open(save_path, 'wb') as fd:
        for chunk in r.iter_content(chunk_size=1024):
            fd.write(chunk)


# def pdf_text_remover(pdf_path, save_path):
#     """
#     This function takes a pdf and removes the text from it and saves it
    
#     params:
#         pdf_path: str: path to the pdf file
#         save_path: str: path to save the pdf file
#     """
#     doc = fitz.open(pdf_path)
#     # Iterate over PDF pages
#     for page_num in range(len(doc)):
#         page = doc[page_num]
#         # Get text blocks
#         text_blocks = page.get_text("dict")["blocks"]
#         # with open('test_text_block.txt', 'w') as f:
#         #     f.write(str(text_blocks))
#         for block in text_blocks:
#             # Check if the block is a text block
#             if block['type'] == 0:  # 0 is the type for text blocks
#                 # For each text block, translate the text and replace the original text
#                 for line in block["lines"]:
#                     for span in line["spans"]:
#                         r = fitz.Rect(span["bbox"])
#                         page.add_redact_annot(r)
#         page.apply_redactions()

#     doc.save(save_path)
            
def pdf_text_remover(pdf_path, save_path):
    url = "https://avepdf.com"
    proxy_json = os.path.join(BASE_SRC_FOLDER, "proxies.json")
    with open(proxy_json, "r") as f:
        proxy_list = json.load(f)
    proxy = random.choice(proxy_list['proxy'])
    proxy = proxy.split(":")
    proxy = {"http": f"http://{proxy[2]}:{proxy[3]}@{proxy[0]}:{proxy[1]}",
            "https": f"http://{proxy[2]}:{proxy[3]}@{proxy[0]}:{proxy[1]}"}

    headers = {}

    file_name = pdf_path.split('/')[-1]

    payload = {'processedContextId': ''}
    files=[
    ('7004b126-d6cc-47a4-e4fc-efbeea10fb7b',(file_name,open(pdf_path,'rb'),'application/pdf'))
    ]


    # upload file
    response = requests.request("POST", url+'/en/file/upload', headers=headers, data=payload, files=files, proxies=proxy)

    resp_josn = response.json()
    print(resp_josn)

    # start file processing

    payload2 = {"fileIds":resp_josn['fileIds'],"processedContextId":resp_josn['processedContextId'],"toolId":"1F4618D2-B763-4C5D-AE40-E9A33024B846","pdfParameters":None}

    response2 = requests.request("POST", url+'/en/file/prepare-file-for-tool', headers=headers, json=payload2)
    resp_josn2 = response2.json()

    # remove all text frm all pages
    payload3 = {"processedContextId": resp_josn2['processedContextId'], "fileId":resp_josn2['files'][0]['uploadedFileId'], "pageRange":"*","removeOnlyHiddenText":False}

    response3 = requests.request("POST", url+'/en/tools/remove-text-action', headers=headers, json=payload3)
    resp_josn3 = response3.json()

    # download file
    f_down_url = url+'/en/file/downloadClient/'+resp_josn3['processedContextId']+'?filename='+resp_josn3['outputFileName']

    response4 = requests.request("GET", f_down_url, headers=headers)
    with open(save_path, "wb") as f:
        f.write(response4.content)


def is_url(url):
    """
    This function checks if a string is a valid url
    
    params:
        url: str: url to be checked
    
    returns:
        bool: True if the url is valid, False otherwise
    """
    url_lists = ["http://", "https://", "www."]
    for u in url_lists:
        if u in url:
            return True
    return False

def translate_text(text, src_lang, dest_lang):
    """
    This function takes a text and translates it
    
    params:
        text: str: text to be translated
        src_lang: str: source language
        dest_lang: str: destination language
    
    returns:
        translated_text: str: translated text
    """
    proxy_json = os.path.join(BASE_SRC_FOLDER, "proxies.json")
    user_agent_json = os.path.join(BASE_SRC_FOLDER, "user_agents.json")
    with open(proxy_json, "r") as f:
        proxy_list = json.load(f)
    with open(user_agent_json, "r") as f:
        user_agents = json.load(f)

    proxy = random.choice(proxy_list['proxy'])
    proxy = proxy.split(":")
    proxy = {"http": f"http://{proxy[2]}:{proxy[3]}@{proxy[0]}:{proxy[1]}"}

    user_agent = random.choice(user_agents['agents'])

    translate_obj = Translator(
        proxies=proxy,
        user_agent=user_agent
    )
    translated_text = ""
    if is_url(text):
        translated_text = text
    else:
        translated_text = translate_obj.translate(text, src=src_lang, dest=dest_lang).text
    return translated_text

class ThreadWithReturnValue(Thread):

    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs={}, Verbose=None):
        Thread.__init__(self, group, target, name, args, kwargs)
        self._return = None

    def run(self):
        if self._target is not None:
            self._return = self._target(*self._args,
                                                **self._kwargs)
    def join(self, *args):
        Thread.join(self, *args)
        return self._return

def find_keys(data, target_key, current_path=''):
    keys = []

    if isinstance(data, dict):
        for key, value in data.items():
            new_path = f"{current_path}.{key}" if current_path else key
            keys.extend(find_keys(value, target_key, new_path))
    elif isinstance(data, list):
        for i, item in enumerate(data):
            new_path = f"{current_path}[{i}]"
            keys.extend(find_keys(item, target_key, new_path))
    elif current_path.endswith(target_key):
        keys.append(current_path)

    return keys

def get_put_value_by_key(data, key, src_lang, dest_lang):
    keys = key.split('.')
    current_data = data
    for k in keys:
        if '[' in k and ']' in k:
            k, index = k.split('[')
            index = int(index[:-1])
            current_data = current_data[k][index]
        else:
            current_data = current_data[k]
    
    ## translate text
    t_text = translate_text(current_data, src_lang, dest_lang)

    ## addind hindi text to json as _value_t just before _value
    keys = key.split('.')[:-1]
    current_data = data
    for k in keys:
        if '[' in k and ']' in k:
            k, index = k.split('[')
            index = int(index[:-1])
            current_data = current_data[k][index]
        else:
            current_data = current_data[k]
    
    current_data['_value_t'] = t_text

# def bulk_translate(metadata_json, src_lang, dest_lang, save_path)
    
def json_data_extract(html_data, src_lang, dest_lang):
    soup = BeautifulSoup(html_data, 'html.parser')

    # Create a list to store parsed data
    parsed_data = []

    # Extracting paragraph data
    paragraph_tags = soup.find_all('p')
    for p in paragraph_tags:
        p_data = {'style': p['style'], 'content': []}

        # Extract content within 'b' and 'span' tags
        for content_tag in p.find_all(['b', 'span']):
            # Check if span is inside b
            if content_tag.name == 'b':
                content_dict = {'tag': 'b', 'style': content_tag.get('style', ''), 'text': '', 'content': []}

                # Add span content to the b content
                for span_tag in content_tag.find_all('span'):
                    span_content_dict = {'tag': 'span', 'style': span_tag.get('style', ''), 
                                         'text': span_tag.get_text(strip=True), 
                                         'text_t': translate_text(span_tag.get_text(strip=True), src_lang, dest_lang)}
                    content_dict['content'].append(span_content_dict)
                
                # Add text to the b if it doesn't have span with text
                if not content_dict['content']:
                    content_dict['text'] = content_tag.get_text(strip=True)
                    content_dict['text_t'] = translate_text(content_tag.get_text(strip=True), src_lang, dest_lang)

                p_data['content'].append(content_dict)
            elif content_tag.name == 'span' and not content_tag.find_parents('b'):
                p_data['content'].append({'tag': 'span', 'style': content_tag.get('style', ''), 
                                          'text': content_tag.get_text(strip=True),
                                          'text_t': translate_text(content_tag.get_text(strip=True), src_lang, dest_lang)})

        parsed_data.append(p_data)
    
    return parsed_data
    
def pdf_data_extractor_html(pdf_path, save_path, src_lang, dest_lang):
    doc = fitz.open(pdf_path)
    all_pdf_data = {}
    for page_num in range(len(doc)):
        page = doc[page_num]
    
        height = page.rect.height
        width = page.rect.width
        print(height, width)
        all_pdf_data[page_num] = {'height': height, 'width': width}
        
        htm = page.get_text('html')
        all_pdf_data[page_num]['metadata'] = json_data_extract(htm, src_lang, dest_lang)

        
        # with open('{}.html'.format(page_num), 'w') as f:
        #     f.write(htm)
        
        # with open('{}.html'.format(page_num), 'r') as f:
        #     htm = f.read()
        #     json_ = xmltojson.parse(htm)
        # output_json = html_to_json.convert(htm)

        # ## translation
        # target_key = "_value"
        # key_paths = find_keys(output_json, target_key)

        # thd = []
        # for key in tqdm(key_paths):
        #     txt = ThreadWithReturnValue(target=get_put_value_by_key, args=(output_json, key, src_lang, dest_lang))
        #     txt.start()
        #     thd.append(txt)
        # for t in thd:
        #     t.join()
        
        # all_pdf_data[page_num] = output_json
    
    with open(save_path, 'w') as f:
        json.dump(all_pdf_data, f, indent=4)

class AwsBackNFro():
    def __init__(self):
        self.aws_access_key_id = "AKIAVZBVXJWJLAWNRCWZ"
        self.aws_secret_access_key="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"
        self.region_name="us-east-1"

        self.s3 = boto3.client('s3', aws_access_key_id=self.aws_access_key_id,
                              aws_secret_access_key=self.aws_secret_access_key,
                              region_name=self.region_name)
        
        self.s3_bucket = boto3.resource('s3', aws_access_key_id=self.aws_access_key_id,
                          aws_secret_access_key=self.aws_secret_access_key,
                          region_name=self.region_name)

        self.bucket_name = 'effy-ai-translate-result'

    def upload(self, file_obj, file_path):
        self.s3.upload_fileobj(file_obj, self.bucket_name, file_path)
        s3_url = f'https://{self.bucket_name}.s3.amazonaws.com/{file_path}'
        return s3_url

    def download(self, file_path, local_file_path):
        self.s3.download_file(self.bucket_name, file_path, local_file_path)
        return local_file_path

    def upload_dict(self, file_dict):
        for subdir, dirs, files in os.walk(file_dict):
            for file in files:
                full_path = os.path.join(subdir, file)
                with open(full_path, 'rb') as data:
                    self.s3_bucket.Bucket(self.bucket_name).put_object(Key=full_path[len(file_dict)+1:], Body=data)

        print("Folders Uploaded to S3")

class mongo_db_connection():
    def __init__(self, db_name):
        self.uri = "mongodb+srv://effybizai:AhM2SPj8dKfLId89@cluster0.yfq6agh.mongodb.net/?retryWrites=true&w=majority"
        self.ca = certifi.where()
        self.client = MongoClient(self.uri, tlsCAFile=self.ca)
        try:
            self.client.admin.command('ping')
            print("Pinged your deployment. You successfully connected to MongoDB!")
        except Exception as e:
            print(e)

        self.db = self.client[db_name]

    def insert_one(self, collection_name, item):
        collection = self.db[collection_name]
        mongo_id = collection.insert_one(item)
        return mongo_id.inserted_id
    
    def get_all(self, collection_name):
        collection = self.db[collection_name]
        all_items = collection.find()
        return all_items

    def delete_item(self, collection_name, item):
        collection = self.db[collection_name]
        collection.delete_one(item)

    def find_one_by_uiqu_id(self, collection_name, id):
        all_items = self.get_all(collection_name)
        for item in all_items:
            if id in item:
                return item
        return None
    
    def update_by_mongo_id(self, collection_name, mongo_id, new_json):
        collection = self.db[collection_name]
        collection.update_one({"_id": mongo_id}, {"$set": new_json})

def ai_to_pdf(pdf_path, save_path):

    # gs -dNOPAUSE -dBATCH -sDEVICE=pdfwrite -sOutputFile=out.pdf 5817155.ai
    subprocess.call(['gs', '-dNOPAUSE', '-dBATCH', '-sDEVICE=pdfwrite', '-sOutputFile='+save_path, pdf_path])

def ai_to_png(png_path, save_path, dpi, keep_text, BASE_DATA_FOLDER):
    # gs -dNOPAUSE -dBATCH -sDEVICE=pngalpha  -r300 -sOutputFile=page-%03d.png 5817155.ai

    print(keep_text)
    if keep_text == "1":
        uuid1 = str(uuid.uuid4())
        n_png_path = os.path.join(BASE_DATA_FOLDER,uuid1+'.png')
        subprocess.call(['gs', '-dNOPAUSE', '-dBATCH', '-sDEVICE=pngalpha', '-r'+str(dpi), '-sOutputFile='+n_png_path, png_path])
        return n_png_path
    
    #coverted to pdf
    subprocess.call(['gs', '-dNOPAUSE', '-dBATCH', '-sDEVICE=pdfwrite', '-sOutputFile='+save_path, png_path])

    # #remove pdf data
    pdf_f = png_path.split('.')[0]+'_2.pdf'
    pdf_text_remover(save_path, pdf_f)

    # ## convert pdf to png
    unq_id = str(uuid.uuid4())
    png_f = os.path.join(BASE_DATA_FOLDER,unq_id+'.png')

    # gs -dNOPAUSE -dBATCH -sDEVICE=pngalpha -r1200 -sOutputFile=out12.png out.pdf
    subprocess.call(['gs', '-dNOPAUSE', '-dBATCH', '-sDEVICE=pngalpha', '-r'+str(dpi), '-sOutputFile='+png_f, pdf_f])

    return png_f

    #  pdf_f = png_path.split('.')[0]+'.pdf'
    # pdf_f_2 = png_path.split('.')[0]+'_2.pdf'

    # #remove pdf data
    # pdf_text_remover(pdf_f, pdf_f_2)

    # ## convert pdf to png
    # subprocess.call(['convert', pdf_f_2, save_path])

def esp_to_pdf(esp_path, save_path):
    # convert 5817156.eps p.pdf
    subprocess.call(['convert', esp_path, save_path])

def esp_to_png(esp_path, save_path):
    # convert 5817156.eps p.png
    subprocess.call(['convert', esp_path, save_path])

def get_lang_list():
    lng_file = os.path.join(BASE_SRC_FOLDER, "lang_list.json")
    with open(lng_file, "r") as f:
        lang_list = json.load(f)
    return lang_list
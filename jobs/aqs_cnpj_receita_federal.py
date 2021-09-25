# ### description:
# 

#_______________________________________________________________________________________________
# Imports 
from datetime import datetime
import urllib
import re
from bs4 import BeautifulSoup
from google.cloud import storage
from multiprocessing.pool import ThreadPool
import multiprocessing as ms
from zipfile import ZipFile
from zipfile import is_zipfile
import io

#_______________________________________________________________________________________________
# Variáveis globais
job_name = 'aqs_cnpj_receita_federal'

project_id = 'gcp_project_id'
bucket_name = 'bucket'
destination_blob_name = 'bucket/base_cnpj/input/'
gcs_key_file = '/opt/process_cnpj/gcp-project-id.json'

storage_client = storage.Client.from_service_account_json(gcs_key_file)

#_______________________________________________________________________________________________
# 
def get_files_download(url):
    files = None
    try:
        site = urllib.request.urlopen(url).read()
        soup = BeautifulSoup(site, 'lxml')
        files = [link.get("href") for link in soup.findAll("a", attrs={'href':re.compile(".zip")})]
    except Exception as e:
        msg = 'Erro na listagem do arquivo link: "{0}" Mensagem de erro: "{1}"'.format(url, str(e))
        print(msg)
    return files

#_______________________________________________________________________________________________
def upload_blob(source_file_name):
    try:
        start_time_ = datetime.now()
        destination_blob = destination_blob_name + source_file_name.split("/")[-1]

        file = urllib.request.urlopen(source_file_name)
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(destination_blob)

        blob.upload_from_string(file.read())

        print('Arquivo : {0} extraido, tempo gasto: {1}'.format(source_file_name.split("/")[-1], (datetime.now() - start_time_)))

    except Exception as e:
        msg = 'Erro na aquisicao do arquivo : "{0}" Mensagem de erro: "{1}"'.format(source_file_name, str(e))
        print(msg)

#_______________________________________________________________________________________________
#lista arquivos do bucket .zip
def list_blobs_zip(bucket_name, prefix):
    ls = []
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix, delimiter=None)

    for blob in blobs:
        if blob.name.endswith(".zip"):
            ls.append(blob.name)

    return ls        


#_______________________________________________________________________________________________
def zipextract(zipfilename_with_path):
    try:
        start_time_ = datetime.now()
        bucket = storage_client.get_bucket(bucket_name)

        destination_blob_pathname = zipfilename_with_path
        
        blob = bucket.blob(destination_blob_pathname)
        zipbytes = io.BytesIO(blob.download_as_string())

        if is_zipfile(zipbytes):
            with ZipFile(zipbytes, 'r') as myzip:
                for contentfilename in myzip.namelist():
                    contentfile = myzip.read(contentfilename)
                    blob = bucket.blob(destination_blob_name + "tmp/" + contentfilename)
                    blob.upload_from_string(contentfile)
        print('Arquivo : {0} extraido, tempo gasto: {1}'.format(zipfilename_with_path.split("/")[-1], (datetime.now() - start_time_)))
        
    except Exception as e:
        msg = 'Erro zipextract do arquivo : "{0}" Mensagem de erro: "{1}"'.format(zipfilename_with_path, str(e))
        print(msg)
        
#_______________________________________________________________________________________________
def run():

    start_time = datetime.now()

    list_files = get_files_download("https://www.gov.br/receitafederal/pt-br/assuntos/orientacao-tributaria/cadastros/consultas/dados-publicos-cnpj")
    # list_files = ['http://200.152.38.155/CNPJ/F.K03200$Z.D10814.CNAECSV.zip', 'http://200.152.38.155/CNPJ/F.K03200$Z.D10814.MUNICCSV.zip']
    
    # multiprocessing
    start_time_ = datetime.now()
    pool = ThreadPool(ms.cpu_count()*2)
    opt = pool.map(upload_blob, list_files)
    pool.close()
    print('Tempo gasto multiprocessamento download arquivos: {0}'.format((datetime.now() - start_time_)))
    

    # Descompactar arquivos .zip
    start_time_ = datetime.now()
    list_blob = list_blobs_zip(bucket_name, destination_blob_name)
    pool = ThreadPool(ms.cpu_count()*2)
    opt = pool.map(zipextract, list_blob)
    pool.close()
    print('Tempo gasto multiprocessamento para descompactar os arquivos: {0}'.format((datetime.now() - start_time_)))
    

    print('Total de arquivos processados: {0} Tempo total gasto: {1}'.format(len(list_files), (datetime.now() - start_time)))

#________________________________________________________________________________________
if __name__ == '__main__':
    try:
        print('Start Job')
        run()
    except Exception as e:
        msg = 'error on job: "{0}"...'.format(str(e))
        print(msg)
    finally:
        print('Finalize Job')
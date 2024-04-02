from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator,BranchPythonOperator
from airflow.operators.bash import BashOperator
from random import randint
import random
import os
from zipfile import ZipFile
import urllib.request
from urllib.request import urlopen
from shutil import copyfileobj
from urllib.request import urlopen
from shutil import copyfileobj
from zipfile import ZipFile
import shutil
import sys

def copy_zip_file(source_path, destination_path):
    try:
        shutil.copy(source_path, destination_path)
        print("Zip file copied successfully from", source_path, "to", destination_path)
    except IOError as e:
        print("Unable to copy file. %s" % e)
    except:
        print("Unexpected error:", sys.exc_info())

def _copyfileobj_patched(fsrc, fdst, length=16*1024*1024):
    """Patches shutil method to hugely improve copy speed"""
    while 1:
        buf = fsrc.read(length)
        if not buf:
            break
        fdst.write(buf)
shutil.copyfileobj = _copyfileobj_patched

def _select_files(**kwargs):
    csv_urlpaths = []
    num_links = kwargs['no_req_links']
    csv_files=[]
    print("Working dir: ",os.getcwd())
    with open('/Users/nikhilanand/airflow/list_of_files2.txt','r') as file:
        lines = file.readlines()
        flag=False
        for line in lines:
            print("Line: ",line,flag)
            if(flag==True):
                csv_files.append(line)
            if line == "/data/local-climatological-data/access/\n":
                flag = True

    print("csv_files: ",csv_files)
    csv_files = random.sample(csv_files, 3)
    
    for item in csv_files:
        csv_urlpaths.append('https://www.ncei.noaa.gov/data/local-climatological-data/access/1905/'+item[:-1])

    return csv_urlpaths

def _fetch_zip(ti):
    csv_urlpaths = ti.xcom_pull(task_ids=['select_files'])[0]
    path = '/Users/nikhilanand/airflow/data_saving/'
    zipObj = ZipFile(path+"archive.zip", 'w')
    print('hi')

    i=0
    print("CSV_URLPATHS: ",csv_urlpaths)
    for urlpath in csv_urlpaths:
        i+=1
        with urlopen(urlpath) as in_stream, open(path+"file"+str(i)+".csv", 'wb') as out_file:
            copyfileobj(in_stream, out_file)
            print('hi2')

        print("Writing...")
        zipObj.write(path+"file"+str(i)+".csv","file"+str(i)+".csv")
    print("Done writing")
    zipObj.close()
    print("Closed")

def _place_files(**kwargs):
    try:
        copy_zip_file("/Users/nikhilanand/airflow/data_saving/archive.zip", "/Users/nikhilanand/airflow/location_to_save/saved_archive.zip")
        
    except Exception as e:
        print('Error: ' + str(e))

with DAG("task1_dag2", start_date = datetime(2021,1,1),
    schedule_interval="@daily", catchup=False) as dag: # scheduled at midnight of 2nd Jan 2021 (as soon as the next day starts)

    # test_wget = BashOperator(task_id="test_wget",bash_command='which curl')
    fetch_data = BashOperator(task_id="fetch_data",bash_command='curl -s https://www.ncei.noaa.gov/data/local-climatological-data/access/1905/ | grep -o \'href="[^"]*"\' | cut -d\'"\' -f2 > /${AIRFLOW_HOME}/list_of_files2.txt')
    select_files = PythonOperator(task_id="select_files",python_callable=_select_files, op_kwargs={'no_req_links':3})
    fetch_zip = PythonOperator(task_id="fetch_zip",python_callable=_fetch_zip)
    place_files = PythonOperator(task_id="place_files",python_callable=_place_files,op_kwargs={'path':'location_to_save/'})

    fetch_data >> select_files >> fetch_zip >> place_files
    # test_wget
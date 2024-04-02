from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator,BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from random import randint
import apache_beam as beam
import numpy as np
import pandas as pd
from apache_beam.io.textio import ReadFromTextWithFilename
import re
import ast
import geopandas
from geodatasets import get_path
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

os.environ["no_proxy"]="*"

def plotMap(feat_i,month_i): # feat_i = 1 for the 1st feature, 2 for 2nd feature, and so on.
    lats = []
    longs = []
    vals = []
    with open("/Users/nikhilanand/airflow/location_to_save/monthly_avg-00000-of-00001.txt",'r') as file:
        for line in file.readlines():
            tup = ast.literal_eval(line)
            month = tup[0][0]
            location = tup[0][1:3]
            feature = tup[feat_i]
            # print(month,location,feature)

            if(month==month_i):
                lats.append(location[0])
                longs.append(location[1])
                vals.append(feature)

    data = {
        'latitude': lats,
        'longitude': longs,
        'value': vals
    }

    df = pd.DataFrame(data)

    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude))

    fig, ax = plt.subplots(figsize=(10, 8))

    sns.kdeplot(x=gdf.geometry.x, y=gdf.geometry.y, cmap='viridis', shade=True, ax=ax)

    ax.set_title('Heatmap of Month '+str(month_i) +' and feature '+str(feat_i))
    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')

    plt.savefig("/Users/nikhilanand/airflow/location_to_save/figures/figure_month"+str(month_i)+"_feat_"+str(feat_i))

def extract_float(string):
    pattern = re.compile(r"[-+]?\d*\.?\d+")
    match = pattern.search(string)

    if match:
        return float(match.group())

    else:
        return None

class ComputeMonthlyAverages(beam.DoFn):
    def process(self,element):
        filename,line = element
        arr = line.split(",")
        date = arr[0][8:10]
        yield (extract_float(date),extract_float(arr[1]),extract_float(arr[2]),[extract_float(elt) for elt in arr[3:]] + [1])

def f(record,arr):
    for i in arr:
        if record[i]=='""':
            record[i] = '"0"'
    return np.logical_and.reduce([float(record[i].strip("\""))>0 for i in arr])


def _filter_extract(**kwargs):
    with beam.Pipeline() as p1:
        arr = kwargs['arr']
        df = (
            p1
            | 'Weather conditions' >> beam.io.ReadFromText("/Users/nikhilanand/airflow/location_to_save/*.csv",skip_header_lines=1)
            | "split the record" >> beam.Map(lambda record: record.split(','))
            | 'Filter HourlyDryBulb' >> beam.Filter(lambda record: f(record,arr))
            | 'Drop fields' >> beam.Map(lambda record: [record[1],record[2],record[3]]+[record[i] for i in arr])
            | 'Write to text'>> beam.io.WriteToText('/Users/nikhilanand/airflow/location_to_save/file',file_name_suffix='.txt')
        )
    
def _monthly_averages():
    with beam.Pipeline() as p2:
        (p2
            | 'Read files' >> ReadFromTextWithFilename("/Users/nikhilanand/airflow/location_to_save/*.txt")
            | 'Split lines' >> beam.ParDo(ComputeMonthlyAverages())
            | 'Pair with 1' >> beam.Map(lambda x: ((x[0], x[1], x[2]), np.array(x[3])))
            | 'Sum per key' >> beam.CombinePerKey(sum)
            | 'Mean' >> beam.Map(lambda x: (x[0],x[1][0]/x[1][2],x[1][1]/x[1][2]))
            | 'Write to text'>> beam.io.WriteToText('/Users/nikhilanand/airflow/location_to_save/monthly_avg',file_name_suffix='.txt')
        )

def _plot_fields(**kwargs):
    try:
        num = len(kwargs["arr"])
        for fi in range(1,num+1):
            for mi in range(1,13):
                plotMap(fi,mi)
    except:
        print("Error")

with DAG("task2_dag", start_date = datetime(2021,1,1),
    schedule_interval="@daily", catchup=False) as dag: # scheduled at midnight of 2nd Jan 2021 (as soon as the next day starts)
    wait_for_file = FileSensor(
        task_id="wait_for_file", 
        filepath="/Users/nikhilanand/airflow/location_to_save/saved_archive.zip",
        timeout=5)
    
    test_zip = BashOperator(task_id="test_zip",bash_command="which unzip")

    extract_zip = BashOperator(task_id="extract_zip",bash_command="[[ -f \"/Users/nikhilanand/airflow/location_to_save/saved_archive.zip\" ]] && unzip -o \"/Users/nikhilanand/airflow/location_to_save/saved_archive.zip\" -d \"/Users/nikhilanand/airflow/location_to_save\"")
    
    filter_extract = PythonOperator(task_id="filter_extract", python_callable=_filter_extract, op_kwargs={'arr':[11,22]})
    
    monthly_averages = PythonOperator(task_id="monthly_averages",python_callable=_monthly_averages)
    
    plot_fields = PythonOperator(task_id="plot_fields",python_callable=_plot_fields, op_kwargs={"arr":[11,22]})

    delete_csvs = BashOperator(task_id="delete_csvs",bash_command="rm location_to_save/*.csv")
    
    wait_for_file >> test_zip>>extract_zip >> filter_extract >> monthly_averages >> plot_fields >> delete_csvs
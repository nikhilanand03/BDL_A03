from urllib.request import urlopen
from shutil import copyfileobj
from zipfile import ZipFile
import shutil

def _copyfileobj_patched(fsrc, fdst, length=16*1024*1024):
    """Patches shutil method to hugely improve copy speed"""
    while 1:
        buf = fsrc.read(length)
        if not buf:
            break
        fdst.write(buf)
shutil.copyfileobj = _copyfileobj_patched

path = '/Users/nikhilanand/airflow/data_saving/'
zipObj = ZipFile(path+"archive.zip", 'w')
print('hi')

with urlopen('https://www.ncei.noaa.gov/data/local-climatological-data/access/1905/22707099999.csv') as in_stream, open('data_saving/'+"file"+str(2)+".csv", 'wb') as out_file:
    copyfileobj(in_stream, out_file)
    print('hi2')

print("Writing...")
zipObj.write(path+"file"+str(2)+".csv","file"+str(2)+".csv")
print("Done writing")
zipObj.close()
print("Closed")
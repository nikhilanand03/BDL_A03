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

# Example usage:
source_file_path = "/Users/nikhilanand/airflow/data_saving/archive.zip"
destination_file_path = "/Users/nikhilanand/airflow/location_to_save/saved_archive2.zip"
copy_zip_file(source_file_path, destination_file_path)
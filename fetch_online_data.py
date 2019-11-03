
import os
import urllib.request
import zipfile

all_dataset_link = 'http://files.grouplens.org/datasets/movielens/ml-latest.zip'
small_dataset_link= 'http://files.grouplens.org/datasets/movielens/ml-latest-small.zip'


all_data_path = os.path.join("datasets", 'ml-latest.zip')
small_data_path = os.path.join("datasets", 'ml-latest-small.zip')



small = urllib.request.urlretrieve (small_dataset_link, small_data_path)
all_ = urllib.request.urlretrieve (all_dataset_link, all_data_path)


with zipfile.ZipFile(small_data_path, "r") as z:
    z.extractall("datasets")

with zipfile.ZipFile(all_data_path, "r") as z:
    z.extractall("datasets")
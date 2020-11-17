from azure.storage.blob import ContainerClient
from azure.storage.blob import BlobServiceClient
import datetime
from datetime import timedelta
import json
import pandas as pd
from azure.storage.blob import BlobClient

date_string = str(datetime.date.today() - timedelta(days=1))
date_string = "Event/" + date_string
container = ContainerClient.from_connection_string(conn_str="DefaultEndpointsProtocol=https;AccountName=ebhdevstorage"
                                                            "001;AccountKey=QYSKZ1suXASpD3Cy67U7pkFHOOWPB0Jtl4MEOFF+CNn"
                                                            "PDp72j4uDVEv9p5X7HTvpafiJpbakvBsyWiSHEqFDOQ==;EndpointSuff"
                                                            "ix=core.windows.net",
                                                   container_name="dailyinsightsmailing")
blob_service_client = BlobServiceClient(account_url="https://ebhdevstorage001.blob.core.windows.net/",
                                        credential="QYSKZ1suXASpD3Cy67U7pkFHOOWPB0Jtl4MEOFF+CNnPDp72j4uDVEv9p5X7HTvpafi"
                                                   "JpbakvBsyWiSHEqFDOQ==")
data_list = []
blob_list = container.list_blobs()
for blob in blob_list:
    if date_string in blob.name:
        name = blob.name
        blob_client = blob_service_client.get_blob_client(blob=name, container="dailyinsightsmailing")
        stream = blob_client.download_blob().content_as_text()
        stream = stream.split("}}}")
        stream = stream[:-1]
        for ele in stream:
            ele = ele + "}}}"
            data_list.append(json.loads(ele))
no_of_clicks = 0
no_of_searches = 0
id_list = []
os_list = []
city_list = []
rank_list = []
tag_list = []
for ele in data_list:
    if ele['event'][0]['name'] == 'Click':
        no_of_clicks += 1
        rank_list.append(ele['context']['custom']['dimensions'][1]['Rank'])
        os_list.append(ele['context']['device']['osVersion'])
        if 'city' in ele['context']['location'].keys():
            city_list.append(ele['context']['location']['city'])
        else:
            city_list.append(" ")
        tag_list.append(ele['context']['custom']['dimensions'][-1]['Tag'])
    elif ele['event'][0]['name'] == 'Search':
        no_of_searches += 1
df = pd.DataFrame(zip(os_list, city_list, rank_list, tag_list), columns=["OS", "City", "Rank", "Tags"])
rank_bucket = {'1': 0, '2': 0, '3': 0, '4': 0, '5': 0, '6': 0, '7': 0, '8': 0, '9': 0, '10': 0,
                                '11-20': 0,
                                '21-30': 0, '30+': 0}
for ele in df.Rank:
    if ele == "NaN":
        ele = 0
    rnk = int(ele)
    if rnk == 1:
        rank_bucket['1'] += 1
    elif rnk == 2:
        rank_bucket['2'] += 1
    elif rnk == 3:
        rank_bucket['3'] += 1
    elif rnk == 4:
        rank_bucket['4'] += 1
    elif rnk == 5:
        rank_bucket['5'] += 1
    elif rnk == 6:
        rank_bucket['6'] += 1
    elif rnk == 7:
        rank_bucket['7'] += 1
    elif rnk == 8:
        rank_bucket['8'] += 1
    elif rnk == 9:
        rank_bucket['9'] += 1
    elif rnk == 10:
        rank_bucket['10'] += 1
    elif 20 > rnk > 10:
        rank_bucket['11-20'] += 1
    elif 30 > rnk > 20:
        rank_bucket['21-30'] += 1
    else:
        rank_bucket['30+'] += 1
df2 = pd.DataFrame.from_dict(rank_bucket, orient='index').reset_index()
df2 = df2.rename(columns={'index': 'Search Rank', 0: 'count'})
df2["% of searches"] = df2['count']/no_of_searches*100
df2["% of clicks"] = df2['count']/no_of_clicks*100
df2["% of searches"] = df2["% of searches"].round(decimals=1)
df2["% of clicks"] = df2["% of clicks"].round(decimals=1)
print(df2)


local_file_path = "search and id freq.csv"
output = df2.to_csv(local_file_path)

file = open("no_of_searches.txt", 'w')
file.write(str(no_of_searches))
file.close()
file = open("no_of_searches.txt")
print(file.read())
file.close()

file = open("no_of_clicks.txt", 'w')
file.write(str(no_of_clicks))
file.close()
file = open("no_of_clicks.txt")
print(file.read())
file.close()

blob = BlobClient.from_connection_string(conn_str="DefaultEndpointsProtocol=https;AccountName=ebhdevstorage"
                                                          "001;AccountKey=QYSKZ1suXASpD3Cy67U7pkFHOOWPB0Jtl4MEOFF+CNn"
                                                          "PDp72j4uDVEv9p5X7HTvpafiJpbakvBsyWiSHEqFDOQ==;EndpointSuff"
                                                          "ix=core.windows.net", container_name="dailyinsightsoutputdf",
                                                 blob_name="output_df.csv")

with open("search and id freq.csv", "rb") as data:
    blob.upload_blob(data, overwrite=True)
blob = BlobClient.from_connection_string(conn_str="DefaultEndpointsProtocol=https;AccountName=ebhdevstorage"
                                                          "001;AccountKey=QYSKZ1suXASpD3Cy67U7pkFHOOWPB0Jtl4MEOFF+CNn"
                                                          "PDp72j4uDVEv9p5X7HTvpafiJpbakvBsyWiSHEqFDOQ==;EndpointSuff"
                                                          "ix=core.windows.net", container_name="dailyinsightsoutputdf",
                                                 blob_name="no_of_clicks.txt")

with open("no_of_clicks.txt", "rb") as data:
    blob.upload_blob(data, overwrite=True)
blob = BlobClient.from_connection_string(conn_str="DefaultEndpointsProtocol=https;AccountName=ebhdevstorage"
                                                          "001;AccountKey=QYSKZ1suXASpD3Cy67U7pkFHOOWPB0Jtl4MEOFF+CNn"
                                                          "PDp72j4uDVEv9p5X7HTvpafiJpbakvBsyWiSHEqFDOQ==;EndpointSuff"
                                                          "ix=core.windows.net", container_name="dailyinsightsoutputdf",
                                                 blob_name="no_of_searches.txt")

with open("no_of_searches.txt", "rb") as data:
    blob.upload_blob(data, overwrite=True)
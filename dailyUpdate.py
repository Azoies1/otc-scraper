from pymongo import *
import pymongo
import csv
import requests
import pandas as pd
import json

size = 1
pageSize = "&pageSize=50"
url = "https://backend.otcmarkets.com/otcapi/market-data/active/current?tierGroup=ALL&page="
pages = requests.get(url + str(size) + pageSize).json().get("pages")
response = requests.get(url + str(pages) + pageSize)
data = response.json().get("records")

bulkArr = [];
if (data != None):
    for t in data:
        ticker = t.get("symbol")
        lastPrice = t.get("price")
        nVolume = t.get("shareVolume")
        iCE = t.get("isCaveatEmptor")
        tierName = t.get("tierName")
        tierCode = t.get("tierCode")
        bulkArr.append(
            UpdateOne(
                {"symbol": ticker},
                {"$set": {"lastSale": lastPrice, "volume": nVolume, "isCaveatEmptor": iCE,
                          "tierName": tierName, "tierCode": tierCode}}
            )
        )
        print(ticker + " is modified")

    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["mydatabase"]
    result = mydb["Data"].bulk_write(bulkArr)

    # mydb["data"].find_one_and_update({"symbol": ticker},
    #                                  {"$set": {"lastSale": lastPrice, "volume": nVolume, "isCaveatEmptor": iCE,
    #                                            "tierName": tierName, "tierCode": tierCode}},
    #                                  upsert=True)
    print("Completed update")
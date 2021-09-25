import time
from pprint import pprint
import requests
import pymongo
import json
import csv
import pandas as pd
import concurrent.futures as cf
from webscaper import *


def getCashUrl(ticker):
    return baseFinUrl + "cash-flow/" + ticker + "?symbol=" + ticker + "&duration=quarterly"


def getIncomeUrl(ticker):
    return baseFinUrl + "income-statement/" + ticker + "?symbol=" + ticker + "&duration=quarterly"


def getBalanceUrl(ticker):
    return baseFinUrl + "balance-sheet/" + ticker + "?symbol=" + ticker + "&duration=quarterly"


def getResponse(url):
    response = requests.get(url)
    if response.status_code == 403:
        raise Exception(url + " got response of " + str(response.status_code))
    if response.status_code != 200:
        print(url + " got response of " + str(response.status_code) + "trying again in 40s")
        time.sleep(40)
        return getResponse(url)
    time.sleep(3)
    return response


def addtoDB(tickerId, symbol, responseJson, collection):
    idList = []
    if len(responseJson) > 0:
        item =responseJson[0]
        item["ticker_id"] = tickerId
        item["symbol"] = symbol
        cId = collection.insert_one(item).inserted_id
        idList.append(cId)
        return idList
    raise Exception("Empty Response")

def setFins(symbol):
    try:
        if income.find_one({"symbol": symbol}) is None:
            tickerId = dataCol.find_one({"symbol": symbol}).get("_id")

            iResponse = getResponse(getIncomeUrl(symbol))
            iIdList = addtoDB(tickerId, symbol, iResponse.json(), income)

            cResponse = getResponse(getCashUrl(symbol))
            cIdList = addtoDB(tickerId, symbol, cResponse.json(), cash)

            bResponse = getResponse(getBalanceUrl(symbol))
            bIdList = addtoDB(tickerId, symbol, bResponse.json(), balance)

            dataCol.update_one({"_id": tickerId}, {"$set": {"cash": cIdList, "income": iIdList, "balance": bIdList}})
            print(symbol + " financials are added")
        else:
            print(symbol + " is already added")
    except Exception as e:
        print(symbol + "financials were not added: " + e)


if __name__ == "__main__":
    MAX_THREADS = 20

    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["mydatabase"]
    cash = mydb["Cash"]
    income = mydb["Income"]
    balance = mydb["Balance"]
    officer = mydb["Officers"]
    dataCol = mydb["Data"]

    csvUrl = "https://www.otcmarkets.com/research/stock-screener/api/downloadCSV?sortField=symbol&sortOrder=asc&volMin=1"
    fullUrl = "https://backend.otcmarkets.com/otcapi/company/profile/full/"
    baseFinUrl = "https://backend.otcmarkets.com/internal-otcapi/financials/"
    cashUrl = "https://backend.otcmarkets.com/internal-otcapi/financials/cash-flow/SHRG?symbol=SHRG&duration=quarterly"
    tickerList = listOfTickers(csvUrl)
    with cf.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        processing = executor.map(setFins, [x for x in tickerList])

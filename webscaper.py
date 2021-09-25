# Web scraper
import time
from financials import *
from pprint import pprint
import requests
import pymongo
import csv
import pandas as pd
import concurrent.futures as cf

MAX_THREADS = 7

csvUrl = "https://www.otcmarkets.com/research/stock-screener/api/downloadCSV?sortField=symbol&sortOrder=asc&volMin=1"
fullUrl = "https://backend.otcmarkets.com/otcapi/company/profile/full/"
dayUrl = "https://backend.otcmarkets.com/otcapi/stock/trade/inside/"

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["mydatabase"]
tickerCol = mydb["Ticker"]
dataCol = mydb["Data"]

def listOfTickers(url):
    # Read csv data of all tickers
    data = pd.read_csv(url)
    return data.get("Symbol").tolist()


# Data from OTCMarkets
def otcMarketData(ticker):
    num = dataCol.count_documents({'symbol': ticker}), {'limit': 1}
    if num[0] == 0:
        fullResponse = getResponse(fullUrl + ticker)
        dayResponse = getResponse(dayUrl + ticker)
        fullData = fullResponse.json()
        dayData = dayResponse.json()
        return dayData, fullData
    return None, None

# Add data to the db
def addTickerData(ticker):
    try:
        dayData, fullData = otcMarketData(ticker)
        if dayData is not None:
            securities = fullData.get("securities")
            datacolumn = {
                "_id": fullData.get("id"),
                "name": fullData.get("name"),
                "symbol": securities[0].get("symbol"),
                "tierName": securities[0].get("tierName"),
                "shortInterest": securities[0].get("shortInterest"),
                "outstandingShares": securities[0].get("outstandingShares"),
                "outstandingSharesAsOfDate": securities[0].get("outstandingSharesAsOfDate"),
                "unlimitedAuthorizedShares": securities[0].get("unlimitedAuthorizedShares"),
                "authorizedShares": securities[0].get("authorizedShares"),
                "authorizedSharesAsOfDate": securities[0].get("authorizedSharesAsOfDate"),
                "publicFloat": securities[0].get("publicFloat"),
                "publicFloatAsOfDate": securities[0].get("publicFloatAsOfDate"),
                "restrictedShares": securities[0].get("restrictedShares"),
                "unrestrictedShares": securities[0].get("unrestrictedShares"),
                "unrestrictedSharesAsOfDate": securities[0].get("unrestrictedSharesAsOfDate"),
                "annualHigh": dayData.get("annualHigh"),
                "annualLow": dayData.get("annualLow"),
                "dailyHigh": dayData.get("dailyHigh"),
                "dailyLow": dayData.get("dailyLow"),
                "lastSale": dayData.get("lastSale"),
                "openingPrice": dayData.get("openingPrice"),
                "previousClose": dayData.get("previousClose"),
                "thirtyDaysAvgVol": dayData.get("thirtyDaysAvgVol"),
                "volume": dayData.get("volume")
            }
            dataCol.update_one({"_id": fullData.get("id")}, {"$set": datacolumn}, upsert=True)
            doc = {"_id": fullData.get("id"),
                   "address": fullData.get("address1"),
                   "country": fullData.get("country"),
                   "website": fullData.get("website"),
                   "phone": fullData.get("phone"),
                   "businessDesc": fullData.get("businessDesc"),
                   "isSponsored": securities[0].get("isSponsored"),
                   "shortInterest": securities[0].get("shortInterest"),
                   "shortInterestChange": securities[0].get("shortInterestChange"),
                   "shortInterestDate": securities[0].get("shortInterestDate"),
                   "restrictedShares": securities[0].get("restrictedShares"),
                   "unrestrictedShares": securities[0].get("unrestrictedShares"),
                   "unrestrictedSharesAsOfDate": securities[0].get("unrestrictedSharesAsOfDate"),
                   "spac": fullData.get("spac"),
                   "latestFilingType": fullData.get("latestFilingType"),
                   "latestFilingDate": fullData.get("latestFilingDate")
                   }
            tickerCol.update_one({"_id": fullData.get("id")}, {"$set": doc}, upsert=True)
            print(datacolumn.get("symbol"), " is added to the db")
    except ValueError:
        print("Error printing json of " + ticker)
        return "", ""
    except Exception as e:
        print(ticker + " Was not able to be added")
        print(e)

    # "premierDirectorList": fullData.get("premierDirectorList"),
    # "standardDirectorList": fullData.get("standardDirectorList"),
    # "officers": fullData.get("officers"),

if __name__ == "__main__":
    tickerList = listOfTickers(csvUrl)
    with cf.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        executor.map(addTickerData, [x for x in tickerList])
    print("Adding tickers to db a success")



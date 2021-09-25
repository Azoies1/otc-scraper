import pymongo
import datetime
from dateutil.parser import parse
import csv


def checkRevenue(revenue):
    if revenue is None:
        return None
    return float(revenue.replace('(', '-').replace(')', '').replace(',', ''))


# Revenue in the 000s
def filterCurrentPriceRevs(gtPrice, ltPrice, revenue):
    updatedList = []
    try:
        tickerList = dataCol.find({"lastSale": {"$lt": ltPrice, "$gt": gtPrice}, "tierName": {"$ne": "Pink No Information"}})
        for ticker in tickerList:
            incomeList = incomeCol.find({"symbol": ticker.get("symbol")})
            for income in incomeList:
                checkedRevs = checkRevenue(income.get("totalRevenue"))
                if parse(income.get("periodEndDate")).date() > datetime.date(2020, 8, 10) \
                        and checkedRevs is not None:
                    if checkedRevs > revenue:
                        updatedList.append({
                            "symbol": ticker.get("symbol"),
                            "tierName": ticker.get("tierName"),
                            "lastSale": ticker.get("lastSale"),
                            "revenue": income.get("totalRevenue"),
                            "authorizedShares": ticker.get("authorizedShares"),
                            "outstandingShares": ticker.get("outstandingShares"),
                            "restrictedShares": ticker.get("restrictedShares"),
                            "unrestrictedShares": ticker.get("unrestrictedShares"),
                            "publicFloat": ticker.get("publicFloat")

                        })
                        break
    except Exception as e:
        print(e)
    return updatedList


if __name__ == "__main__":
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["mydatabase"]
    incomeCol = mydb["Income"]
    dataCol = mydb["Data"]
    finalList = filterCurrentPriceRevs(0.2,0.40, 10000)
    keys = finalList[0].keys()
    with open("stock_list.csv", 'w') as output_file:
        writer = csv.DictWriter(output_file, keys, lineterminator='\n')
        writer.writeheader()
        writer.writerows(finalList)
        print("Stock list is a success")

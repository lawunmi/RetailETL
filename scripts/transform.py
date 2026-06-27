import logging
import time
from datetime import date, datetime

import pandas as pd

recordId = 0
today = date.today()

pipelineLogger = logging.getLogger("pipeline")
errorLogger = logging.getLogger("errors")

badRecords = []
def addBadRecords(id, reason):
    badRecords.append({
        "record_id": id,
        "error_reason": reason,
        "timestamp": datetime.today()
    })

def customerTransformation(data):
    global recordId
    pipelineLogger.info("Customer transformation started...")
    start = time.time()

    #Missing values
    null_rows = data[data.isna().any(axis=1)]
    if len(null_rows) > 0:
        recordId += 1
        addBadRecords(recordId, "Missing value from customer transformation")
        pd.DataFrame(badRecords).to_csv("logs/bad_records.csv", index=False)

    df = data.dropna()

    #Duplicate
    initial = len(df)
    df = df.drop_duplicates(subset=['customer_id'])

    # Text standardization and data validation
    stringCols = ['customer_name', 'city']
    df[stringCols] = df[stringCols].map(lambda x: x.str.title())
    df['registration_date'] = pd.to_datetime(df['registration_date'], errors='coerce').dt.strftime('%Y-%m-%d')

    diff = (time.time() - start)
    pipelineLogger.info(f"Customer transformation took {diff} seconds and number of removed rows: {initial - len(df)}")

    return df

def productTransformation(data):
    pipelineLogger.info("Product transformation started...")
    start = time.time()

    #Missing values
    df = data.dropna()

    #Duplicate
    initial = len(df)
    df = df.drop_duplicates(subset=['product_id'])

    #Text standardization and data validation
    stringCols = ['product_id', 'product_name']
    df[stringCols] = df[stringCols].map(lambda x: x.str.title())
    df['unit_price'] = df['unit_price'].apply(pd.to_numeric)

    end = (time.time() - start)
    pipelineLogger.info(f"Product transformation took {end} seconds and number of removed rows: {initial - len(df)}")

    return df

def salesTransformation(data):
    global recordId
    pipelineLogger.info("Sales transformation started...")
    start = time.time()

    #Missing values
    df = data.dropna()

    #Duplicate
    initial = len(df)
    df = df.drop_duplicates(subset=['sale_id'])

    # Data validation
    df['customer_id'] = pd.to_numeric(df['customer_id'], errors='coerce').astype("int64")
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
    df['sale_date'] = pd.to_datetime(df['sale_date'], errors='coerce').dt.strftime('%Y-%m-%d')

    future_sales = df[df['sale_date'] > today]

    for _, row in future_sales.iterrows():
        recordId += 1
        addBadRecords(recordId, "Sale date is in the future")
        pd.DataFrame(badRecords).to_csv("logs/bad_records.csv", index=False)

    #Additional business column
    df['total_sales'] = df['quantity'] * df['amount']
    df['sales_month'] = df['sale_date'].dt.month_name()
    df['sales_year'] = df['sale_date'].dt.year

    diff = (time.time() - start)
    pipelineLogger.info(f"Sales transformation took {diff} seconds and number of removed rows: {initial - len(df)}")

    return df

def storesTransformation(data):
    pipelineLogger.info("Stores transformation started...")
    start = time.time()

    #Missing values
    df = data.dropna()

    #Duplicate
    initial = len(df)
    df = df.drop_duplicates(subset=['store_id'])

    # Text standardization and data validation
    stringCols = ['store_name', 'city']
    df[stringCols] = df[stringCols].map(lambda x: x.str.title())

    diff = (time.time() - start)
    pipelineLogger.info(f"Stores transformation took {diff} seconds and number of removed rows: {initial - len(df)}")

    return df

def apiDataTransformation(data):
    pipelineLogger.info("API data transformation started...")
    start = time.time()

    #Missing values
    df = data.dropna()

    #Duplicate
    initial = len(df)
    df = df.drop_duplicates(subset=['id'])

    # Text standardization and data validation
    stringCols = ['firstname', 'lastname', 'city']
    df[stringCols] = df[stringCols].map(lambda x: x.str.title())

    diff = (time.time() - start)
    pipelineLogger.info(f"API data transformation took {diff} seconds and number of removed rows: {initial - len(df)}")

    return df

def customerSalesDataMerger(cus, sales):
    pipelineLogger.info("Customer sales data merger transformation started...")
    start = time.time()
    merged = pd.merge(cus, sales, on='customer_id')

    diff = (time.time() - start)
    pipelineLogger.info(f"Customer sales data transformation took {diff} seconds")

    return merged

def dataTransformation(data):
    pipelineLogger.info("Data transformation started...")
    start = time.time()

    cleanCustomerData = customerTransformation(data.get("customers", pd.DataFrame()))
    cleanProductData  = productTransformation(data.get("products", pd.DataFrame()))
    cleanSalesData    = salesTransformation(data.get("sales", pd.DataFrame()))
    cleanStoresData    = storesTransformation(data.get("stores", pd.DataFrame()))
    cleanAPIData = apiDataTransformation(data.get("apiData", pd.DataFrame()))


    diff = (time.time() - start)
    pipelineLogger.info(f"Data transformation took {diff} seconds")

    return {
        "customers": cleanCustomerData,
        "products": cleanProductData,
        "sales": cleanSalesData,
        "stores": cleanStoresData,
        "apiData": cleanAPIData,
    }


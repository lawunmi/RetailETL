from datetime import time
import time
import pandas as pd
from dotenv import load_dotenv
import requests
import os
import logging

pipelineLogger = logging.getLogger("pipeline")
errorLogger = logging.getLogger("error")

load_dotenv()
url = os.getenv("API_URL")

pipelineLogger.info("Starting retail ETL data extraction...")
def extractCSVData(data):
    pipelineLogger.info(f"Starting CSV extraction: '{data}'")
    start = time.time()
    try:
        df = pd.read_csv(data)
        diff = (time.time() - start)
        pipelineLogger.info(f"Rows extracted from '{data}' is {len(df)} - {diff} seconds")
        return df
    except Exception as e:
        errorLogger.error(f"Extraction failed '{data}': {e}")
        return pd.DataFrame()

def extractJSONData(data):
    pipelineLogger.info(f"Starting JSON data extraction: '{data}'")
    start = time.time()
    try:
        df = pd.read_json(data)
        diff = (time.time() - start)
        pipelineLogger.info(f"Rows extracted from '{data}' is {len(df)} - {diff} seconds")
        return df
    except Exception as e:
        errorLogger.error(f"Extraction failed '{data}': {e}")
        return pd.DataFrame()

def extractAPIData(url):
    start = time.time()
    try:
        response = requests.get(url)
        if response.status_code == 200:
            pipelineLogger.info(f"Extracting data from API, status code: {response.status_code}")
            res = response.json()
            resDf = pd.json_normalize(res)
            diff = (time.time() - start)
            pipelineLogger.info(f"Rows extracted from '{url}' is {len(resDf)} - {diff} seconds")
            return resDf
        else:
            errorLogger.error(f"Error getting data from API, status code: {response.status_code}")
            return None
    except Exception as e:
        errorLogger.error(f"Exception thrown: {e}")
        return pd.DataFrame()

def extractAllData():
    pipelineLogger.info(f"Starting data extraction...")
    return {
        "products": extractCSVData('data/products.csv'),
        "sales": extractCSVData('data/sales.csv'),
        "stores": extractCSVData('data/stores.csv'),
        "customers": extractJSONData('data/customers.json'),
        "apiData": extractAPIData(url)
    }
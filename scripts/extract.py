import pandas as pd
import json
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
    try:
        df = pd.read_csv(data)
        pipelineLogger.info(f"Rows extracted from '{data}' is {len(df)}")
        return df
    except Exception as e:
        errorLogger.error(f"Extraction failed '{data}': {e}")
        return None

def extractJSONData(data):
    pipelineLogger.info(f"Starting JSON data extraction: '{data}'")
    try:
        df = pd.read_json(data)
        pipelineLogger.info(f"Rows extracted from '{data}' is {len(df)}")
        return df
    except Exception as e:
        errorLogger.error(f"Extraction failed '{data}': {e}")
        return None

def extractAPIData(url):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            pipelineLogger.info(f"Extracting data from API, status code: {response.status_code}")
            return response.json()
        else:
            errorLogger.error(f"Error getting data from API, status code: {response.status_code}")
            return None
    except Exception as e:
        errorLogger.error(f"Exception thrown: {e}")
        return None

def extractAllData():
    pipelineLogger.info(f"Starting data extraction...")
    return {
        "Products": extractCSVData('data/products.csv'),
        "Sales": extractCSVData('data/sales.csv'),
        "Stores": extractCSVData('data/stores.csv'),
        "Customers": extractJSONData('data/customers.json'),
        "API Data": extractAPIData(url)
    }
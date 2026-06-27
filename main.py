import logging

from scripts.extract import extractAllData
from scripts.transform import dataTransformation

logger = logging.getLogger(__name__)

def logSetup(pipelineLog, errorLog):
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    pipelineHandler =  logging.FileHandler(pipelineLog, mode='w')
    pipelineHandler.setFormatter(formatter)
    pipeline = logging.getLogger('pipeline')
    pipeline.setLevel(logging.INFO)
    pipeline.addHandler(pipelineHandler)

    errorHandler = logging.FileHandler(errorLog, mode='w')
    errorHandler.setFormatter(formatter)
    error = logging.getLogger('errors')
    error.setLevel(logging.ERROR)
    error.addHandler(errorHandler)


if __name__ == "__main__":
    logSetup("./logs/pipeline.log", "./logs/errors.log")

def pipeline():
    data = extractAllData()
    if not data:  # if data == None
        logging.error("No data found")
        exit()

    transformedData = dataTransformation(data)

pipeline()

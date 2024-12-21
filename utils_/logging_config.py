import logging
from datetime import datetime
today=datetime.today().strftime('%Y-%m-%d')
logging.basicConfig(filename=f'logs/app-{today}.log',level=logging.INFO, filemode='w+', \
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%d/%m/%y %H:%M:%S')


logformat=logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%d/%m/%y %H:%M:%S')
logging.getLogger("py4j").setLevel(logging.INFO)
logging.getLogger('pyspark').setLevel(logging.CRITICAL)
logger=logging.getLogger("Kafka-Spark-ETL")

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logformat)
logger.addHandler(consoleHandler)
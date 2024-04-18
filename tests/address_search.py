
from duckduckgo_search import DDGS

import requests

def get_public_ip():
    try:
        # Use a public IP address API to fetch the IP address
        response = requests.get('https://api.ipify.org?format=json')
        if response.status_code == 200:
            ip_data = response.json()
            ip_address = ip_data['ip']
            return ip_address
        else:
            return None
    except Exception as e:
        print(f"Error fetching IP address: {e}")
        return None


import socks
import socket
from stem import Signal
from stem.control import Controller
from duckduckgo_search import DDGS
import requests

def change_tor_identity():
    with Controller.from_port(port=9051) as controller:
        controller.authenticate(password="password123")  # Replace "your_password" with your actual Tor password
        controller.signal(Signal.NEWNYM)

import asyncio
import logging
import psycopg2
import pandas as pd
from tqdm import tqdm
import warnings
from duckduckgo_search import AsyncDDGS
from time import sleep
import csv
import os
from prometheus_client import start_http_server, Counter, Histogram

proxies_list = {}

def getProxies():
    with open("proxies.txt") as f:
        for line in f:
            (k, v) = line.split("|")
            proxies_list[k] = v

# Initialize Prometheus metrics server
start_http_server(8000)

# Define a Prometheus counter for API requests
api_requests_counter = Counter('api_requests_total', 'Total number of API requests')

# Define a Prometheus histogram for API request latency
api_request_latency_histogram = Histogram(
    'api_request_latency_seconds',
    'API request latency in seconds',
    buckets=[0.1, 0.5, 1, 2, 5, 10]  # Define custom buckets for latency measurements
)

# Define a Prometheus histogram for number of API requests
api_requests_histogram = Histogram(
    'api_requests_count',
    'Histogram of API requests count',
    buckets=[1, 5, 10, 20, 50, 100]  # Define custom buckets for request count
)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DB_NAME = 'Belden'
DB_USER = 'postgres'
DB_PASSWORD = '1BeldenUser'
DB_HOST = 'belden.c4zokjoxtfcq.us-east-2.rds.amazonaws.com'
DB_PORT = 5432
INPUT_CSV_FILE_PATH = 'result_db.csv'
OUTPUT_CSV_FILE_PATH = 'updated_result_db.csv'
CHECKPOINT_FILE = 'checkpoint.txt'

mini_query = '''
SELECT id, primary_business_name, primary_postalcode, primary_state, primary_city, primary_country, 
TRIM(COALESCE(primary_address_street_line_1 || ' ', '') || COALESCE(primary_address_street_line_2, '')) AS primary_street, 
data_quality, enrichedregion 
FROM extractfortranche 
WHERE id IN (SELECT e.id FROM extractfortranche e WHERE e.majortranche = 'Tranche 1.5 Iteration 2' AND e.match_confidence_code NOT IN ('10')) 
ORDER BY id
'''

# Try connecting to the database
try:
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
    logging.info("Successfully connected to the database.")
except (Exception, psycopg2.Error) as dbError:
    logging.error("Error while connecting to PostgreSQL", exc_info=True)
    exit()


def save_checkpoint(index):
    logging.debug(f"Saving checkpoint at index: {index}")
    with open(CHECKPOINT_FILE, 'w') as file:
        file.write(str(index))


def load_checkpoint():
    try:
        with open(CHECKPOINT_FILE, 'r') as file:
            checkpoint = int(file.read())
        logging.debug(f"Loaded checkpoint: {checkpoint}")
        return checkpoint
    except FileNotFoundError:
        logging.debug("Checkpoint file not found. Starting from the beginning.")
        return 0


async def aget_results(word):
    logging.debug(f"Fetching results for: {word}")
    #ddgs = DDGS(proxies="socks5://localhost:9150", timeout=20)
    ddgs = DDGS(proxies=proxies_list, timeout=20)
    results = ddgs.text(word, max_results=4, backend="api")
    return results


async def getAddress(phrase):
    logging.debug(f"Getting address for phrase: {phrase}")
    return await asyncio.gather(aget_results(phrase))


def update_csv(row, header, mode='a'):
    logging.debug(f"Updating CSV for record with ID: {row.get('id', 'Unknown ID')}")
    with open(INPUT_CSV_FILE_PATH, mode, newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=header)
        if mode == 'w':
            writer.writeheader()
        # Ensure row is properly converted to a dictionary before writing
        row_dict = row.to_dict() if isinstance(row, pd.Series) else row
        writer.writerow(row_dict)


def main():
    # Connect to Tor network
    socks.setdefaultproxy(socks.PROXY_TYPE_SOCKS5, "127.0.0.1", 9050)
    socket.socket = socks.socksocket

    logging.info("Starting main process.")
    if not os.path.exists(INPUT_CSV_FILE_PATH):
        logging.info("CSV file not found. Fetching data from the database...")
        df = pd.read_sql_query(mini_query, conn)
        logging.info(f"Fetched {len(df)} records from the database.")
        df['address_string'] = df[
            ['primary_business_name', 'primary_street', 'primary_city', 'primary_state', 'primary_country',
             'primary_postalcode']].apply(
            lambda x: ''.join(x.dropna().astype(str)), axis=1)
        df['search_result'] = None
        df['search_url'] = None
        df.to_csv(INPUT_CSV_FILE_PATH, index=False)
        logging.info("CSV file initialized.")
        start_index = 0
    else:
        df = pd.read_csv(INPUT_CSV_FILE_PATH)
        start_index = load_checkpoint() + 1
        logging.info(f"Resuming from checkpoint at index: {start_index}")

    # Open or create the output CSV file for writing
    mode = 'w' if start_index == 0 else 'a'
    with open(OUTPUT_CSV_FILE_PATH, mode, newline='', encoding='utf-8') as file:
        writer = None

        for index, row in tqdm(df.iloc[start_index:].iterrows(), initial=start_index, total=df.shape[0]):
            try:
                change_tor_identity()  # Change Tor identity before each request
                search = 'address of ' + row['address_string']
                #sleep(2)  # Rate limit
                print("Starting...", index)
                print(search)
                results = asyncio.run(getAddress(search))
                urls, result_addr = '', ''
                for result in results:
                    for item in result:
                        if 'dnb.com' not in item['href']:
                            urls += item['href'] + ','
                            result_addr += item['body'] + ','

                row['search_url'] = urls[:-1] if urls.endswith(',') else urls
                row['search_result'] = result_addr[:-1] if result_addr.endswith(',') else result_addr
            except Exception as err:
                logging.error(f"Error processing record {index}: {err}", exc_info=True)
                # In case of exception, 'search_url' and 'search_result' remain unchanged (or empty if new)

            # Initialize CSV writer after first row is ready to determine headers
            if writer is None:
                writer = csv.DictWriter(file, fieldnames=row.index.tolist())
                if mode == 'w':  # Write header only for new file
                    writer.writeheader()

            writer.writerow(row.to_dict())
            save_checkpoint(index)
            api_requests_counter.inc()  # Increment Prometheus counter for each API request
            api_requests_histogram.observe(1)  # Observe 1 for each API request
            ip_address = get_public_ip()
            if ip_address:
                print(f"Public IP Address: {ip_address}")
            else:
                print("Failed to fetch IP address.")


if __name__ == "__main__":
    main()

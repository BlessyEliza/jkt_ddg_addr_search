import psycopg2
from psycopg2 import Error
from duckduckgo_search import DDGS
import sys
import os
from time import sleep
from tqdm import tqdm
from unidecode import unidecode
import warnings
import pandas as pd
import shutil
from datetime import datetime
import logging
import traceback
import time
import requests
from bs4 import BeautifulSoup
import pandas as pd
import threading
import multiprocessing
import uuid
from prometheus_client import start_http_server, Counter
import queue as Q

main_logger = logging.getLogger('MainLogger')
main_logger.setLevel(logging.INFO)
main_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
main_file_handler = logging.FileHandler('main_log.log', encoding="utf-8")
main_file_handler.setLevel(logging.INFO)
main_file_handler.setFormatter(main_formatter)
main_logger.addHandler(main_file_handler)

main_stream_handler = logging.StreamHandler(sys.stdout)
main_stream_handler.setLevel(logging.INFO)
main_stream_handler.setFormatter(main_formatter)
main_logger.addHandler(main_stream_handler)


def get_core_logger(core_id):
    core_logger = logging.getLogger(f'Core{core_id}Logger')
    core_logger.setLevel(logging.DEBUG)
    core_formatter = logging.Formatter('%(asctime)s - Core %(processName)s - %(levelname)s - %(message)s')
    core_file_handler = logging.FileHandler(f'core_{core_id}_log.log', encoding="utf-8")
    core_file_handler.setLevel(logging.DEBUG)
    core_file_handler.setFormatter(core_formatter)
    core_logger.addHandler(core_file_handler)

    core_stream_handler = logging.StreamHandler(sys.stdout)
    core_stream_handler.setLevel(logging.DEBUG)
    core_stream_handler.setFormatter(core_formatter)
    core_logger.addHandler(core_stream_handler)

    return core_logger


def get_public_ip(core_logger):
    try:
        response = requests.get('https://api.ipify.org?format=json')
        if response.status_code == 200:
            ip_data = response.json()
            ip_address = ip_data['ip']
            return ip_address
        else:
            return None
    except Exception as e:
        core_logger.error(f"Error fetching IP address: {e}")
        return None

def file_exists_in_current_directory(file_name):
    return os.path.exists(file_name)
def combine_output_files():
    output_folder = 'output'
    output_csv = 'output.csv'
    output_files = [file for file in os.listdir(output_folder) if file.endswith('.csv')]

    if not output_files:
        main_logger.info("No CSV files found in the output folder.")
        return

    combined_df = pd.DataFrame()

    for file in output_files:
        file_path = os.path.join(output_folder, file)
        df = pd.read_csv(file_path)
        combined_df = combined_df.append(df, ignore_index=True)

    combined_df.to_csv(output_csv, index=False)
    main_logger.info(f"Combined data from {len(output_files)} CSV files into {output_csv}.")


start_http_server(8000)

api_requests_counter = Counter('api_requests_total', 'Total number of API requests')


def process_chunks(queue, chunk_names):
    while True:
        try:
            chunk_data, chunk_name = queue.get(timeout=1)
            scrape_data(chunk_data, chunk_name)
            queue.task_done()
        except queue.Empty:
            break


def getAddress(word, core_logger, retries=0):
    max_retries = 1
    backoff_factor = 0

    if not word or word.strip() == "":
        core_logger.warning(f"Skipping empty search term: {word}")
        return []
    try:
        ddgs = DDGS()
        results = ddgs.text(word, max_results=5, backend="api")
        core_logger.info(f"Proxy results{results}")
        core_logger.info(f"Returning results{results}")
        return results
    except Exception as e:
        core_logger.exception(f"Error occurred: {e}. Fetching a new proxy and retrying...")
        if retries < max_retries:
            sleep_time = backoff_factor ** retries
            core_logger.exception(f"Retrying in {sleep_time} seconds...")
            time.sleep(sleep_time)
            return getAddress(word, core_logger=core_logger, retries=retries + 1)
        else:
            core_logger.error(f"Skipping record after {max_retries} attempts. Error: {e}")
            return []


def simulate_rate_limit():
    global rate_limit_hits
    rate_limit_hits += 1
    if rate_limit_hits % 3 == 0:
        raise Exception("RatelimitException")
def file_exists(file_name):
    return os.path.exists(file_name)

def create_or_replace_folder(folder_name):
    current_directory = os.getcwd()
    folder_path = os.path.join(current_directory, folder_name)
    if os.path.exists(folder_path):
        try:
            shutil.rmtree(folder_path)
            main_logger.info(f"Removed existing folder '{folder_name}'")
        except OSError as e:
            main_logger.error(f"Error: {folder_name} : {e.strerror}")
            return False
    try:
        os.mkdir(folder_path)
        main_logger.info(f"Created folder '{folder_name}'")
        return True
    except OSError as e:
        main_logger.error(f"Error: {folder_name} : {e.strerror}")
        return False


def check_and_create_output_directory():
    if not os.path.exists('output'):
        os.makedirs('output')


def get_timestamp():
    now = datetime.now()
    return now.strftime("%d/%m/%y %H:%M:%S")


def fetch_and_save_proxies(proxy_list_path, limit=50):
    url = 'https://free-proxy-list.net/'
    response = requests.get(url)
    proxies = []

    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        table = soup.find('table', {'class': 'table table-striped table-bordered'})
        row_count = 0
        for tr in table.find_all('tr'):
            if row_count >= limit:
                break
            row = [td.text.strip() for td in tr.find_all('td')]
            if row:
                protocol = "https" if row[6] == "yes" else "http"
                proxy_url = f"{protocol}://{row[0]}:{row[1]}"
                proxies.append(proxy_url)
                row_count += 1

        with open(proxy_list_path, 'w') as f:
            f.write('\n'.join(proxies))

    else:
        print("Failed to fetch proxy list.")
        if not os.path.exists(proxy_list_path):
            open(proxy_list_path, 'a').close()
    return proxy_list_path


def load_proxies(proxy_list_path):
    with open(proxy_list_path, 'r') as f:
        return [line.strip() for line in f.readlines()]


def save_proxies(proxy_list_path, proxies):
    with open(proxy_list_path, 'w') as f:
        f.write('\n'.join(proxies))


def get_proxy_ip(core_logger):
    proxy_list_path = 'available_proxies.txt'
    used_proxies_path = 'used_proxies.txt'

    if os.path.exists(used_proxies_path):
        with open(used_proxies_path, 'r') as f:
            used_proxies = [line.strip() for line in f.readlines()]
    else:
        used_proxies = []

    if not os.path.exists(proxy_list_path) or os.stat(proxy_list_path).st_size == 0:
        fetch_and_save_proxies(proxy_list_path)

    available_proxies = load_proxies(proxy_list_path)

    for proxy_url in available_proxies:
        protocol = "https" if "https://" in proxy_url else "http"
        proxy = {protocol: proxy_url}
        if proxy_url not in used_proxies:
            used_proxies.append(proxy_url)
            available_proxies.remove(proxy_url)
            save_proxies(proxy_list_path, available_proxies)
            save_proxies(used_proxies_path, used_proxies)
            core_logger.info(f"Using new proxy: {proxy_url}")
            return proxy

    core_logger.warning("No available fast proxies found, fetching new proxies...")
    proxy_list_path = fetch_and_save_proxies(proxy_list_path)
    return get_proxy_ip(core_logger)


def fetch_data_from_db():
    connection = psycopg2.connect(
        user="postgres",
        password="1BeldenUser",
        host="belden.c4zokjoxtfcq.us-east-2.rds.amazonaws.com",
        port="5432",
        database="Belden"
    )
    connection.autocommit = True
    cursor = connection.cursor()

    fetch_data_query = """SELECT id, primary_business_name,
        TRIM(COALESCE(primary_address_street_line_1, '') || ' ' ||
            COALESCE(primary_address_street_line_2, '') || ' ' ||
            COALESCE(primary_city, '') || ' ' ||
            COALESCE(primary_state, '') || ' ' ||
            COALESCE(primary_country, '') || ' ' ||
            COALESCE(primary_postalcode, '')) as primary_address
        FROM extractfortranche
        WHERE id IN (SELECT e.id FROM extractfortranche e WHERE e.majortranche = 'Tranche 1.5 Iteration 2')"""

    if file_exists_in_current_directory("run_ids.csv"):
        run_ids = pd.read_csv("run_ids.csv")['id'].tolist()
        if len(run_ids) > 0:
            ids_str = ",".join([f"'{id}'" for id in run_ids])
            fetch_data_query += f" AND id IN ({ids_str})"

    fetch_data_query += " ORDER BY id"

    cursor.execute(fetch_data_query)
    records = cursor.fetchall()
    cursor.close()
    connection.close()
    return records


def scrape_data(records_chunk, chunk_name, core_id):
    core_logger = get_core_logger(core_id)
    core_logger.info(f"\nStarting processing for chunk: {chunk_name} on Core {core_id}\n")

    processed_count = 0
    output_file_path = 'output'
    write_header = not os.path.exists(output_file_path)

    for record in tqdm(records_chunk):
        try:
            sleep(2.5)
            core_logger.info(f"Processing record {record} in chunk: {chunk_name} on Core {core_id}")
            search_terms = [f'address {record[1]} {record[2]}',
                            f'company {record[1]} website contact address {record[2]}']
            results = []
            for term in search_terms:
                results = getAddress(term, core_logger=core_logger)
                if results:
                    break
            if results:
                for item in results:
                    if 'dnb.com' in item['href']:
                        continue
                    row = {
                        'id': record[0],
                        'url': item['href'].replace("'", "").replace('"', ''),
                        'body': unidecode(item['body']).replace("'", "").replace('"', '')
                    }
                    result_df = pd.DataFrame([row])
                    result_df.to_csv("output/" + output_file_path + "_" + chunk_name + ".csv", mode='a',
                                     header=write_header, index=False)
                    write_header = False
            api_requests_counter.inc()
            core_logger.info("API Count" + api_requests_counter)
            core_logger.info("API Count" + api_requests_counter)
            ip_address = get_public_ip(core_logger)
            if ip_address:
                core_logger.info(f"\nPublic IP Address: {ip_address}")
            else:
                core_logger.error("\nFailed to fetch IP address.")
        except Exception as e:
            core_logger.error(f"Error processing record {record} in chunk: {chunk_name}: {e}")

        processed_count += 1

    if write_header:
        pd.DataFrame(columns=['id', 'url', 'body']).to_csv(output_file_path, index=False)

    core_logger.info(f"Finished processing for chunk: {chunk_name} on Core {core_id}.")
    return True


def read_failed_ids():
    if not os.path.exists('failed_ids.csv'):
        return []
    with open('failed_ids.csv', 'r') as file:
        failed_ids = file.read().splitlines()
    return failed_ids


def get_processed_ids(output_file='output.csv'):
    if not os.path.exists(output_file) or os.stat(output_file).st_size == 0:
        return []
    df = pd.read_csv(output_file)
    return df['id'].unique().tolist()


def fetch_data_from_db_and_save_to_csv(records_csv='db_records.csv'):
    records = fetch_data_from_db()
    df = pd.DataFrame(records, columns=['id', 'name', 'billing_address'])
    df.to_csv(records_csv, index=False)
    return df


def read_records_from_csv(records_csv='db_records.csv'):
    return pd.read_csv(records_csv)


def callback_function(result, core_logger):
    core_logger.info(f"Chunk processed, result: {result}")

from multiprocessing import Manager


import queue
import multiprocessing
import uuid


def process_chunk_worker(core_id, input_queue, processed_chunks):
    while True:
        try:
            main_logger.info(f"Core {core_id} attempting to get chunk from the queue...")
            chunk_data, chunk_name = input_queue.get(timeout=1)  # Fetch a chunk from the queue
            main_logger.info(f"Core {core_id} got chunk: {chunk_name} from the queue")

            # Check if the chunk name is in the processed chunks set
            if chunk_name in processed_chunks:
                main_logger.info(f"Chunk {chunk_name} already processed. Skipping...")
                input_queue.task_done()  # Mark the task as complete
                continue  # Skip processing the chunk

            # Print Core ID processing the chunk
            main_logger.info(f"Core {core_id} processing chunk: {chunk_name}")

            # Example processing logic - replace this with your actual processing code
            result = scrape_data(chunk_data, chunk_name, core_id)
            result = f"Processed {chunk_name}"

            processed_chunks.add(chunk_name)
            input_queue.task_done()

        except queue.Empty:
            # Queue is empty, no more chunks to process
            main_logger.info(f"Core {core_id} - Queue is empty. No more chunks to process.")
            break

def process_records():
    # Check and create output directory
    check_and_create_output_directory()

    # Load records from CSV or fetch from DB
    records_csv = 'db_records.csv'
    records = []
    if file_exists(records_csv):
        main_logger.info("Reading records from CSV file.")
        records_df = read_records_from_csv(records_csv)
        records = records_df.values.tolist()
    else:
        main_logger.info("Fetching records from the database and saving to CSV.")
        records_df = fetch_data_from_db_and_save_to_csv(records_csv)
        records = records_df.values.tolist()

    if not records:
        main_logger.warning("No records fetched. Exiting.")
        return False

    # Get processed and failed IDs
    processed_ids = get_processed_ids()
    failed_ids = read_failed_ids()

    main_logger.info(f"Count of Processed ids: {len(processed_ids)}")
    main_logger.info(f"Count of overall records: {len(records)}")

    # Filter records to process
    records_to_process = [record for record in records if record[0] not in processed_ids and record[0] not in failed_ids]
    main_logger.info(f"Count of records to process: {len(records_to_process)}")
    if not records_to_process:
        main_logger.info("No new records to process.")
        return False

    # Initialize a multiprocessing Manager to create a shared set for processed chunks
    with multiprocessing.Manager() as manager:
        processed_chunks = manager.list()  # Use manager.list() instead of manager.Set()

        # Multiprocessing
        num_cores = multiprocessing.cpu_count()
        main_logger.info(f"Number of CPU cores available: {num_cores}")

        chunk_size = 100
        total_records = len(records_to_process)
        total_chunks = (total_records + chunk_size - 1) // chunk_size

        chunk_names = [str(uuid.uuid4()) for _ in range(total_chunks)]
        records_chunks = [records_to_process[i:i + chunk_size] for i in range(0, total_records, chunk_size)]
        main_logger.info(f"Single Chunk Size: {chunk_size}")
        main_logger.info(f"Total chunks: {len(records_chunks)}")
        main_logger.info("Total chunk_names: " + str(len(chunk_names)))

        input_queue = multiprocessing.Queue()
        for chunk, chunk_name in zip(records_chunks, chunk_names):
            input_queue.put((chunk, chunk_name))

        main_logger.info(f"Queue length before starting workers: {input_queue.qsize()}")

        processes = []
        for core_id in range(num_cores):
            main_logger.info(f"\n\nAssigning Chunks to Core ID: {core_id}.....\n\n")
            process = multiprocessing.Process(target=process_chunk_worker, args=(core_id, input_queue, processed_chunks))
            processes.append(process)
            process.start()

        for process in processes:
            process.join()

    main_logger.info("\n\nAll chunks have been processed.\n\n")
    return True

def main():
    start_time_main = time.time()
    done = process_records()
    if done:
        main_logger.info("Combining all output files.....")
        combine_output_files()
        end_time_main = time.time()
        execution_time_main = end_time_main - start_time_main
        main_logger.info(f"Execution time: {execution_time_main:.2f} seconds.")
        main_logger.info("\n\nEnd of Processing! ")
    else:
        main_logger.info("Not Completed")

if __name__ == "__main__":
    main()
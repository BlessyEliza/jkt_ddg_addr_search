import psycopg2
from psycopg2 import Error
from duckduckgo_search import DDGS
import os
from time import sleep
from tqdm import tqdm
from unidecode import unidecode
import warnings
import pandas as pd
import shutil
warnings.filterwarnings('ignore')
from datetime import datetime
import logging
#from dotenv import load_dotenv
import traceback  # Import traceback module
import time
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import threading
import multiprocessing
import uuid
from multiprocessing import Value
import  sys
from multiprocessing import Manager

# Create a manager instance
#manager = Manager()

def get_public_ip(core_logger):
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
        core_logger.error(f"Error fetching IP address: {e}")
        return None

from duckduckgo_search import DDGS
import requests
from prometheus_client import start_http_server, Counter, Histogram
# Initialize Prometheus metrics server

start_http_server(8000)

# Define a Prometheus counter for API requests
api_requests_counter = Counter('api_requests_total', 'Total number of API requests')

#load_dotenv()  # Load environment variables

# Configure logging
import logging
import multiprocessing

from functools import partial
import queue

# Your existing imports and functions...

def process_chunks(queue, chunk_names):
    while True:
        try:
            chunk_data, chunk_name = queue.get(timeout=1)  # Timeout helps to periodically check for new chunks
            scrape_data(chunk_data, chunk_name)  # Process the chunk
            queue.task_done()  # Mark the task as done
        except queue.Empty:  # Queue is empty, no more chunks to process
            break

# Create a logger for the main log
main_logger = logging.getLogger('MainLogger')
main_logger.setLevel(logging.INFO)
main_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
main_file_handler = logging.FileHandler('main_log.log')
main_file_handler.setLevel(logging.INFO)
main_file_handler.setFormatter(main_formatter)
main_logger.addHandler(main_file_handler)

# Function to configure and return a logger for each core processor
def get_core_logger(core_id):
    core_logger = logging.getLogger(f'Core{core_id}Logger')
    core_logger.setLevel(logging.DEBUG)
    core_formatter = logging.Formatter('%(asctime)s - Core %(processName)s - %(levelname)s - %(message)s')
    core_file_handler = logging.FileHandler(f'core_{core_id}_log.log')
    core_file_handler.setLevel(logging.DEBUG)
    core_file_handler.setFormatter(core_formatter)
    core_logger.addHandler(core_file_handler)
    return core_logger


import pandas as pd
import os


# Function to combine all CSV files in the output folder into output.csv
def combine_output_files():
    output_folder = 'output'
    output_csv = 'output.csv'
    output_files = [file for file in os.listdir(output_folder) if file.endswith('.csv')]

    # Check if any CSV files exist in the output folder
    if not output_files:
        main_logger.info("No CSV files found in the output folder.")
        return

    # Initialize an empty DataFrame to hold the combined data
    combined_df = pd.DataFrame()

    # Loop through each CSV file and append its data to the combined DataFrame
    for file in output_files:
        file_path = os.path.join(output_folder, file)
        df = pd.read_csv(file_path)
        combined_df = combined_df.append(df, ignore_index=True)

    # Write the combined DataFrame to output.csv
    combined_df.to_csv(output_csv, index=False)
    main_logger.info(f"Combined data from {len(output_files)} CSV files into {output_csv}.")




ddgs = DDGS()
get_address_call_count = 0


def getAddress(word, core_logger, retries=0):
    global timeout_occurred, get_address_call_count, current_proxy, results, operation_completed
    timeout_occurred = False  # Reset timeout flag at the beginning of the function
    operation_completed = False  # Reset operation completed flag
    max_retries = 1
    backoff_factor = 0

    start_time = time.time()
    timeout_thread = threading.Thread(target=check_operation_timeout, args=(start_time, 10))  # 10 seconds timeout
    timeout_thread.start()

    if not word or word.strip() == "":
        core_logger.warning(f"Skipping empty search term: {word}")
        return []
    try:
        ddgs = DDGS()
        current_proxy = None
        # First attempt without proxy, if retries are 0 and no proxy set
        current_timer = threading.Timer(10, raise_exception)  # 10 seconds timeout
        current_timer.start()
        if retries == 0 and not current_proxy:
            core_logger.info(f"Running search with no proxy")
            results = ddgs.text(word, max_results=5, backend="api")
            operation_completed = True  # Indicate that the operation has completed to stop the timeout thread
            timeout_thread.join()  # Wait for the timeout thread to exit gracefully
            current_timer.cancel()  # Cancel the timer as soon as the operation completes
            core_logger.info(f"results{results}")
        else:
            # If first attempt fails, retry with proxy
            if not current_proxy:
                current_proxy = get_proxy_ip(core_logger)
            ddgs = DDGS(proxies=current_proxy)
            results = ddgs.text(word, max_results=5, backend="api")
            operation_completed = True  # Indicate that the operation has completed to stop the timeout thread
            timeout_thread.join()  # Wait for the timeout thread to exit gracefully
            current_timer.cancel()  # Cancel the timer as soon as the operation completes
            core_logger.info(f"Proxy results{results}")

        if timeout_occurred:
            core_logger.error("Operation timed out.")

        get_address_call_count += 1
        core_logger.info(f"Returning results{results}")
        return results

    except TimeoutException:
        core_logger.error("Timeout occurred in getAddress")
        operation_completed = True  # Ensure the flag is set even in case of exceptions

        return []  # Or signal the timeout in another appropriate way

    except Exception as e:
        core_logger.exception(f"Error occurred: {e}. Fetching a new proxy and retrying...")
        operation_completed = True  # Ensure the flag is set even in case of exceptions
        if retries < max_retries:
            current_proxy = get_proxy_ip(core_logger)  # Fetch a new proxy after failure
            sleep_time = backoff_factor ** retries
            core_logger.exception(f"Retrying in {sleep_time} seconds...")
            time.sleep(sleep_time)
            return getAddress(word, core_logger=core_logger, retries=retries + 1)
        else:
            core_logger.error(f"Skipping record after {max_retries} attempts. Error: {e}")
            return []

    finally:
        operation_completed = True
        timeout_thread.join()  # Ensure the thread is cleaned up properly


# Global counter for simulating rate limit hits
rate_limit_hits = 0

def simulate_rate_limit():
    global rate_limit_hits
    rate_limit_hits += 1
    if rate_limit_hits % 3 == 0:
        raise Exception("RatelimitException")

def get_timestamp():
    now = datetime.now()
    return now.strftime("%d/%m/%y %H:%M:%S")

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

def file_exists_in_current_directory(file_name):
    return os.path.exists(file_name)

used_proxies = []  # Initialize the list of used proxies as empty
current_proxy = None  # Initialize the current proxy as None
results = []
current_timer = None


# Use a shared variable to track timeout status
timeout_occurred = False
operation_completed = False

def check_operation_timeout(start_time, timeout_duration):
    global timeout_occurred, operation_completed
    while not operation_completed:
        if time.time() - start_time > timeout_duration:
            main_logger.error("Operation timed out.")
            timeout_occurred = True
            break
        time.sleep(1) 
        
class TimeoutException(Exception):
    pass

def reset_timeout_timer(timeout=10):
    global current_timer
    if current_timer:
        current_timer.cancel()  # Cancel the existing timer if it's running
    current_timer = threading.Timer(timeout, lambda: raise_exception())
    current_timer.start()

def raise_exception():
    global timeout_occurred
    timeout_occurred = True



def is_proxy_fast(proxy,core_logger):
    try:
        start_time = time.time()
        response = requests.get("https://www.google.com", proxies=proxy, timeout=5)
        latency = time.time() - start_time
        is_fast = response.status_code == 200 and latency < 1
        core_logger.info(f"Proxy latency: {latency:.2f} seconds. Status: {'Fast' if is_fast else 'Slow'}.")
        return is_fast
    except requests.RequestException:
        return False
    except:
        return False

    

def fetch_and_save_proxies(proxy_list_path, limit=50):
    url = 'https://free-proxy-list.net/'
    response = requests.get(url)
    proxies = []

    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        table = soup.find('table', {'class': 'table table-striped table-bordered'})
        row_count = 0
        for tr in table.find_all('tr'):
            if row_count >= limit:  # Stop when the limit is reached
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
            # If we can't fetch new proxies and no file exists, create an empty file.
            open(proxy_list_path, 'a').close()

def load_proxies(proxy_list_path):
    with open(proxy_list_path, 'r') as f:
        return [line.strip() for line in f.readlines()]

def save_proxies(proxy_list_path, proxies):
    with open(proxy_list_path, 'w') as f:
        f.write('\n'.join(proxies))

def get_proxy_ip(core_logger):
    proxy_list_path = 'available_proxies.txt'
    used_proxies_path = 'used_proxies.txt'

    # Load used proxies
    if os.path.exists(used_proxies_path):
        with open(used_proxies_path, 'r') as f:
            used_proxies = [line.strip() for line in f.readlines()]
    else:
        used_proxies = []

    # Check if available proxy list exists and has content; if not, fetch and save new proxies
    if not os.path.exists(proxy_list_path) or os.stat(proxy_list_path).st_size == 0:
        fetch_and_save_proxies(proxy_list_path)
    
    available_proxies = load_proxies(proxy_list_path)

    for proxy_url in available_proxies:
        protocol = "https" if "https://" in proxy_url else "http"
        proxy = {protocol: proxy_url}
        if proxy_url not in used_proxies and is_proxy_fast(proxy,core_logger):
            used_proxies.append(proxy_url)
            available_proxies.remove(proxy_url)

            # Save the updated lists
            save_proxies(proxy_list_path, available_proxies)
            save_proxies(used_proxies_path, used_proxies)

            core_logger.info(f"Using new proxy: {proxy_url}")
            return proxy

    # If no proxy is found (or all are slow), attempt to fetch new proxies
    core_logger.warning("No available fast proxies found, fetching new proxies...")
    fetch_and_save_proxies(proxy_list_path)
    return get_proxy_ip(core_logger)  # Recursive call to try again with the new list


def fetch_data_from_db():
    connection = psycopg2.connect(
        user="postgres",
        password="1BeldenUser",  # Consider using environment variables for sensitive data
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
    #fetch_data_query += " limit 3000"


    cursor.execute(fetch_data_query)
    records = cursor.fetchall()
    cursor.close()
    connection.close()
    return records

def write_failed_id(id):
    """Write a failed ID to the 'failed_ids.csv' file."""
    with open('failed_ids.csv', 'a') as file:
        file.write(f"{id}\n")

def start_timeout_timer(record_id, core_logger,timeout=10):
    """Starts a timer that marks the record as failed if it times out."""
    def timeout_action():
        core_logger.error(f"Timeout for ID {record_id}. Marking as failed.")
        write_failed_id(record_id)
    
    timer = threading.Timer(timeout, timeout_action)
    timer.start()
    return timer

def scrape_data(records_chunk, chunk_name, core_id):
    # Function to process a chunk of records
    core_logger = get_core_logger(core_id)
    core_logger.info(f"\nStarting processing for chunk: {chunk_name} on Core {core_id}\n")

    start_time = time.time()

    processed_count = 0
    output_file_path = 'output'
    write_header = not os.path.exists(output_file_path)
    
    for record in tqdm(records_chunk):
        timer = start_timeout_timer(record[0],core_logger=core_logger)  # Start the timeout timer
        try:  # Start of try block

            sleep(2.5)  # Respectful scraping delay
            core_logger.info(f"Processing record {record} in chunk: {chunk_name} on Core {core_id}")
            search_terms = [f'address {record[1]} {record[2]}', f'company {record[1]} website contact address {record[2]}']
            results = []
            for term in search_terms:
                results = getAddress(term,core_logger=core_logger)
                core_logger.info(f"Got Results {results}")
                if results:
                    break
            if results:
                for item in results:
                    if 'dnb.com' in item['href']:
                        continue  # Skip certain links
                    row = {
                        'id': record[0],
                        'url': item['href'].replace("'", "").replace('"', ''),
                        'body': unidecode(item['body']).replace("'", "").replace('"', '')
                    }
                    result_df = pd.DataFrame([row])
                    result_df.to_csv("output/"+output_file_path+"_"+chunk_name+".csv", mode='a', header=write_header, index=False)
                    write_header = False
            api_requests_counter.inc()  # Increment Prometheus counter for each API request
            main_logger.info("API Count"+api_requests_counter)
            core_logger.info("API Count" + api_requests_counter)
            ip_address = get_public_ip(core_logger)
            if ip_address:
                core_logger.info(f"\nPublic IP Address: {ip_address}")
            else:
                core_logger.error("\nFailed to fetch IP address.")

                # Increment the total_processed counter atomically
            #with total_processed.get_lock():
            #    total_processed.value += 1
            #    core_logger.info("\nTotal records processed: ",total_processed)

        except TimeoutException:
            # Handle the case where getAddress times out
            core_logger.error(f"\nTimeout while processing record  {record[0]} in chunk: {chunk_name} on Core {core_id}")
            write_failed_id(record[0])
            continue  # Skip to the next record

        except Exception as e:  # Exception is caught here
            core_logger.error(f"\nTimeout while processing record  {record[0]} in chunk: {chunk_name} on Core {core_id}")
            write_failed_id(record[0])  # Write the failed ID to file
            # Optionally, you can log the error or print a message here

        finally:
            timer.cancel()  # Cancel the timer as the record has been processed or failed        
            # Finally block ensures this code runs regardless of exceptions
            processed_count += 1  # Increment processed_count for each record attempted

    if write_header:
        # If no records were processed and the header still needs to be written, write an empty header
        pd.DataFrame(columns=['id', 'url', 'body']).to_csv(output_file_path, index=False)

    end_time = time.time()
    processing_time = end_time - start_time

    core_logger.info(
        f"Finished processing for chunk: {chunk_name} on Core {core_id}. Processing time: {processing_time:.2f} seconds")
    core_logger.info(f"Total records processed in the chunk {chunk_name}: {processed_count}")
    #core_logger.info(f"Total records processed overall : {total_processed}")
    core_logger.info("_______________________________________________________________________________\n\n")
    main_logger.info(
        f"Finished processing for chunk: {chunk_name} on Core {core_id}. Processing time: {processing_time:.2f} seconds")
    main_logger.info(f"Total records processed in the chunk {chunk_name}: {processed_count}")
    #main_logger.info(f"Total records processed overall : {total_processed}")
    main_logger.info("_______________________________________________________________________________\n\n")


def read_failed_ids():
    """Read failed IDs from 'failed_ids.csv' and return them as a list."""
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

def file_exists(file_name):
    return os.path.exists(file_name)

def fetch_data_from_db_and_save_to_csv(records_csv='db_records.csv'):
    """
    Fetch data from the database and save to a CSV file.
    """
    records = fetch_data_from_db()
    df = pd.DataFrame(records, columns=['id', 'name', 'billing_address'])
    df.to_csv(records_csv, index=False)
    return df

def read_records_from_csv(records_csv='db_records.csv'):
    """
    Read records from a CSV file.
    """
    return pd.read_csv(records_csv)
    
def callback_function(result, core_logger):

    core_logger.info(f"Chunk processed, result: {result}")
    main_logger.info(f"Chunk processed, result: {result}")

def process_records():
    check_and_create_output_directory()
    records_csv = 'db_records.csv'
    records = []

    # Check if the CSV file exists. If it does, read from it, otherwise fetch from the DB and save.
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

    processed_ids = get_processed_ids()
    failed_ids = read_failed_ids()  # Get failed IDs
    main_logger.info(f"Count of Processed ids:  {str(len(processed_ids))}")
    main_logger.info(f"Count of overall records: {str(len(records))}")
    records_to_process = [record for record in records if record[0] not in processed_ids and record[0] not in failed_ids]
    main_logger.info(f"Count of records to process: {str(len(records_to_process))}")
    if not records_to_process:
        main_logger.info("No new records to process.")
        return False

    #Multiprocessing
    # Print number of available CPU cores
    num_cores = multiprocessing.cpu_count()
    main_logger.info(f"Number of CPU cores available: {num_cores}")

    # Calculate the chunk size and total number of chunks
    chunk_size = 100
    total_records = len(records_to_process)
    total_chunks = (total_records + chunk_size - 1) // chunk_size  # Ensure all records are assigned

    # Generate unique chunk names
    chunk_names = [str(uuid.uuid4()) for _ in range(total_chunks)]

    # Split records into chunks
    records_chunks = [records_to_process[i:i + chunk_size] for i in range(0, total_records, chunk_size)]

    # Print total number of chunks
    total_chunks = len(records_chunks)
    main_logger.info(f"Single Chunk Size: {chunk_size}")
    main_logger.info(f"Total chunks: {total_chunks}")
    main_logger.info("Total chunk_names: " + str(len(chunk_names)))

    # Create a shared counter for total processed records using the manager
    #total_processed = manager.Value('i', 0)

    main_logger.info("\n\nStarting Multithread Processing.....\n\n")

    # Create a multiprocessing Queue
    queue = multiprocessing.Queue()

    # Process each chunk using multiprocessing
    with multiprocessing.Pool(processes=num_cores) as pool:
        # Use starmap to pass additional arguments (chunk_name, core_id) to the process function
        pool.starmap(scrape_data, [(chunk, chunk_names[i], i % num_cores) for i, chunk in
                                             enumerate(records_chunks)])
    main_logger.info("\n\nAll chunks have been processed.\n\n")

    return True

def main():
    start_time_main = time.time()
    process_records()
    main_logger.info("Checking for more records to process...")
    main_logger.info("Combining all output files.....")
    # Call the function to combine the output files
    combine_output_files()
    # End the timer
    end_time_main = time.time()
    # Calculate the execution time
    execution_time_main = end_time_main - start_time_main
    main_logger.info(f"Execution time: {execution_time_main:.2f} seconds.")
    main_logger.info("\n\nEnd of Processing! ")

if __name__ == "__main__":

    main()
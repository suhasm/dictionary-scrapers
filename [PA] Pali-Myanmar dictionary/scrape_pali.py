import csv
import logging
import os
from typing import List, Dict, Any
import concurrent.futures

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

# Constants
BASE_URL = "https://pm12e.pali.tools/word/"
CSV_FILENAME = 'pali_words.csv'
CHECKPOINT_FILENAME = 'scraping_checkpoint.txt'
BATCH_SIZE = 100
MAX_WORD_ID = 204049
PARALLEL_WORKERS = 100  # New constant for parallel processing

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_last_processed_id() -> int:
    """Read the last processed ID from the checkpoint file."""
    try:
        with open(CHECKPOINT_FILENAME, 'r') as f:
            return int(f.read().strip())
    except FileNotFoundError:
        return 0

def save_checkpoint(word_id: int) -> None:
    """Save the current progress to the checkpoint file."""
    with open(CHECKPOINT_FILENAME, 'w') as f:
        f.write(str(word_id))

def scrape_word_data(word_id: int) -> Dict[str, Any]:
    """Scrape data for a single word."""
    url = f"{BASE_URL}{word_id}"
    try:
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')

        data = {
            'Entry Number': word_id,
            'URL': url,
            'Roman Pali': '',
            'Myanmar Pali': '',
            'Roman Breakup': '',
            'Myanmar Breakup': '',
            'Myanmar Definition': '',
            'English Definition': ''
        }

        # Extract data from input fields
        for field in ['roman_pali', 'myanmar_pali', 'roman_breakup', 'myanmar_breakup']:
            input_field = soup.find('input', {'id': f'id_{field}'})
            if input_field:
                data[field.replace('_', ' ').title()] = input_field.get('value', '')

        # Extract data from textarea fields
        for field in ['myanmar_definition', 'english_definition']:
            textarea = soup.find('textarea', {'id': f'id_{field}'})
            if textarea:
                data[field.replace('_', ' ').title()] = textarea.text.strip()

        return data
    except Exception as e:
        logging.error(f"Error on word ID {word_id}: {e}")
        return None

def write_to_csv(data: List[Dict[str, Any]], mode: str = 'a') -> None:
    """Write data to CSV file."""
    with open(CSV_FILENAME, mode, newline='', encoding='utf-8') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=data[0].keys())
        if mode == 'w':
            writer.writeheader()
        writer.writerows(data)

def print_data_table(data: List[Dict[str, Any]]) -> None:
    """Print data as a table using pandas DataFrame."""
    df = pd.DataFrame(data)
    print(df)

def main() -> None:
    start_id = get_last_processed_id() + 1
    logging.info(f"Starting scraping from word ID {start_id}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as executor:
        for batch_start in range(start_id, MAX_WORD_ID + 1, BATCH_SIZE):
            batch_end = min(batch_start + BATCH_SIZE, MAX_WORD_ID + 1)
            future_to_word_id = {executor.submit(scrape_word_data, word_id): word_id for word_id in range(batch_start, batch_end)}
            
            data_batch = []
            for future in concurrent.futures.as_completed(future_to_word_id):
                word_id = future_to_word_id[future]
                try:
                    word_data = future.result()
                    if word_data:
                        data_batch.append(word_data)
                except Exception as e:
                    logging.error(f"Error on word ID {word_id}: {e}")

            if data_batch:
                write_to_csv(data_batch, 'a' if batch_start > start_id else 'w')
                print_data_table(data_batch)
                save_checkpoint(batch_end - 1)
                logging.info(f"Processed entries up to {batch_end - 1}")

    logging.info("Finished scraping and writing to CSV file")

if __name__ == "__main__":
    main()
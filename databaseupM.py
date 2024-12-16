import psycopg2
from psycopg2 import sql
import requests
import datetime
import time
import pytz
import warnings
from dateutil import parser
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib3.exceptions import InsecureRequestWarning
from collections import defaultdict 
from datetime import date, timedelta
import logging
from logging.handlers import RotatingFileHandler

# Disable urllib3 InsecureRequestWarning
warnings.filterwarnings('ignore', category=InsecureRequestWarning)

# Set up rotating log handler
log_file = '/var/log/energy_data_collector.log'
max_log_size = 10 * 1024 * 1024  # 10 MB
backup_count = 5  # Number of backup files to keep

logging.info("Script started")
print("Script started")

# Create a rotating file handler
file_handler = RotatingFileHandler(
    log_file, 
    maxBytes=max_log_size, 
    backupCount=backup_count
)

# Create a formatter and add it to the handler
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
file_handler.setFormatter(formatter)

# Get the root logger and add the file handler to it
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)

class APIReader:

    COUNTRY_CODE = "RS"  # RS for Serbia

    @staticmethod
    def create_session():
        session = requests.Session()
        retries = Retry(total=5, 
                        backoff_factor=1, 
                        status_forcelist=[429, 500, 502, 503, 504],
                        method_whitelist=["HEAD", "GET", "OPTIONS"])
        session.mount("https://", HTTPAdapter(max_retries=retries))
        return session

    def __init__(self):
        APIReader.self = self

    def return_daylight():
        # Set timezone to Central European Time
        serbia_tz = pytz.timezone("Europe/Belgrade")
        # Get current time in Serbia
        current_time = datetime.datetime.now(serbia_tz)
        # Determine whether daily saving time is in effect in order to set adjustment
        daylight_saving = current_time.dst() != timedelta(0)
        return daylight_saving

    @staticmethod
    def collect_data(day):
        logging.info(f"Collecting data for {day}")
        session = APIReader.create_session()

        yesterday = (datetime.datetime.strptime(day, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
        tomorrow = (datetime.datetime.strptime(day, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')

        site_urlcon = "https://ems.energyflux.rs/"
        site_urlprod = "https://ems.energyflux.rs/"
        api_urlcon = f"{site_urlcon}api/consumption/SRBIJA?fromDate={yesterday}T22:00:00.000Z&toDate={tomorrow}T21:59:59.999Z"
        api_urlprod = f"{site_urlprod}api/production/SRBIJA?sourceType=NONE&fromDate={yesterday}T22:00:00.000Z&toDate={tomorrow}T21:59:59.999Z"

        max_retries = 5
        for attempt in range(max_retries):
            try:
                response_c = session.get(api_urlcon, verify=False, timeout=30)
                response_c.raise_for_status()
                data_c = response_c.json()

                response_p = session.get(api_urlprod, verify=False, timeout=30)
                response_p.raise_for_status()
                data_p = response_p.json()

                # Process data_c and data_p as before
                energy_p = {}
                energy_c = {}
                
                if "production" in data_p:
                    production_data = data_p["production"]
                    for source, values in production_data.items():
                        energy_p[source] = {item["t"]: item["v"] for item in values}
                
                if "consumption" in data_c:
                    consumption_data = data_c["consumption"]
                    energy_c = {item["t"]: item["v"] for item in consumption_data}

                logging.info(f"Successfully collected data for {day}")
                return [energy_c, energy_p]

            except (requests.exceptions.RequestException, ValueError) as e:
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt  # exponential backoff
                    logging.warning(f"Error occurred: {e}. Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logging.error(f"Failed to fetch data for {day} after {max_retries} attempts.")
                    return None

    @staticmethod
    def print_data():
        # Method implementation...
        logging.info("Printing data")

    @staticmethod
    def collect_historical_data(start_date, end_date):
        logging.info(f"Collecting historical data from {start_date} to {end_date}")
        all_data = []
        current_date = start_date
        while current_date <= end_date:
            logging.info(f"Collecting data for {current_date}")
            day_data = APIReader.collect_data(current_date.strftime('%Y-%m-%d'))
            if day_data is not None:
                all_data.append((current_date, day_data))
            else:
                logging.warning(f"Skipping {current_date} due to data collection failure.")
            current_date += timedelta(days=1)
            time.sleep(1)  # Add a delay between requests to avoid overwhelming the server
        return all_data 
    
    @staticmethod
    def insert_into_database(data, db_params):
        logging.info("Starting database insertion")
        conn = None
        cur = None
        try:
            conn = psycopg2.connect(**db_params)
            cur = conn.cursor()

            insert_query = sql.SQL("""
                INSERT INTO energy_production_consumption
                (country, date, hour, minute, consumption, production_vetro, production_pumpa, production_gas, production_hidro, production_termo)  
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (country, date, hour, minute) DO UPDATE SET
                consumption = EXCLUDED.consumption,
                production_vetro = EXCLUDED.production_vetro,
                production_pumpa = EXCLUDED.production_pumpa,
                production_gas = EXCLUDED.production_gas,
                production_hidro = EXCLUDED.production_hidro,
                production_termo = EXCLUDED.production_termo                   
            """)

            for date, (energy_c, energy_p) in data:
                timepoints = set(energy_c.keys())
                for source in energy_p.values():
                    timepoints.update(source.keys())
                
                for timepoint in sorted(timepoints):
                    try:
                        local_time = parser.parse(timepoint)
                        if local_time.tzinfo is None:
                            local_time = pytz.utc.localize(local_time)
                        local_time = local_time.astimezone(pytz.timezone("Europe/Belgrade"))
                        
                        if local_time.date() == date:
                            consumption = energy_c.get(timepoint, None)
                            production_vetro = energy_p.get('VETRO', {}).get(timepoint, None)
                            production_pumpa = energy_p.get('PUMPA', {}).get(timepoint, None)
                            production_gas = energy_p.get('GAS', {}).get(timepoint, None)
                            production_hidro = energy_p.get('HIDRO', {}).get(timepoint, None)
                            production_termo = energy_p.get('TERMO', {}).get(timepoint, None)

                            cur.execute(insert_query, (
                                APIReader.COUNTRY_CODE,
                                local_time.date(),
                                local_time.hour,
                                local_time.minute,
                                consumption,
                                production_vetro,
                                production_pumpa,
                                production_gas,
                                production_hidro,
                                production_termo
                            ))
                            logging.info(f"Inserted data for {local_time}")
                    except ValueError as e:
                        logging.error(f"Unable to parse time: {timepoint}. Error: {e}")
                        continue

            conn.commit()
            logging.info("Data successfully inserted into the database.")

        except (Exception, psycopg2.Error) as error:
            logging.error(f"Error while connecting to PostgreSQL or inserting data: {error}")
            if conn:
                conn.rollback()

        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()
            logging.info("Database connection closed.")

    @staticmethod
    def process_recent_data(db_params):
        end_date = datetime.date.today()
        start_date = end_date - datetime.timedelta(days=60)  # Last 2 months

        logging.info(f"Processing recent data from {start_date} to {end_date}")
        recent_data = APIReader.collect_historical_data(start_date, end_date)
        
        APIReader.insert_into_database(recent_data, db_params)
        
        logging.info("Data collection and insertion completed.")

        # Check for missing dates and timepoints
        APIReader.check_data_completeness(recent_data, start_date, end_date)

    @staticmethod
    def check_data_completeness(data, start_date, end_date):
        logging.info("Checking data completeness")
        collected_dates = set(date for date, _ in data)
        all_dates = set(start_date + datetime.timedelta(days=x) for x in range((end_date - start_date).days + 1))
        
        missing_dates = all_dates - collected_dates
        if missing_dates:
            logging.warning("Missing dates detected:")
            for date in sorted(missing_dates):
                logging.warning(date.strftime("%Y-%m-%d"))
        else:
            logging.info("No missing dates.")

        missing_timepoints = defaultdict(list)
        for date, (energy_c, energy_p) in data:
            timepoints = set(energy_c.keys())
            for source in energy_p.values():
                timepoints.update(source.keys())
            
            if len(timepoints) < 144:
                missing_count = 144 - len(timepoints)
                missing_timepoints[date].append(f"{missing_count} timepoints")

        if missing_timepoints:
            logging.warning("Dates with missing timepoints:")
            for date, missing in missing_timepoints.items():
                logging.warning(f"{date.strftime('%Y-%m-%d')}: {', '.join(missing)}")
        else:
            logging.info("No missing timepoints in any day.")

# Database connection parameters
db_params = {
    'host': 'agptl-finaldb-cluster.cluster-cf4kquqco6iv.eu-central-1.rds.amazonaws.com',
    'port': 5432,
    'user': 'AlphaGPTLabs',
    'password': '***alphagptlabs11',  
    'database': 'postgres'
}

logging.info("Script execution started")
APIReader.process_recent_data(db_params)
logging.info("Script execution completed")

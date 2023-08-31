import mysql.connector
import csv
from decimal import *
from mysql.connector import errorcode
from datetime import datetime
from config import (DB_HOST,
                    DB_PORT,
                    DB_NAME,
                    DB_USERNAME,
                    DB_PASSWORD,
                    PCD_FILE_PATH,
                    TIMESTAMP_FORMAT)


def time_now(TIMESTAMP_FORMAT):
    ''' Get formatted timestamp '''
    return datetime.now().strftime(TIMESTAMP_FORMAT)

def connect_to_db(DB_HOST, DB_USERNAME, DB_PASSWORD, DB_NAME, DB_PORT):
    """ Connect to DB and create user and db if needed """
    try:
        connector = define_connection(DB_HOST, DB_USERNAME, DB_PASSWORD, DB_NAME, DB_PORT)
        print(f"{time_now(TIMESTAMP_FORMAT)}\tConnection to database {DB_NAME} on {DB_HOST}:{DB_PORT} successfully established")
        return connector
    except mysql.connector.errors.Error as db_error:
        print(f"{time_now(TIMESTAMP_FORMAT)}\t{db_error.msg}")
        if db_error.errno not in (errorcode.ER_BAD_DB_ERROR, errorcode.ER_ACCESS_DENIED_ERROR):
            quit()
        if db_error.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            quit()
        if db_error.errno == errorcode.ER_BAD_DB_ERROR:
            print(f"{time_now(TIMESTAMP_FORMAT)}\tDatabase {DB_NAME} does not exist on host {DB_HOST}")
            quit()
        connector = connect_to_db()
        return connector
    
def define_connection(db_host, db_user, db_password, db_name, db_port=3306):
    """ Define DB connection """
    db_connector = mysql.connector.connect(
        host=db_host,
        user=db_user,
        password=db_password,
        database=db_name,
        port = db_port
    )
    print(f"{time_now(TIMESTAMP_FORMAT)}\tEstablishing connection to database {db_name} on {db_host}...")
    return db_connector

def download_db():
    print(f"{time_now(TIMESTAMP_FORMAT)}\tDownloading from database...")
    download_query = """
    SELECT pcd, latitude, longitude 
    FROM postcodes
    """
    pcd_cursor.execute(download_query)
    db_records = pcd_cursor.fetchall()
    db_dict = {}
    for record in db_records:
        record_dict = {record[0]:{
            "lat": record[1].normalize(), 
            "long": record[2].normalize()}}
        db_dict.update(record_dict)
    return db_dict

def upsert_record(pcd, coords):
    upsert_query = """
    INSERT INTO postcodes (pcd, latitude, longitude, created_at, updated_at)
    VALUES (%(pcd)s, %(lat)s, %(long)s, current_timestamp(), current_timestamp())
        ON DUPLICATE KEY UPDATE latitude=%(lat)s, longitude=%(long)s, updated_at=current_timestamp();
    """
    record_to_upsert = {"pcd": pcd}
    record_to_upsert.update(coords)
    pcd_cursor.execute(upsert_query, record_to_upsert)
    pcd_db.commit()

def delete_record(pcd_record):
    upsert_query = """
    DELETE FROM postcodes where pcd = %(pcd)s;
    """
    record_to_delete = {"pcd": next(iter(pcd_record))}
    pcd_cursor.execute(upsert_query, record_to_delete)
    pcd_db.commit()

def read_csv(PCD_FILE_PATH):
    pcd_csv_dict = {}
    with open(PCD_FILE_PATH, 'r') as pcd_csv:
        print(f"{time_now(TIMESTAMP_FORMAT)}\tImporting CSV file...")
        pcd_reader = csv.reader(pcd_csv,)
        next(pcd_reader, None)
        for row in pcd_reader:
            pcd_record = {row[0]:{
                        "lat": Decimal(row[1]), 
                        "long": Decimal(row[2])}}
            pcd_csv_dict.update(pcd_record)
    return pcd_csv_dict

def compare_dictionaries(old_values, new_values):
    print(f"{time_now(TIMESTAMP_FORMAT)}\tComparing...")
    records_to_upsert = {}
    records_to_remove = {}

    common_keys = old_values.keys() & new_values.keys()
    for key in common_keys:
        if old_values[key] != new_values[key]:
            records_to_upsert[key] = new_values[key]

    for key in old_values.keys() - common_keys:
        records_to_remove[key] = old_values[key]

    for key in new_values.keys() - common_keys:
        records_to_upsert[key] = new_values[key]
        
    print(f"{time_now(TIMESTAMP_FORMAT)}\tComparison complete. {len(records_to_upsert)} records to insert/update, {len(records_to_remove)} records to delete.")
    return records_to_upsert, records_to_remove

pcd_db = connect_to_db(DB_HOST, DB_USERNAME, DB_PASSWORD, DB_NAME, DB_PORT)
pcd_cursor = pcd_db.cursor()
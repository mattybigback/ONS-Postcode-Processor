
from libonspostcodes import *

def process_deletion_list(deletion_list):
    if len(deletion_list) == 0:
        return
    print(f"{time_now(TIMESTAMP_FORMAT)}\tDeleting {len(deletion_list)} records")
    for item in deletion_list.items():
        delete_record(item)

def process_upsert_list(upsert_list):
    if len(upsert_list) == 0:
        return
    print(f"{time_now(TIMESTAMP_FORMAT)}\tInserting/Updating {len(upsert_list)} records")
    for pcd, coords in upsert_list.items():
        upsert_record(pcd, coords)

getcontext().prec=12
csv_files = scan_directory(INPUT_CSV_DIR)
csv_dict = {}
for file_name, file_properties in scan_directory(INPUT_CSV_DIR).items():
    csv_dict.update(read_csv(validate_csv(csv_files[file_name])))

if OUTPUT_CSV is True:
    output_csv(csv_dict, OUTPUT_CSV_PATH)

upsert_records, delete_records = compare_dictionaries(download_db(), csv_dict)
process_deletion_list(delete_records)
process_upsert_list(upsert_records)
print(f"{time_now(TIMESTAMP_FORMAT)}\tComplete!")

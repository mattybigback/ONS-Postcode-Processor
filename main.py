from libonspostcodes import *

getcontext().prec=12
upsert_records, delete_records = compare_dictionaries(download_db(), read_csv(PCD_FILE_PATH))

print(f"{time_now(TIMESTAMP_FORMAT)}\tDeleting {len(delete_records)} records")
for item in delete_records.items():
    delete_record(item)

print(f"{time_now(TIMESTAMP_FORMAT)}\tInserting/Updating {len(upsert_records)} records")
for pcd, coords in upsert_records.items():
    upsert_record(pcd, coords)

print(f"{time_now(TIMESTAMP_FORMAT)}\tComplete!")

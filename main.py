def compare_dictionaries(old_values, new_values):
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

    return records_to_upsert, records_to_remove

# Example dictionaries
old_values = {'a': {"lat": 30, "long": 40},
         'b': {"lat": 31, "long": 40},
         'c': {"lat": 32, "long": 40},
         'd': {"lat": 33, "long": 40},
         'e': {"lat": 34, "long": 40}}
new_values = {'a': {"lat": 30, "long": 40},
         'b': {"lat": 32, "long": 40},
         'c': {"lat": 33, "long": 40},
         'd': {"lat": 34, "long": 40},
         'f': {"lat": 30, "long": 40}}

records_to_upsert, records_to_remove = compare_dictionaries(old_values, new_values)

print("Keys to add/update:", records_to_upsert)
print("Keys to remove:", records_to_remove)
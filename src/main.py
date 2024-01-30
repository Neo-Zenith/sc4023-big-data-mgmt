import csv
import struct
import os
import sys
from constants import INPUT_PATH


class ColumnStore:
    def __init__(self, column_files):
        self.column_files = column_files
        self.zone_maps = {}

    def create_zone_maps(self, columns_of_interest, zone_size=25000):
        for column_name in columns_of_interest:
            column_files = self.column_files.get(column_name)
            if not column_files:
                print(f"Column '{column_name}' not found.")
                continue

            zone_maps = []
            for file_path in column_files:
                zone_map = ZoneMap(file_path, column_name, zone_size)
                zone_map.build_zone_map()
                zone_maps.append(zone_map)

            self.zone_maps[column_name] = zone_maps


class ZoneMap:
    def __init__(self, column_file, column_name, zone_size=50000):
        self.column_file = column_file
        self.column_name = column_name
        self.zone_size = zone_size
        self.zone_map = {}  # Dictionary to store zone metadata

    def build_zone_map(self):
        with open(self.column_file, 'r', encoding='utf-8') as file:
            zone_start = 0
            while True:
                data = file.read(self.zone_size)

                if not data:
                    break

                values = [int(value) if value.isdigit()
                          else value for value in data.split()]
                min_val, max_val = min(values) if values else None, max(
                    values) if values else None

                self.zone_map[zone_start] = {
                    'min_idx': zone_start,
                    'max_idx': zone_start + len(values) - 1,
                    'min_val': min_val,
                    'max_val': max_val
                }
                print(self.zone_map[zone_start])

                zone_start += self.zone_size

    def get_zone_metadata(self, zone_start):
        return self.zone_map.get(zone_start, {})


class DataWriter:
    def __init__(self, csv_file_path, output_folder, columns_of_interest, chunk_size=50000):
        self.csv_file_path = csv_file_path
        self.output_folder = output_folder
        self.columns_of_interest = columns_of_interest
        self.chunk_size = chunk_size

    def write_columns_to_txt(self):
        with open(self.csv_file_path, 'r', newline='', encoding='utf-8') as csv_file:
            reader = csv.DictReader(csv_file)
            column_files = {}

            for column_name in self.columns_of_interest:
                column_files[column_name] = []
                chunk_count = 0

                with open(os.path.join(self.output_folder, f"{column_name}_chunk_{chunk_count}.txt"), 'w') as txt_file:
                    for row_count, row in enumerate(reader, start=1):
                        entry = str(row[column_name])
                        txt_file.write(entry + '\n')

                        # Close the current file and start a new one
                        if row_count % self.chunk_size == 0:
                            txt_file.close()
                            column_files[column_name].append(txt_file.name)
                            chunk_count += 1
                            txt_file = open(os.path.join(
                                self.output_folder, f"{column_name}_chunk_{chunk_count}.txt"), 'w')

                # Close the last file if not yet closed
                txt_file.close()
                column_files[column_name].append(txt_file.name)

                # Reset the file pointer for the next column
                csv_file.seek(0)

        return column_files


csv_file_path = INPUT_PATH
output_folder = 'processed'
columns_of_interest = ['month', 'town', 'floor_area_sqm', 'resale_price']

data_writer = DataWriter(csv_file_path, output_folder, columns_of_interest)
column_files = data_writer.write_columns_to_txt()

# Now, column_files will be a dictionary mapping column names to lists of their respective binary file paths
print(column_files)

# Create a ColumnStore and add the columns
column_store = ColumnStore(column_files)
column_store.create_zone_maps(columns_of_interest)

# Get metadata for a specific zone in the "month" column
zone_map_month = column_store.zone_maps.get('month')
if zone_map_month:
    zone_start_position = 0
    zone_metadata_month = zone_map_month.get_zone_metadata(zone_start_position)
    print(
        f"Zone Metadata for Zone at {zone_start_position} in 'month': {zone_metadata_month}")

# Get metadata for a specific zone in the "town" column
zone_map_town = column_store.zone_maps.get('town')
if zone_map_town:
    zone_start_position = 0
    zone_metadata_town = zone_map_town.get_zone_metadata(zone_start_position)
    print(
        f"Zone Metadata for Zone at {zone_start_position} in 'town': {zone_metadata_town}")

# Get metadata for a specific zone in the "floor_area_sqm" column
zone_map_floor_area_sqm = column_store.zone_maps.get('floor_area_sqm')
if zone_map_floor_area_sqm:
    zone_start_position = 0
    zone_metadata_floor_area_sqm = zone_map_floor_area_sqm.get_zone_metadata(
        zone_start_position)
    print(
        f"Zone Metadata for Zone at {zone_start_position} in 'floor_area_sqm': {zone_metadata_floor_area_sqm}")

# Get metadata for a specific zone in the "resale_price" column
zone_map_resale_price = column_store.zone_maps.get('resale_price')
if zone_map_resale_price:
    zone_start_position = 0
    zone_metadata_resale_price = zone_map_resale_price.get_zone_metadata(
        zone_start_position)
    print(
        f"Zone Metadata for Zone at {zone_start_position} in 'resale_price': {zone_metadata_resale_price}")

import os
import csv
from typing import Dict, List
from constants import DISK_FOLDER, INPUT_PATH, TOWN_MAPPING, MAX_FILE_LINES
from datetime import datetime, timedelta
from enum import Enum


class ColumnsOfInterest(Enum):
    TOWN = 'town'
    MONTH = 'month'
    FLOOR_AREA_SQM = 'floor_area_sqm'
    RESALE_PRICE = 'resale_price'


class ColumnStore:
    def __init__(self, csv_file_path: str, disk_folder: str, columns_of_interest: List[ColumnsOfInterest], max_file_lines=MAX_FILE_LINES):
        self.csv_file_path = csv_file_path
        self.disk_folder = disk_folder
        # this ordering is important -> it dictates which column will be processed first when building zone maps
        self.columns_of_interest = columns_of_interest
        self.max_file_lines = max_file_lines
        self.zone_maps: Dict[str, List[ZoneMap]] = {}

        # initialize ZoneMap for interested columns
        for column_name in columns_of_interest:
            self.zone_maps[column_name] = []

    def process_csv(self):
        idx = 0  # line index
        zone_count = 0
        opened_files = {}

        with open(self.csv_file_path, 'r', newline='', encoding='utf-8') as csv_file:
            reader = csv.DictReader(csv_file)
            for _, row in enumerate(reader, start=1):
                # new zone -> create file and update min index
                if idx % self.max_file_lines == 0:
                    for col_name in self.columns_of_interest:
                        opened_files[col_name] = open(os.path.join(
                            self.disk_folder, f"{col_name}_chunk_{zone_count}.txt"), 'w')
                        self.zone_maps[col_name].append(
                            ZoneMap(col_name, zone_count))
                        self.zone_maps[col_name][-1].set_min_idx(idx)
                    zone_count += 1

                mapped_value = 0
                for column_name in self.columns_of_interest:
                    value = row[column_name]
                    if column_name == 'town':
                        value = TOWN_MAPPING[value] if value in TOWN_MAPPING else -1
                        if value == -1:
                            # flag to skip the rest of the columns
                            mapped_value = -1
                            break

                        self.zone_maps[column_name][-1].update_zone_map(
                            value)
                    elif column_name == 'month':
                        self.zone_maps[column_name][-1].update_zone_map(value)

                    file = opened_files[column_name]
                    file.write(f"{value}\n")

                # skip the rest of the columns if the town is not in the mapping
                if mapped_value == -1:
                    continue

                # end of zone -> close the current files and update max index
                if idx % self.max_file_lines == self.max_file_lines - 1:
                    for col_name in self.columns_of_interest:
                        opened_files[col_name].close()
                        self.zone_maps[col_name][-1].set_max_idx(idx)

                idx += 1

        # close the last set of files and update max index
        for col_name in self.columns_of_interest:
            opened_files[col_name].close()
            self.zone_maps[col_name][-1].set_max_idx(idx)

    def get_zone_maps(self):
        return self.zone_maps


class ZoneMap:
    def __init__(self, column_name, zone_count):
        self.column_name = column_name
        self.zone_count = zone_count
        self.data = {
            'min_idx': float('inf'),
            'max_idx': float('-inf'),
        }

        if column_name == 'month':
            self.data['min_month'] = '9999-01'
            self.data['max_month'] = '0001-01'
        elif column_name == 'town':
            self.data['min_town'] = float("inf")
            self.data['max_town'] = -1

    def set_min_idx(self, min_idx):
        self.data['min_idx'] = min_idx

    def set_max_idx(self, max_idx):
        self.data['max_idx'] = max_idx

    def update_zone_map(self, value):
        if self.column_name == 'month':
            min_month = self.data['min_month']
            max_month = self.data['max_month']

            self.data['min_month'] = min(min_month, value)
            self.data['max_month'] = max(max_month, value)
        elif self.column_name == 'town':
            min_town = self.data['min_town']
            max_town = self.data['max_town']

            self.data['min_town'] = min(min_town, value)
            self.data['max_town'] = max(max_town, value)

    def get_zone_map(self):
        return self.data

    def get_zone_count(self):
        return self.zone_count


class QueryProcessor:
    def __init__(self, year: int, month: int, town: int, column_store: ColumnStore):
        self.year = year
        self.month = month
        self.town = town
        self.column_store = column_store
        self.buffer_folder = 'temp'
        self.num_buffer_folders = 0

    def process_year_and_month(self, column_name: ColumnsOfInterest = 'month'):
        print("=" * 60)
        print(f"Processing year and month...")
        zone_maps = self.column_store.get_zone_maps()
        zone_map_arr = zone_maps[column_name]
        start_value = f"{self.year}-{self.month:02}"
        start_date = datetime.strptime(start_value, "%Y-%m")
        end_date = start_date + timedelta(days=3*30)
        end_value = end_date.strftime("%Y-%m")

        # Iterate through ZoneMaps for the specified column
        for zone_map in zone_map_arr:
            zone_data = zone_map.get_zone_map()
            zone_count = zone_map.get_zone_count()

            # Check if the specified year and month fall within the range
            if zone_data['min_month'] <= start_value <= zone_data['max_month'] \
                    or zone_data['min_month'] <= end_value <= zone_data['max_month']:
                # Process the split files within the zone
                self.process_split_files(
                    column_name, zone_count, start_value, end_value)

    def process_towns(self, column_name='town'):
        print("=" * 60)
        print(f"Processing towns...")

        # Read the indexes from the previous month's temp folder
        indexes = []
        start, end = float('inf'), float('-inf')
        for i in range(self.num_buffer_folders + 1):
            month_temp_file_path = os.path.join(
                self.buffer_folder, f"month_chunk_{i}.txt")

            if not os.path.exists(month_temp_file_path):
                print(f"Error: {month_temp_file_path} not found.")
                continue

            with open(month_temp_file_path, 'r', encoding='utf-8') as month_temp_file:
                indexes += [int(line.split(" ")[1])
                            for line in month_temp_file]
                start = min(start, indexes[0])
                end = max(end, indexes[-1])
        print(start, end)

        # Find the zone containing the indexes
        for zone_map in self.column_store.get_zone_maps()[column_name]:
            min_idx, max_idx = \
                zone_map.get_zone_map()['min_idx'], zone_map.get_zone_map()[
                    'max_idx']
            if min_idx <= start <= max_idx or min_idx <= end <= max_idx:
                print(
                    f"Found the zone containing the indexes: {zone_map.get_zone_count()}")
                # Process the split files within the target zone
                self.process_split_files(column_name, zone_map.get_zone_count(), str(
                    self.town), str(self.town), indexes)

    def process_split_files(self, column_name: str, zone_count: int, start, end, indexes: list = []):
        directory = self.column_store.disk_folder
        file_path = os.path.join(
            directory, f"{column_name}_chunk_{zone_count}.txt")
        lower_bound = zone_count * MAX_FILE_LINES

        # Keep track of the lines processed within the current split
        lines_processed = 0

        # Temporary output file to store lines for the current split
        temp_output_file = None

        # Sequential scan
        with open(file_path, 'r', encoding='utf-8') as file:
            if indexes:
                content = file.readlines()
                for index in indexes:
                    # Seek to the index positions
                    offset = index - zone_count * MAX_FILE_LINES
                    # Process the lines from the index positions
                    line = content[offset - 1]
                    value = line.rstrip()

                    if start <= value <= end:
                        # If lines_processed reaches MAX_FILE_LINES, close the current temporary file
                        if lines_processed % MAX_FILE_LINES == 0:
                            if temp_output_file:
                                temp_output_file.close()

                            # Create a new temporary file
                            temp_output_file_path = os.path.join(
                                self.buffer_folder, f"{column_name}_chunk_{lines_processed // MAX_FILE_LINES}.txt")
                            temp_output_file = open(
                                temp_output_file_path, 'w', encoding='utf-8')

                        # Write the line to the current temporary file
                        temp_output_file.write(f"{value} {index}\n")
                        lines_processed += 1
            else:
                # Process the lines sequentially
                for offset, line in enumerate(file):
                    value = line.rstrip()

                    if start <= value <= end:
                        # If lines_processed reaches MAX_FILE_LINES, close the current temporary file
                        if lines_processed % MAX_FILE_LINES == 0:
                            if temp_output_file:
                                temp_output_file.close()

                            # Create a new temporary file
                            temp_output_file_path = os.path.join(
                                self.buffer_folder, f"{column_name}_chunk_{lines_processed // MAX_FILE_LINES}.txt")
                            temp_output_file = open(
                                temp_output_file_path, 'w', encoding='utf-8')

                        # Write the line to the current temporary file
                        temp_output_file.write(
                            f"{value} {lower_bound + offset}\n")
                        lines_processed += 1

            # Close the last temporary file if it's not closed
            if temp_output_file:
                temp_output_file.close()

        self.num_buffer_folders = max(
            self.num_buffer_folders, lines_processed // MAX_FILE_LINES)


columns_of_interest = [ColumnsOfInterest.TOWN, ColumnsOfInterest.MONTH,
                       ColumnsOfInterest.FLOOR_AREA_SQM, ColumnsOfInterest.RESALE_PRICE]

column_store = ColumnStore(INPUT_PATH, DISK_FOLDER, columns_of_interest)
column_store.process_csv()
zone_maps = column_store.get_zone_maps()

for column_name, zone_map_arr in zone_maps.items():
    for zone_map in zone_map_arr:
        print(f"ZoneMap for column '{column_name}': {zone_map.get_zone_map()}")

processor = QueryProcessor(2021, 11, 0, column_store)
processor.process_year_and_month()
processor.process_towns()

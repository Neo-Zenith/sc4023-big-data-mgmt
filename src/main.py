import os
import csv
from typing import Dict, List

MAX_FILE_LINES = 40000


class ColumnStore:
    def __init__(self, csv_file_path: str, output_folder: str, columns_of_interest: List[str], max_file_lines=MAX_FILE_LINES):
        self.csv_file_path = csv_file_path
        self.output_folder = output_folder
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
                            self.output_folder, f"{col_name}_chunk_{zone_count}.txt"), 'w')
                        self.zone_maps[col_name].append(
                            ZoneMap(col_name, zone_count))
                        self.zone_maps[col_name][-1].set_min_idx(idx)
                    zone_count += 1

                for column_name in self.columns_of_interest:
                    value = row[column_name]

                    file = opened_files[column_name]
                    file.write(f"{value}\n")

                    if column_name == 'month':
                        self.zone_maps[column_name][-1].update_month(value)

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

    def set_min_idx(self, min_idx):
        self.data['min_idx'] = min_idx

    def set_max_idx(self, max_idx):
        self.data['max_idx'] = max_idx

    def update_month(self, value):
        min_month = self.data['min_month']
        max_month = self.data['max_month']

        self.data['min_month'] = min(min_month, value)
        self.data['max_month'] = max(max_month, value)

    def get_zone_map(self):
        return self.data


csv_file_path = 'data/ResalePricesSingapore.csv'
output_folder = 'processed'
columns_of_interest = ['month', 'town', 'floor_area_sqm', 'resale_price']

column_store = ColumnStore(csv_file_path, output_folder, columns_of_interest)
column_store.process_csv()
zone_maps = column_store.get_zone_maps()

for column_name, zone_map_arr in zone_maps.items():
    for zone_map in zone_map_arr:
        print(f"ZoneMap for column '{column_name}': {zone_map.get_zone_map()}")

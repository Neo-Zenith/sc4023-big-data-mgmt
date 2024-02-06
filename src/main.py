import os
import csv
import statistics
import time
from constants import *
from typing import Dict, List
from datetime import datetime, timedelta
from enum import Enum


class ColumnsOfInterest(Enum):
    TOWN = 'town'
    MONTH = 'month'
    FLOOR_AREA_SQM = 'floor_area_sqm'
    RESALE_PRICE = 'resale_price'


class ZoneMap:
    def __init__(self, column_name, zone_count):
        """
        Initializes a ZoneMap object.

        Args:
            column_name (str): The name of the column.
            zone_count (int): The number of zones.
        """
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
        """
        Sets the minimum index.

        Args:
            min_idx (float): The minimum index value.
        """
        self.data['min_idx'] = min_idx

    def set_max_idx(self, max_idx):
        """
        Sets the maximum index.

        Args:
            max_idx (float): The maximum index value.
        """
        self.data['max_idx'] = max_idx

    def update_zone_map(self, value):
        """
        Updates the zone map data based on the given value.

        Args:
            value: The value to update the zone map data with.
        """
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
        """
        Returns the zone map data.

        Returns:
            dict: The zone map data.
        """
        return self.data

    def get_zone_count(self):
        """
        Returns the number of zones.

        Returns:
            int: The number of zones.
        """
        return self.zone_count


class ColumnStore:
    def __init__(self, csv_file_path: str, disk_folder: str, columns_of_interest: List[ColumnsOfInterest], max_file_lines=MAX_FILE_LINES):
        """
        Initializes a ColumnStore object.

        Args:
            csv_file_path (str): The path to the CSV file.
            disk_folder (str): The path to the folder where the chunk files will be stored.
            columns_of_interest (List[ColumnsOfInterest]): A list of columns of interest.
            max_file_lines (int, optional): The maximum number of lines per chunk file. Defaults to MAX_FILE_LINES.
        """
        self.csv_file_path = csv_file_path
        self.disk_folder = disk_folder
        self.columns_of_interest = columns_of_interest
        self.max_file_lines = max_file_lines
        self.zone_maps: Dict[str, List[ZoneMap]] = {}

        create_directory_if_not_exists(self.disk_folder)

        # initialize ZoneMap for interested columns
        for column_name in columns_of_interest:
            self.zone_maps[column_name] = []

    def process_csv(self):
        """
        Processes the CSV file and creates chunk files and zone maps.
        """
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
                        self.zone_maps[column_name][-1].update_zone_map(
                            value)

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

    def get_zone_maps(self) -> Dict[str, List[ZoneMap]]:
        """
        Returns the zone maps.

        Returns:
            Dict[str, List[ZoneMap]]: A dictionary containing the zone maps for each column of interest.
        """
        return self.zone_maps


class QueryProcessor:
    def __init__(self, year: int, month: int, town: int, column_store: ColumnStore, buffer_folder=BUFFER_FOLDER):
        """
        Initializes a QueryProcessor object.

        Args:
            year (int): The year value.
            month (int): The month value.
            town (int): The town value.
            column_store (ColumnStore): The column store object.
        """
        self.year = year
        self.month = month
        self.town = town
        self.column_store = column_store
        self.buffer_folder = buffer_folder
        self.num_buffer_folders = 0
        self.data = []
        create_directory_if_not_exists(self.buffer_folder)

    def process_year_and_month(self, column_name: ColumnsOfInterest = 'month'):
        """
        Processes the year and month data.

        Args:
            column_name (ColumnsOfInterest, optional): The column name of interest. Defaults to 'month'.
        """
        print("\n" + "=" * 60)
        print("Processing year and month...")
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

    def process_towns(self, column_name: ColumnsOfInterest = 'town'):
        """
        Processes the towns data.

        Args:
            column_name (ColumnsOfInterest, optional): The column name of interest. Defaults to 'town'.
        """
        print("\n" + "=" * 60)
        print("Processing towns...")

        # Read the indexes from the month's temp folder
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

        # Reset the buffer folder count
        self.num_buffer_folders = 0

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

    def process_query(self, column_name: ColumnsOfInterest, interested_stat: int):
        """
        Processes the query.

        Args:
            column_name (ColumnsOfInterest): The column name of interest.
        """
        print("\n" + "=" * 60)
        print(f"Processing {column_name}...")

        # Read the indexes from the town's temp folder
        indexes = []
        start, end = float('inf'), float('-inf')
        for i in range(self.num_buffer_folders + 1):
            town_temp_file_path = os.path.join(
                self.buffer_folder, f"town_chunk_{i}.txt")

            if not os.path.exists(town_temp_file_path):
                print(f"Error: {town_temp_file_path} not found.")
                continue

            with open(town_temp_file_path, 'r', encoding='utf-8') as month_temp_file:
                indexes += [int(line.split(" ")[1])
                            for line in month_temp_file]
                start = min(start, indexes[0])
                end = max(end, indexes[-1])

        # Reset the buffer folder count
        self.num_buffer_folders = 0

        # Find the zone containing the indexes
        for zone_map in self.column_store.get_zone_maps()[column_name]:
            min_idx, max_idx = \
                zone_map.get_zone_map()['min_idx'], zone_map.get_zone_map()[
                    'max_idx']
            if min_idx <= start <= max_idx or min_idx <= end <= max_idx:
                print(
                    f"Found the zone containing the indexes: {zone_map.get_zone_count()}")
                # Process the split files within the target zone
                self.process_split_files(
                    column_name, zone_map.get_zone_count(), None, None, indexes, True)

        output = self.calc_stat(interested_stat)

        # reset data buffer
        self.data = []
        return output

    def calc_stat(self, interested_stat) -> list:
        stat = None
        if interested_stat % 3 == 1:
            stat = min(self.data)
        elif interested_stat % 3 == 2:
            stat = round(statistics.mean(self.data), 2)
        elif interested_stat % 3 == 0:
            stat = round(statistics.stdev(self.data), 2)

        data = [self.year, f"{self.month:02}", REVERSE_TOWN_MAPPING[self.town],
                STATISTIC_TYPE[interested_stat], stat]
        return data

    def process_split_files(self, column_name: str, zone_count: int, start: str, end: str, indexes: list = [], final: bool = False):
        """
        Processes the split files.

        Args:
            column_name (str): The column name.
            zone_count (int): The zone count.
            start (str): The start value.
            end (str): The end value.
            indexes (list, optional): The list of indexes. Defaults to [].
            final (bool, optional): Indicates if it's the final processing. Defaults to False.
        """
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

                    if final:
                        self.data.append(int(value))
                        continue

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
                            f"{value} {index}\n")
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


def create_directory_if_not_exists(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)


def output_to_csv(file_path, data):
    mode = 'a' if os.path.exists(file_path) else 'w'
    with open(file_path, mode, newline='') as csvfile:
        writer = csv.writer(csvfile)
        if mode == 'w':
            writer.writerow(OUTPUT_HEADERS)
        writer.writerow(data)


def run(column_store):
    while True:
        print()
        text = 'Enter your matriculation number for processing [q to quit]: '
        matric_num = input(text).strip()

        if matric_num == 'q':
            print('System quitting...')
            break

        """ 
        467B 
        -> 1st digit: town
        -> 2nd digit: starting month
        -> 3rd digit: year
        """
        try:
            if len(matric_num) != 9:
                print('Invalid input - matriculation number is of length 9')
                continue
            town, month, year = int(
                matric_num[-4]), int(matric_num[-3]), int(matric_num[-2])
            year += 2010 if year > 3 else 2020
        except ValueError:
            print('Invalid input! Please try again...')
            continue

        print("Available statistics:")
        for key, value in STATISTIC_TYPE.items():
            print(f"\t{key} - {value}")
        text = 'Pick your statistic of interest: '
        interested_stat = input(text).strip()

        try:
            interested_stat = int(interested_stat)
            if interested_stat > 6:
                print('Invalid input! Please select only from the available choices...')
                continue
            interested_column = ColumnsOfInterest.FLOOR_AREA_SQM.value if interested_stat < 4 else ColumnsOfInterest.RESALE_PRICE.value
        except ValueError:
            print('Invalid input! Please try again...')
            continue

        start = time.time()
        processor = QueryProcessor(year, month, town, column_store)
        processor.process_year_and_month()
        processor.process_towns()
        data = processor.process_query(interested_column, interested_stat)
        print(f"\nQuery time: {time.time() - start}s")
        create_directory_if_not_exists(OUTPUT_FOLDER)
        output_file_path = os.path.join(
            OUTPUT_FOLDER, f"ScanResult_{matric_num}.csv")
        output_to_csv(output_file_path, data)
        print("Output written to", output_file_path)


def main():
    columns_of_interest = [ColumnsOfInterest.TOWN.value, ColumnsOfInterest.MONTH.value,
                           ColumnsOfInterest.FLOOR_AREA_SQM.value, ColumnsOfInterest.RESALE_PRICE.value]

    column_store = ColumnStore(
        INPUT_PATH, DISK_FOLDER, columns_of_interest)
    column_store.process_csv()

    # zone_maps = column_store.get_zone_maps()
    # for column_name, zone_map_arr in zone_maps.items():
    #     for zone_map in zone_map_arr:
    #         print(
    #             f"ZoneMap for column '{column_name}': {zone_map.get_zone_map()}")

    run(column_store)


if __name__ == "__main__":
    main()

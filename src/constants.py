MAX_FILE_LINES = 40000
INPUT_PATH = '../data/ResalePricesSingapore.csv'
DISK_FOLDER = 'processed'
BUFFER_FOLDER = 'temp'
OUTPUT_FOLDER = 'output'
OUTPUT_HEADERS = ['Year', 'Month', 'Town', 'Category', 'Value']

STATISTIC_TYPE = {
    1: 'Minimum Area', 2: 'Average Area', 3: 'Standard Deviation of Area', 4: 'Minimum Price', 5: 'Average Price', 6: 'Standard Deviation of Price', 7: 'Change Matriculation Number', 0: 'Quit Program'
}

TOWN_MAPPING = {
    'ANG MO KIO': 0,
    'BEDOK': 1,
    'BUKIT BATOK': 2,
    'CLEMENTI': 3,
    'CHOA CHU KANG': 4,
    'HOUGANG': 5,
    'JURONG WEST': 6,
    'PUNGGOL': 7,
    'WOODLANDS': 8,
    'YISHUN': 9
}
OTHER_TOWNS = {'SERANGOON', 'PASIR RIS', 'KALLANG/WHAMPOA', 'SEMBAWANG', 'SENGKANG', 'TOA PAYOH', 'BUKIT PANJANG',
               'BUKIT TIMAH', 'MARINE PARADE', 'GEYLANG', 'CENTRAL AREA', 'QUEENSTOWN', 'BISHAN', 'JURONG EAST', 'BUKIT MERAH', 'TAMPINES'}
OTHER_TOWNS_MAPPING = {town: 10 + i for i, town in enumerate(OTHER_TOWNS)}
ALL_TOWNS_MAPPING = {**TOWN_MAPPING, **OTHER_TOWNS_MAPPING}
REVERSE_TOWN_MAPPING = {v: k for k, v in ALL_TOWNS_MAPPING.items()}

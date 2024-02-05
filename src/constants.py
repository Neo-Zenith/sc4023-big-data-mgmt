STATISTIC_TYPE = {
    'MIN_AREA', 'AVG_AREA', 'STD_AREA', 'MIN_PRICE', 'AVG_PRICE', 'STD_PRICE'
}

MAX_FILE_LINES = 40000
INPUT_PATH = 'data/ResalePricesSingapore.csv'
DISK_FOLDER = 'processed'

""" 
467B 
-> 1st digit: town
-> 2nd digit: starting month
-> 3rd digit: year
"""

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

# TOWNS = ['ANG MO KIO', 'BEDOK', 'BISHAN', 'BUKIT BATOK', 'BUKIT MERAH',
#          'BUKIT PANJANG', 'BUKIT TIMAH', 'CENTRAL AREA', 'CHOA CHU KANG',
#          'CLEMENTI', 'GEYLANG', 'HOUGANG', 'JURONG EAST', 'JURONG WEST',
#          'KALLANG/WHAMPOA', 'MARINE PARADE', 'PASIR RIS', 'PUNGGOL',
#          'QUEENSTOWN', 'SEMBAWANG', 'SENGKANG', 'SERANGOON', 'TAMPINES',
#          'TOA PAYOH', 'WOODLANDS', 'YISHUN']

# TOWN_MAPPING = {town: i for i, town in enumerate(TOWNS)}

import pandas as pd
import argparse


def get_year(matriculation_number):
    last_digit = int(matriculation_number[-2])
    if last_digit >= 4:
        year = "201" + str(last_digit)
    else:
        year = "202" + str(last_digit)
    return year


def get_month(year, matriculation_number):
    second_last_digit = int(matriculation_number[-3])
    start_month = str(year) + "-0" + str(second_last_digit)
    end_month = str(year) + "-0" + str(second_last_digit + 2)
    return start_month, end_month


def get_town(matriculation_number):
    third_last_digit = int(matriculation_number[-4])
    towns = ['ANG MO KIO', 'BEDOK', 'BUKIT BATOK', 'CLEMENTI', 'CHOA CHU KANG',
             'HOUGANG', 'JURONG WEST', 'PUNGGOL', 'WOODLANDS', 'YISHUN']
    town = towns[third_last_digit]
    return town


def filter_data(rawfile, town, start_month, end_month):
    filtered = rawfile[(rawfile['town'] == town) & (pd.to_datetime(
        rawfile['month']) >= start_month) & (pd.to_datetime(rawfile['month']) <= end_month)]
    return filtered


def calculate_statistics(filtered):
    sum_area = filtered['floor_area_sqm'].sum()
    min_area = filtered['floor_area_sqm'].min()
    avg_area = filtered['floor_area_sqm'].mean()
    std_dev_area = filtered['floor_area_sqm'].std()

    sum_price = filtered['resale_price'].sum()
    min_price = filtered['resale_price'].min()
    avg_price = filtered['resale_price'].mean()
    std_dev_price = filtered['resale_price'].std()

    summary = pd.DataFrame({'Statistic': ['Sum Area', 'Minimum Area', 'Average Area', 'Standard Deviation of Area', 'Sum Price', 'Minimum Price', 'Average Price',
                                          'Standard Deviation of Price'],
                            'Value': [sum_area, min_area, avg_area, std_dev_area, sum_price, min_price, avg_price, std_dev_price]})
    return summary


def save_to_excel(filtered, summary, excel_file_path):
    with pd.ExcelWriter(excel_file_path, engine='xlsxwriter') as writer:
        filtered.to_excel(writer, sheet_name='Filtered Data', index=False)
        summary.to_excel(writer, sheet_name='Summary', index=False)


def main(matriculation_number):
    rawfile = pd.read_csv("data\ResalePricesSingapore.csv")

    year = get_year(matriculation_number)
    start_month, end_month = get_month(year, matriculation_number)
    town = get_town(matriculation_number)

    filtered = filter_data(rawfile, town, start_month, end_month)
    summary = calculate_statistics(filtered)

    excel_file_path = f"output\\test_output_{matriculation_number}.xlsx"
    save_to_excel(filtered, summary, excel_file_path)


def parse_arguments():
    parser = argparse.ArgumentParser(
        description='Generate test output based on matriculation number')
    parser.add_argument('--matric_num', '-m', type=str,
                        help='Matriculation number of length 9')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_arguments()
    main(args.matric_num)

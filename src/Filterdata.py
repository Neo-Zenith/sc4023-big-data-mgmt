import pandas

rawfile = pandas.read_csv(r"C:\Users\Zhi Quan\Downloads\ResalePricesSingapore.csv")

matriculation_number = 'U2120746D'
last_digit = int(matriculation_number[-2])
second_last_digit = int(matriculation_number[-3])
third_last_digit = matriculation_number[-4]

if last_digit >= 4:
    year = "201" + str(last_digit)
else:
    year = "202" + str(last_digit)
start_month = str(year) + "-0" + str(second_last_digit)
end_month = str(year) + "-0" + str(second_last_digit + 3)

start_month = pandas.to_datetime(start_month)
end_month = pandas.to_datetime(end_month)
towns = ['ANG MO KIO', 'BEDOK', 'BUKIT BATOK', 'CLEMENTI', 'CHOA CHU KANG',
         'HOUGANG', 'JURONG WEST', 'PUNGGOL', 'WOODLANDS', 'YISHUN']
town = towns[third_last_digit]

# Filter the DataFrame based on the specified criteria
filtered = rawfile[(rawfile['town'] == town) & (pandas.to_datetime(rawfile['month']) >= start_month) & (pandas.to_datetime(rawfile['month']) <= end_month)]

# Calculate minimum, average, and standard deviation of floor area and price
min_area = filtered['floor_area_sqm'].min()
avg_area = filtered['floor_area_sqm'].mean()
std_dev_area = filtered['floor_area_sqm'].std()

min_price = filtered['resale_price'].min()
avg_price = filtered['resale_price'].mean()
std_dev_price = filtered['resale_price'].std()

summary = pandas.DataFrame({'Statistic': ['Minimum Area', 'Average Area', 'Standard Deviation of Area','Minimum Price', 'Average Price', 'Standard Deviation of Price'],'Value': [min_area, avg_area, std_dev_area, min_price, avg_price, std_dev_price]})


excel_file_path = r"C:\Users\Zhi Quan\Downloads\summary_data.xlsx"
with pandas.ExcelWriter(excel_file_path, engine='xlsxwriter') as writer:
    filtered.to_excel(writer, sheet_name='Filtered Data', index=False)
    summary.to_excel(writer, sheet_name='Summary', index=False)

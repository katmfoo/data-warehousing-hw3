import sqlite3
import datetime

sqlite_file = 'datamart3_v4.db'

conn = sqlite3.connect(sqlite_file)
c = conn.cursor()

c.execute("SELECT * FROM source_transactions WHERE substr(date,5,2) = '12'")
items = c.fetchall()

final_data = {}

i = 0
for row_item in items:

    c.execute("SELECT date_key FROM date_dimension WHERE date = " + row_item[0])
    date_key = c.fetchone()[0]

    c.execute("SELECT product_key FROM product_dimension WHERE sku = " + str(row_item[2]))
    product_key = c.fetchone()[0]

    store_key = int(row_item[6])

    daily_customer_num = row_item[1]

    composite_key = str(store_key).zfill(3) + str(date_key).zfill(3) + str(daily_customer_num).zfill(4) + str(product_key).zfill(4)

    existingItem = final_data.get(composite_key)

    i = i + 1
    print(i)

    if not existingItem:
        new_row = {}
        new_row['date_key'] = date_key
        new_row['daily_customer_num'] = daily_customer_num
        new_row['product_key'] = product_key
        new_row['quantity_sold'] = 1
        new_row['total_dollar_sales'] = float(row_item[3])
        c.execute("SELECT base_price FROM original_products WHERE sku = " + str(row_item[2]))
        new_row['total_cost_to_store'] = float(c.fetchone()[0][1:])
        gross_profit = new_row['total_dollar_sales'] - new_row['total_cost_to_store']
        new_row['gross_profit'] = float(round(float(gross_profit), 2))
        new_row['store_key'] = store_key
        new_row['composite_key'] = composite_key
        final_data[composite_key] = new_row
    else:
        existingItem['quantity_sold'] = existingItem['quantity_sold'] + 1
        existingItem['total_dollar_sales'] = round(existingItem['total_dollar_sales'] + float(row_item[3]), 2)
        c.execute("SELECT base_price FROM original_products WHERE sku = " + str(row_item[2]))
        existingItem['total_cost_to_store'] = round(existingItem['total_cost_to_store'] + float(c.fetchone()[0][1:]), 2)
        existingItem['gross_profit'] = round(existingItem['total_dollar_sales'] - existingItem['total_cost_to_store'], 2)

text_file = open("deliverable5.csv", "w")

for row in final_data:
    text_file.write("%s,%s,%s,%s,%s,%s,%s,%s,%s\n" % (final_data[row]['composite_key'], final_data[row]['date_key'], final_data[row]['daily_customer_num'], final_data[row]['product_key'], final_data[row]['store_key'], final_data[row]['quantity_sold'], final_data[row]['total_dollar_sales'], final_data[row]['total_cost_to_store'], final_data[row]['gross_profit']))

text_file.close()
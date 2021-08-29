import utils.redshift as rs

print("Connection starting....")
conn = rs.create_conn()
print("Connection complete")

cursor = conn.cursor()
print("Cursor created... Initiating query")
query = rs.create_query()
rs.select(cursor, query)
print("Query complete.")

print("Initiating shutdown....")
cursor.close()
conn.close()


print("\n<<<<--->>>>")
print("\n<<<<DONEZO>>>>")
print("\n<<<<------------>>>>")
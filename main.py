import json
import psycopg2
import upload

#reading json data
with open('students.json') as file:
    students = json.load(file)

with open('rooms.json') as file:
    rooms = json.load(file)

#connector creation
conn = psycopg2.connect(
    host="localhost",
    port="5432",
    database="task1",
    user="postgres",
    password="1864532")

cursor = conn.cursor()

print(upload.table_creation(cursor))

print(upload.data_upload(rooms, students, conn))

print(upload.indexing(cursor))

f = upload.list_of_rooms(cursor)
print(f.read())
f.close()

f1 = upload.min_av_age(cursor)
print(f1.read())
f1.close()

f2 = upload.max_age_diff(cursor)
print(f2.read())
f2.close()

f3 = upload.intersexual(cursor)
print(f3.read())
f3.close()

f4 = upload.males_only(cursor)
print(f4.read())
f4.close()

f5 = upload.females_only(cursor)
print(f5.read())
f5.close()


cursor.close()
conn.close()

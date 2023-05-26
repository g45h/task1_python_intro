import json
import os
def table_creation(cursor):
    temp = ('''SELECT EXISTS
                (SELECT * FROM information_schema.tables 
                WHERE table_name = 'rooms');''')
    cursor.execute(temp)
    exists = cursor.fetchone()
    if not exists:
        cursor.execute('''CREATE TABLE rooms 
            (id int PRIMARY KEY, 
            name VARCHAR(50))''')

    temp = ('''SELECT EXISTS
                    (SELECT * FROM information_schema.tables 
                    WHERE table_name = 'students');''')
    cursor.execute(temp)
    exists = cursor.fetchone()
    if not exists:
        cursor.execute('''CREATE TABLE students 
            (id int PRIMARY KEY,
            name VARCHAR(255),
            birthday TIMESTAMP WITH TIME ZONE,
            room_id INTEGER REFERENCES rooms(id),
            sex CHAR(1))''')
    return "Successfully created 2 tables"

#Написать скрипт, целью которого будет загрузка этих двух файлов и запись данных в базу
def data_upload(rooms, students, connector):
    cursor = connector.cursor()
    temp = '''SELECT CASE WHEN EXISTS (SELECT * FROM rooms LIMIT 1) THEN 1 ELSE 0 END '''
    cursor.execute(temp)
    hasContext = cursor.fetchone()
    if not hasContext:
        insert_query = '''
                INSERT INTO rooms (id, name) 
                VALUES (%s, %s)
            '''
        for record in rooms:
            values = (
                record['id'],
                record['name']
            )
            cursor.execute(insert_query, values)
        connector.commit()

    temp = "SELECT CASE WHEN EXISTS (SELECT * FROM students LIMIT 1) THEN 1 ELSE 0 END"
    cursor.execute(temp)
    hasContext = cursor.fetchone()
    if not hasContext:
        insert_query = '''
               INSERT INTO students (id, name, birthday, room_id, sex)
               VALUES (%s, %s, %s, %s, %s)
           '''
        for record in students:
            values = (
                record['id'],
                record['name'],
                record['birthday'],
                record['room'],
                record['sex']
            )
            cursor.execute(insert_query, values)
        connector.commit()
    return "Successful data upload"

#В результате надо сгенерировать SQL запрос который добавить нужные индексы
def indexing(cursor):
    temp = ('''CREATE INDEX room_index ON rooms (id);''')
    cursor.execute(temp)
    temp = ('''CREATE INDEX student_index ON students (id);''')
    cursor.execute(temp)
    return "Successfully indexed 2 tables"


#Список комнат и количество студентов в каждой из них
def list_of_rooms(cursor):
    select_query = "SELECT rooms.id, COUNT(sir.id) AS amount_of_students " \
                   "FROM rooms LEFT JOIN students sir ON sir.room_id = rooms.id " \
                   "GROUP BY rooms.id;"
    cursor.execute(select_query)
    rows = cursor.fetchall()
    sorted_rows = sorted(rows, key=lambda x: x[0])
    jsonString = json.dumps(sorted_rows, sort_keys=True)
    try:
        if os.stat("list_of_rooms.json").st_size == 0:
            jsonFile = open("list_of_rooms.json", "w")
            jsonFile.write(jsonString)
            jsonFile.close()
    except FileNotFoundError:
        jsonFile = open("list_of_rooms.json", "w")
        jsonFile.write(jsonString)
        jsonFile.close()

    try:
        return open("list_of_rooms.json", mode='r')
    except:
        print('Error. Try again')

#5 комнат, где самый маленький средний возраст студентов
def min_av_age(cursor):
    select_query = "SELECT rooms.id, AVG(EXTRACT(YEAR FROM age(sir.birthday))) AS avage " \
               "FROM rooms LEFT JOIN students sir ON sir.room_id = rooms.id " \
               "GROUP BY rooms.id " \
               "ORDER BY avage LIMIT 5"
    cursor.execute(select_query)
    rows = cursor.fetchall()
    jsonString = json.dumps(rows, default=str)
    try:
        if os.stat("average_age.json").st_size == 0:
            jsonFile = open("average_age.json", "w")
            jsonFile.write(jsonString)
            jsonFile.close()
    except FileNotFoundError:
        jsonFile = open("average_age.json", "w")
        jsonFile.write(jsonString)
        jsonFile.close()

    try:
        return open("average_age.json", mode='r')
    except:
        print('Error. Try again')

#5 комнат с самой большой разницей в возрасте студентов
def max_age_diff(cursor):
    select_query = "SELECT rooms.id, MAX(age(sir.birthday)) - MIN(age(sir.birthday)) as agediff " \
               "FROM rooms LEFT JOIN students sir ON sir.room_id = rooms.id " \
               "WHERE EXISTS (SELECT 1 FROM students WHERE room_id = rooms.id) " \
               "GROUP BY rooms.id " \
               "ORDER BY agediff DESC LIMIT 5"
    cursor.execute(select_query)
    rows = cursor.fetchall()
    jsonString = json.dumps(rows, default=str)
    try:
        if os.stat("age_diff.json").st_size == 0:
            jsonFile = open("age_diff.json", "w")
            jsonFile.write(jsonString)
            jsonFile.close()
    except FileNotFoundError:
        jsonFile = open("age_diff.json", "w")
        jsonFile.write(jsonString)
        jsonFile.close()

    try:
        return open("age_diff.json", mode='r')
    except:
        print('Error. Try again')

#Список комнат где живут разнополые студенты
def intersexual(cursor):
    select_query = "SELECT rooms.id FROM rooms LEFT JOIN students sir ON sir.room_id = rooms.id " \
               "WHERE rooms.id IN(SELECT room_id FROM students WHERE sex = 'M' ) " \
               "AND rooms.id IN(SELECT room_id FROM students WHERE sex = 'F')" \
               "GROUP BY rooms.id " \
               "ORDER BY rooms.id"
    cursor.execute(select_query)
    rows = cursor.fetchall()
    jsonString = json.dumps(rows, default=str)
    try:
        if os.stat("intersexual_rooms.json").st_size == 0:
            jsonFile = open("intersexual_rooms.json", "w")
            jsonFile.write(jsonString)
            jsonFile.close()
    except FileNotFoundError:
        jsonFile = open("intersexual_rooms.json", "w")
        jsonFile.write(jsonString)
        jsonFile.close()

    try:
        return open("intersexual_rooms.json", mode='r')
    except:
        print('Error. Try again')


def males_only(cursor):
    select_query = "SELECT rooms.id " \
               "FROM rooms " \
               "LEFT JOIN students sir ON sir.room_id = rooms.id " \
               "GROUP BY rooms.id " \
               "HAVING COUNT(CASE WHEN sir.sex = 'M' THEN 1 END) > 0 " \
               "AND COUNT(CASE WHEN sir.sex = 'F' THEN 1 END) = 0;"
    cursor.execute(select_query)
    rows = cursor.fetchall()
    jsonString = json.dumps(rows, default=str)
    try:
        if os.stat("males_only.json").st_size == 0:
            jsonFile = open("males_only.json", "w")
            jsonFile.write(jsonString)
            jsonFile.close()
    except FileNotFoundError:
        jsonFile = open("males_only.json", "w")
        jsonFile.write(jsonString)
        jsonFile.close()

    try:
        return open("males_only.json", mode='r')
    except:
        print('Error. Try again')


def females_only(cursor):
    select_query = "SELECT rooms.id " \
               "FROM rooms " \
               "LEFT JOIN students sir ON sir.room_id = rooms.id " \
               "GROUP BY rooms.id " \
               "HAVING COUNT(CASE WHEN sir.sex = 'F' THEN 1 END) > 0 " \
               "AND COUNT(CASE WHEN sir.sex = 'M' THEN 1 END) = 0;"
    cursor.execute(select_query)
    rows = cursor.fetchall()
    jsonString = json.dumps(rows, default=str)
    try:
        if os.stat("females_only.json").st_size == 0:
            jsonFile = open("females_only.json", "w")
            jsonFile.write(jsonString)
            jsonFile.close()
    except FileNotFoundError:
        jsonFile = open("females_only.json", "w")
        jsonFile.write(jsonString)
        jsonFile.close()

    try:
        return open("females_only.json", mode='r')
    except:
        print('Error. Try again')
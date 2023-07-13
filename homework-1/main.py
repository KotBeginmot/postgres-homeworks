"""Скрипт для заполнения данными таблиц в БД Postgres."""
import psycopg2
from csv import DictReader

# Выполняем подключение к postgres
conn = psycopg2.connect(host="localhost", database="north", user="postgres", password="1234")

try:
    with conn.cursor() as cur_emp:  # Устанавливаем курсор
        with open('north_data/employees_data.csv') as file:  # Открываем csv файл
            content = DictReader(file)
            for cont in content:
                cur_emp.execute('INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)',  # Задаём строки для столбцов
                                (int(cont["employee_id"]), cont['first_name'], cont['last_name'], cont['title'],
                                 cont['birth_date'], cont['notes']))
        with open('north_data/customers_data.csv') as file:  # Открываем csv файл
            content = DictReader(file)
            for cont in content:
                cur_emp.execute('INSERT INTO customers VALUES (%s, %s, %s)',  # Задаём строки для столбцов
                                (cont["customer_id"], cont['company_name'], cont['contact_name']))
        with open('north_data/orders_data.csv') as file:  # Открываем csv файл
            content = DictReader(file)
            for cont in content:
                cur_emp.execute('INSERT INTO orders VALUES (%s, %s, %s, %s, %s)',  # Задаём строки для столбцов
                                (cont["order_id"], cont['customer_id'], cont['employee_id'], cont['order_date'],
                                 cont['ship_city']))

except psycopg2.errors.UniqueViolation:
    pass


# Завершаем подключение
finally:
    conn.commit()
    conn.close()

import json
import random
import string
from time import sleep

import pika
import psycopg2


def connect_to_database(database):
    """
    connects to postgres server on the given database
    sets autocommit to true to avoid dealing with transaction code
    :param database:
    :return:
    """
    conn = psycopg2.connect(database=database, user='bb', password='bb')
    print(f' [x] connected to {database} database')
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    return conn, cursor


database_name = 'replica'
conn, cursor = connect_to_database('postgres')
cursor.execute(f'CREATE DATABASE {database_name};')
print(f' [x] database {database_name} created')
cursor.close()
conn.close()
print(' [x] connection to database postgres closed')
conn, cursor = connect_to_database(database_name)
cursor.execute(open('create tables.sql', 'r').read())

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel.exchange_declare(exchange='replication', exchange_type='fanout')
result = channel.queue_declare(queue='', exclusive=True)
queue_name = result.method.queue
channel.queue_bind(exchange='replication', queue=queue_name)


def operation_handler(op):
    def handle_insert():
        print('INSERT')
        table, data = op['table'], op['new_record']
        sql = f"""INSERT INTO {table} 
        VALUES ('{data['id']}','{data['name']}');
        """
        return sql
    sql = None
    if op['operation'] == 'INSERT':
        sql = handle_insert()
    return sql


def callback(channel, method, properties, body):
    op = json.loads(body.decode('utf-8'))
    sql = operation_handler(op)
    cursor.execute(sql)


channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
channel.start_consuming()

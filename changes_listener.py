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


def setup():
    """
    connects to "postgres" database in the postgres server
    creates a new database with a random name
    closes postgres database connection
    connects to the new database
    creates the schema

    connects to rabbitmq server
    connect/create exchange
    create a queue and bind it to exchange
    :return:
    database connection,
    database cursor,
    generated database name,
    rabbitmq channel,
    rabbitmq queue
    """
    # setup postgres connection
    # generate database_name
    database_name = input('enter the database name to replicate into: ')
    database_name += '_' + ''.join(random.choices(string.ascii_letters, k=5))
    database_name = database_name.lower()


    conn, cursor = connect_to_database('postgres')

    cursor.execute(f'CREATE DATABASE {database_name};')
    print(f' [x] database {database_name} created')
    cursor.close()
    conn.close()
    print(' [x] connection to database postgres closed')

    conn, cursor = connect_to_database(database_name)

    cursor.execute(open('create tables.sql', 'r').read())
    print(' [x] table teacher and users created')

    # setup rabbitmq connection and queues
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    print(' [x] connected to rabbitmq')
    channel = connection.channel()
    channel.exchange_declare(exchange='replication', exchange_type='fanout')
    print(' [x] exchange created')
    result = channel.queue_declare(queue='', exclusive=True)
    print(' [x] queue created')
    queue_name = result.method.queue
    channel.queue_bind(exchange='replication', queue=queue_name)
    print(' [x] queue bound to exchange')
    return conn, cursor, database_name, channel, queue_name


def consume(cursor, channel, queue_name, cbk):
    """
    start consuming messages received from queue
    """
    channel.basic_consume(queue=queue_name, on_message_callback=cbk, auto_ack=True)
    print(' [*] Waiting for replication operations. To exit press CTRL+C')
    channel.start_consuming()


def operation_handler(op):
    """
    takes in received operation from postgres trigger
    and generates the appropriate sql statement string
    """
    def handle_insert(op):
        print('INSERT')
        table, data = op['table'], op['new_record']
        sql = f"""INSERT INTO {table} 
        VALUES ('{data['id']}','{data['name']}');
        """
        return sql

    def handle_update(op):
        table, new, old = op['table'], op['new_record'], op['old_record']
        sql = f"""UPDATE {table}
        SET  id={new['id']},
        name='{new['name']}'
        WHERE id={old['id']};
        """
        return sql

    print(f" [x] received {op}")
    sql = None
    if op['operation'] == 'INSERT':
        sql = handle_insert(op)
    if op['operation'] == 'UPDATE':
        sql = handle_update(op)
    return sql


def callback_generator(cur):
    """
    wraps the callback function that is used to consume
    rabbitmq messages and give it access to the database cursor
    """
    def callback(channel, method, properties, body):
        op = json.loads(body.decode('utf-8'))
        sql = operation_handler(op)
        cur.execute(sql)
        print(f' [x] executing "{sql}" done')
        print(' [*] Waiting for replication operations. To exit press CTRL+C')

    return callback


if __name__ == '__main__':
    co, cu, database, ch, q = setup()
    try:
        consume(cu, ch, q, callback_generator(cu))
    except KeyboardInterrupt:
        """
        closes all connections and drop the created database
        """
        ch.close()
        print(' [x] rabbitmq channel closed')
        cu.close()
        co.close()
        print(f' [x] database {database} connection closed')
        conn, cur = connect_to_database('postgres')
        cur.execute(f'DROP DATABASE {database}')
        print(f' [x] database {database} dropped')
        cur.close()
        conn.close()
        print(' [x] database postgres connection closed')

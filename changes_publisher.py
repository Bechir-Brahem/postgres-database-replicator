import asyncio
import pika
import sys
import psycopg2
import uvloop

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
print(' [x] RABBITMQ: connection established')
channel = connection.channel()

channel.exchange_declare(exchange='replication', exchange_type='fanout')
print(' [x] RABBITMQ: exchange declared')
# dbname should be the same for the notifying process
conn = psycopg2.connect(host="localhost", dbname="database_replication", user="bb", password="bb")
print(' [x] POSTGRES: connection established')
conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
cursor = conn.cursor()
cursor.execute(f"LISTEN users_changed;")
print(' [*] POSTGRES: listening to channel "users_changed"')


def publish_queue(payload):
    channel.basic_publish(exchange='replication', routing_key='', body=payload)
    print(f" [x] Sent {payload}")


def handle_notify():
    print(' [x] POSTGRES: notification received')
    conn.poll()
    for notify in conn.notifies:
        publish_queue(notify.payload)

    conn.notifies.clear()


loop = uvloop.new_event_loop()
asyncio.set_event_loop(loop)
loop.add_reader(conn, handle_notify)
try:
    loop.run_forever()
except KeyboardInterrupt:
    cursor.close()
    print(' [x] POSTGRES: cursor closed')
    conn.close()
    print(' [x] POSTGRES: connection closed')
    channel.close()
    print(' [x] RABBITMQ: channel closed')
    connection.close()
    print(' [x] RABBITMQ: connection closed')
    sys.exit(1)

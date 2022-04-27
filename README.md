# Postgres Database Replicator

## TLDR

this program is used to replicate a postgres server using triggers for inserts and deletes and rabbitmq to broadcast the
changes to all database replicators.

## How It Works

postgres supports `LISTEN` and `NOTIFY` commands. meaning that one client can listen to events on a certain 'channel'
and the server can notify the listeners using the `NOTIFY` command.

we define SQL triggers on inserts and updates(and possibly more) so that on each change we notify the listeners about
the changes done.

changes_publisher.py is a listener to these database notifications. and when it receives the incoming data it broadcasts
them to replicators through rabbitmq messaging service. this way only one connection is made with the postgres server
and it uses async function.

messages about changes are persisted in the rabbitmq server.

changes are then received by `changes_listener.py` which then commit the changes to the replica database.also using
async functions.

## setup

- first run the postgres server
- add the triggers for the tables (in this case the table is called users)

```sql
CREATE OR REPLACE FUNCTION notify_account_changes()
    RETURNS trigger AS
$$
BEGIN
    PERFORM pg_notify(
            'users_changed',
            json_build_object(
                    'table', TG_TABLE_NAME,
                    'operation', TG_OP,
                    'new_record', row_to_json(NEW),
                    'old_record', row_to_json(OLD)
                )::text
        );

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER users_changed
    AFTER INSERT OR UPDATE
    ON users
    FOR EACH ROW
EXECUTE PROCEDURE notify_account_changes();
```

- start the rabbitmq server and configure the credentials in the python files
- run `pipenv install` and `pipenv shell` (install pipenv if not found)
- start the changes publisher `python3 changes_publisher.py`
- start the change listener programs for as many replicas as you want
- insert or update rows and observer the listener logs and the databases

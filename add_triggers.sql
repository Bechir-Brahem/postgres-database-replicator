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

CREATE TRIGGER teachers
    AFTER INSERT OR UPDATE
    ON teachers
    FOR EACH ROW
EXECUTE PROCEDURE notify_account_changes();

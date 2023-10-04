import logging
import json
import threading
import os
import time
import pytz
import psycopg2

from psycopg2 import OperationalError, Error, errors
from psycopg2.extras import DictCursor

from enum import Enum
from time import monotonic
from datetime import datetime

TIMEZONE = os.getenv("TIMEZONE", "Asia/Bangkok")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
POSTGRES_SERVICE_NAME = os.getenv("POSTGRES_SERVICE_NAME","localhost")
MIN_SEQUENCE_COMMANDS_WAIT_TIME = os.getenv("MIN_SEQUENCE_COMMANDS_WAIT_TIME", 10)

LOGGING_LEVEL_DICT = {
    "CRITICAL": logging.CRITICAL,
    "ERROR": logging.ERROR,
    "WARNING": logging.WARNING,
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
}

COMMANDS_RECORD = "commands_record"
CONFIGS_RECORD = "configs_record"
CREDITS_ALLOCATION_RECORD = "credits_allocation_record"
BATCHES_RECORD = "batches_record"
LONGAN_LOT_INFO_RECORD = "longan_lot_info_record"
CURRENT_MACHINE_CONTROL_STATUSES = "current_machine_control_statuses"
CURRENT_COMMAND = "current_command"
CURRENT_PANEL_SELECTION = "current_panel_selection"
CURRENT_CREDITS_BALANCE = "current_credits_balance"
CURRENT_NODES_STATUS = "current_nodes_status"
CURRENT_SORTER_DISPLAY = "current_sorter_display"

GENERATE_CURRENT_COMMAND = "generate_current_command"
GENERATE_COMMANDS_RECORD = "generate_commands_record"

TRIGGER_ON_COMMANDS_RECORD = "trigger_on_commands_record"
TRIGGER_ON_CURRENT_MACHINE_CONTROL_STATUSES = "trigger_on_current_machine_control_statuses"

ENUMS = {
    "source_enum":"""
        CREATE TYPE source_enum AS ENUM ('cloud', 'remote', 'local');
    """,
    "command_status_enum": """
        CREATE TYPE command_status_enum AS ENUM ('none','inprogress','satisfied','error');
    """,
    "panel_selection_enum": """
        CREATE TYPE panel_selection_enum AS ENUM ('color', 'off', 'b', 'a', 'aa', 'service', 'link');
    """,
    "machine_display_status_enum": """
        CREATE TYPE machine_display_status_enum AS ENUM ('normal', 'inprogress', 'warn', 'error', 'service');
    """,
    "allocation_action_enum": """
        CREATE TYPE allocation_action_enum AS ENUM ('add', 'subtract');
    """
}

DATABASE_TABLES = {
    CONFIGS_RECORD: f"""
        CREATE TABLE {CONFIGS_RECORD} (
            config_id SERIAL PRIMARY KEY,
            config_name VARCHAR(50) NOT NULL,
            config JSON,
            is_active BOOLEAN NOT NULL,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """,
    COMMANDS_RECORD: f"""
        CREATE TABLE {COMMANDS_RECORD} (
            command_id SERIAL PRIMARY KEY,
            command VARCHAR(50) NOT NULL,
            is_processed BOOLEAN NOT NULL DEFAULT false,
            machine_config JSON,
            source source_enum NOT NULL,
            commander_id VARCHAR(255) NOT NULL,
            panel_selection panel_selection_enum,
            is_activated BOOLEAN NOT NULL DEFAULT false,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """,
    CREDITS_ALLOCATION_RECORD: f"""
        CREATE TABLE {CREDITS_ALLOCATION_RECORD} (
            allocation_id SERIAL PRIMARY KEY,
            allocation_action allocation_action_enum NOT NULL,
            credits_amount DECIMAL(20,5) NOT NULL,
            is_processed BOOLEAN NOT NULL DEFAULT false,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """,
    BATCHES_RECORD: f"""
        CREATE TABLE {BATCHES_RECORD} (
            batch_id SERIAL PRIMARY KEY,
            panel_selection panel_selection_enum NOT NULL,
            eject_config JSON,
            total_longan DECIMAL(13),
            selected_longan DECIMAL(13),
            ejected_longan DECIMAL(13),
            batch_start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            batch_end_time TIMESTAMP,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """,
    LONGAN_LOT_INFO_RECORD: f"""
        CREATE TABLE {LONGAN_LOT_INFO_RECORD} (
            longan_lot_id SERIAL PRIMARY KEY,
            batch_id INTEGER REFERENCES {BATCHES_RECORD}(batch_id),
            offset_start DECIMAL(13),
            offset_end DECIMAL(13),
            total_longan DECIMAL(13),
            selected_longan DECIMAL(13),
            ejected_longan DECIMAL(13),
            is_processed BOOLEAN NOT NULL DEFAULT false, 
            eject_info_dict JSON,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """,
    CURRENT_MACHINE_CONTROL_STATUSES: f"""
        CREATE TABLE {CURRENT_MACHINE_CONTROL_STATUSES} (
            id SERIAL PRIMARY KEY,
            is_unregister BOOLEAN NOT NULL DEFAULT true,
            is_disabled BOOLEAN NOT NULL DEFAULT true,
            is_remote BOOLEAN NOT NULL DEFAULT false,
            is_service BOOLEAN NOT NULL DEFAULT false,
            is_warn BOOLEAN NOT NULL DEFAULT false,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """,
    CURRENT_COMMAND: f"""
        CREATE TABLE {CURRENT_COMMAND} (
            id SERIAL PRIMARY KEY,
            command_id INTEGER REFERENCES {COMMANDS_RECORD}(command_id) NULL,
            command_status command_status_enum NOT NULL DEFAULT 'none',
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """,
    CURRENT_PANEL_SELECTION: f"""
        CREATE TABLE {CURRENT_PANEL_SELECTION} (
            id SERIAL PRIMARY KEY,
            panel_selection panel_selection_enum NOT NULL,
            is_processed BOOLEAN NOT NULL DEFAULT false,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """,
    CURRENT_CREDITS_BALANCE: f"""
        CREATE TABLE {CURRENT_CREDITS_BALANCE} (
            id SERIAL PRIMARY KEY,
            credits_balance DECIMAL(20,5) NOT NULL,
            timestamp TIMESTAMP
        );
    """,
    CURRENT_NODES_STATUS: f"""
        CREATE TABLE {CURRENT_NODES_STATUS} (
            id SERIAL PRIMARY KEY,
            node_type VARCHAR(50),
            node_name VARCHAR(55),
            node_status DECIMAL(1) NOT NULL,
            warning_msg VARCHAR(255),
            error_msg VARCHAR(255),
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """,
    # "CURRENT_SORTER_DISPLAY" : f"""
    #     CREATE TABLE {CURRENT_SORTER_DISPLAY} ();
    # """
}

PROCEDURES_CREATE_COMMANDS = {
    GENERATE_CURRENT_COMMAND : f"""
        CREATE OR REPLACE FUNCTION {GENERATE_CURRENT_COMMAND} ()
        RETURNS TRIGGER AS $$
        DECLARE
            selected_command RECORD;
            is_service BOOLEAN;
            is_remote BOOLEAN;
            is_unregister BOOLEAN;
            is_disabled BOOLEAN;
            statuses_record RECORD;
        BEGIN
            -- Fetch the row from the current_machine_control_statuses table
            SELECT * INTO statuses_record FROM current_machine_control_statuses Limit 1;

            -- If there is no status data, exit
            IF statuses_record IS NULL THEN
                RETURN NEW;
            END IF;

            is_service := statuses_record.is_service;
            is_remote := statuses_record.is_remote;
            is_unregister := statuses_record.is_unregister;
            is_disabled := statuses_record.is_disabled;

            -- If unregister is true
            IF is_unregister THEN
                IF is_service OR is_remote THEN
                    SELECT * INTO selected_command FROM {COMMANDS_RECORD}
                    WHERE is_processed = false AND command LIKE '%_START'
                    ORDER BY
                        CASE 
                            WHEN source = 'cloud' THEN 1
                            WHEN source = 'remote' THEN 2
                            ELSE 3
                        END,
                        timestamp DESC
                    LIMIT 1;
                    IF NOT FOUND THEN
                        SELECT * INTO selected_command FROM {COMMANDS_RECORD}
                        WHERE is_processed = false AND command LIKE '%_STOP'
                        ORDER BY
                            CASE
                                WHEN source = 'remote' THEN 1
                                WHEN source = 'cloud' THEN 2
                                ELSE 3
                            END,
                            timestamp DESC
                        LIMIT 1;
                    END IF;
                END IF;
            ELSE
                -- If is_service or is_remote is true
                IF is_service OR is_remote OR is_disabled THEN
                    SELECT * INTO selected_command FROM {COMMANDS_RECORD}
                    WHERE is_processed = false AND command LIKE '%_START'
                    ORDER BY
                        CASE 
                            WHEN source = 'cloud' THEN 1
                            WHEN source = 'remote' THEN 2
                            ELSE 3
                        END,
                        timestamp DESC
                    LIMIT 1;
                    IF NOT FOUND THEN
                        SELECT * INTO selected_command FROM {COMMANDS_RECORD}
                        WHERE is_processed = false AND command LIKE '%_STOP'
                        ORDER BY
                            CASE
                                WHEN source = 'local' THEN 1
                                WHEN source = 'remote' THEN 2
                                WHEN source = 'cloud' THEN 3
                                ELSE 4
                            END,
                            timestamp DESC
                        LIMIT 1;
                    END IF;
                -- If is_disabled is false
                ELSE
                    SELECT * INTO selected_command FROM {COMMANDS_RECORD}
                    WHERE is_processed = false AND command LIKE '%_START'
                    ORDER BY
                        CASE
                            WHEN source = 'cloud' THEN 1
                            WHEN source = 'remote' THEN 2
                            WHEN source = 'local' THEN 3
                            ELSE 4
                        END,
                        timestamp DESC
                    LIMIT 1;
                    IF NOT FOUND THEN
                        SELECT * INTO selected_command FROM {COMMANDS_RECORD}
                        WHERE is_processed = false AND command LIKE '%_STOP'
                        ORDER BY
                            CASE
                                WHEN source = 'local' THEN 1
                                WHEN source = 'remote' THEN 2
                                WHEN source = 'cloud' THEN 3
                                ELSE 4
                            END,
                            timestamp DESC
                        LIMIT 1;
                    END IF;
                END IF;
            END IF;

            IF FOUND THEN
                INSERT INTO {CURRENT_COMMAND} (id, command_id, command_status)
                VALUES (1, selected_command.command_id, 'none')
                ON CONFLICT (id)
                DO UPDATE SET command_id = excluded.command_id, command_status = excluded.command_status, timestamp = CURRENT_TIMESTAMP;

                UPDATE {COMMANDS_RECORD}
                SET is_processed = true,
                    is_activated = CASE WHEN command_id = selected_command.command_id THEN true ELSE is_activated END;

            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    """,
    # GENERATE_COMMANDS_RECORD : f"""
    #     CREATE OR REPLACE FUNCTION {GENERATE_COMMANDS_RECORD} ()
    #     RETURNS TRIGGER AS $$
    #     DECLARE
    #         is_disabled BOOLEAN;
    #     BEGIN
    #         -- Fetch the row from the current_machine_control_statuses table
    #         SELECT * FROM current_machine_control_statuses
    #         WHERE is_disabled = true
    #         Limit 1;

    #         -- If there is no status data, exit
    #         IF statuses_record IS NULL THEN
    #             RETURN NEW;
    #         END IF;

    #         is_service := statuses_record.is_service;
    #         is_remote := statuses_record.is_remote;
    #         is_unregister := statuses_record.is_unregister;
    #         is_disabled := statuses_record.is_disabled;

    #         -- If unregister is true
    #         IF is_unregister THEN
    #             IF is_service OR is_remote THEN
    #                 SELECT * INTO selected_command FROM {COMMANDS_RECORD}
    #                 WHERE is_processed = false AND command LIKE '%_START'
    #                 ORDER BY
    #                     CASE 
    #                         WHEN source = 'cloud' THEN 1
    #                         WHEN source = 'remote' THEN 2
    #                         ELSE 3
    #                     END,
    #                     timestamp DESC
    #                 LIMIT 1;
    #                 IF NOT FOUND THEN
    #                     SELECT * INTO selected_command FROM {COMMANDS_RECORD}
    #                     WHERE is_processed = false AND command LIKE '%_STOP'
    #                     ORDER BY
    #                         CASE
    #                             WHEN source = 'remote' THEN 1
    #                             WHEN source = 'cloud' THEN 2
    #                             ELSE 3
    #                         END,
    #                         timestamp DESC
    #                     LIMIT 1;
    #                 END IF;
    #             END IF;
    #         ELSE
    #             -- If is_service or is_remote is true
    #             IF is_service OR is_remote OR is_disabled THEN
    #                 SELECT * INTO selected_command FROM {COMMANDS_RECORD}
    #                 WHERE is_processed = false AND command LIKE '%_START'
    #                 ORDER BY
    #                     CASE 
    #                         WHEN source = 'cloud' THEN 1
    #                         WHEN source = 'remote' THEN 2
    #                         ELSE 3
    #                     END,
    #                     timestamp DESC
    #                 LIMIT 1;
    #                 IF NOT FOUND THEN
    #                     SELECT * INTO selected_command FROM {COMMANDS_RECORD}
    #                     WHERE is_processed = false AND command LIKE '%_STOP'
    #                     ORDER BY
    #                         CASE
    #                             WHEN source = 'local' THEN 1
    #                             WHEN source = 'remote' THEN 2
    #                             WHEN source = 'cloud' THEN 3
    #                             ELSE 4
    #                         END,
    #                         timestamp DESC
    #                     LIMIT 1;
    #                 END IF;
    #             -- If is_disabled is false
    #             ELSE
    #                 SELECT * INTO selected_command FROM {COMMANDS_RECORD}
    #                 WHERE is_processed = false AND command LIKE '%_START'
    #                 ORDER BY
    #                     CASE
    #                         WHEN source = 'cloud' THEN 1
    #                         WHEN source = 'remote' THEN 2
    #                         WHEN source = 'local' THEN 3
    #                         ELSE 4
    #                     END,
    #                     timestamp DESC
    #                 LIMIT 1;
    #                 IF NOT FOUND THEN
    #                     SELECT * INTO selected_command FROM {COMMANDS_RECORD}
    #                     WHERE is_processed = false AND command LIKE '%_STOP'
    #                     ORDER BY
    #                         CASE
    #                             WHEN source = 'local' THEN 1
    #                             WHEN source = 'remote' THEN 2
    #                             WHEN source = 'cloud' THEN 3
    #                             ELSE 4
    #                         END,
    #                         timestamp DESC
    #                     LIMIT 1;
    #                 END IF;
    #             END IF;
    #         END IF;

    #         IF FOUND THEN
    #             INSERT INTO {CURRENT_COMMAND} (id, command_id, command_status)
    #             VALUES (1, selected_command.command_id, 'none')
    #             ON CONFLICT (id)
    #             DO UPDATE SET command_id = excluded.command_id, command_status = excluded.command_status, timestamp = CURRENT_TIMESTAMP;

    #             UPDATE {COMMANDS_RECORD}
    #             SET is_processed = true,
    #                 is_activated = CASE WHEN command_id = selected_command.command_id THEN true ELSE is_activated END;

    #         END IF;
    #         RETURN NEW;
    #     END;
    #     $$ LANGUAGE plpgsql;
    # """

}

TRIGGERS_FOR_GENERATE_CURRENT_COMMAND_ORIG = f"""
        -- Trigger for command_record
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trigger_on_commands_record') THEN
                CREATE TRIGGER trigger_on_commands_record
                AFTER INSERT ON {COMMANDS_RECORD}
                FOR EACH ROW
                EXECUTE FUNCTION {GENERATE_CURRENT_COMMAND}();
            END IF;
        END $$;

        -- Trigger for current_machine_control_statuses
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trigger_on_current_machine_control_statuses') THEN
                CREATE TRIGGER trigger_on_current_machine_control_statuses
                AFTER INSERT OR UPDATE ON {CURRENT_MACHINE_CONTROL_STATUSES}
                FOR EACH ROW
                EXECUTE FUNCTION {GENERATE_CURRENT_COMMAND}();
            END IF;
        END $$;       
    """

TRIGGERS_FOR_GENERATE_CURRENT_COMMAND = f"""
        -- Trigger for command_record
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trigger_on_commands_record') THEN
                CREATE TRIGGER trigger_on_commands_record
                AFTER INSERT ON {COMMANDS_RECORD}
                FOR EACH ROW
                EXECUTE FUNCTION {GENERATE_CURRENT_COMMAND}();
            END IF;
        END $$;   
    """

class Testing:
    def __init__(self) -> None:
        self._current_timezone = pytz.timezone(TIMEZONE)
        _log_filename = (
            f"{datetime.now(self._current_timezone).strftime('%d-%m-%Y_%H:%M:%S')}.txt"
        )
        logging.basicConfig(
            # filename=f"{os.getcwd()}/logs/{_log_filename}",
            level=LOGGING_LEVEL_DICT.get(LOG_LEVEL, logging.INFO),
            format="{asctime} {levelname:<8} {message}",
            style="{",
        )
        self._machine_id = "machine123"
        self._conn_str = self._get_valid_connection_str('testing', 'postgres', 'entersecretpassword', POSTGRES_SERVICE_NAME)
        # self._setup_database()
        self._machine_config = self._get_all_configs()

    def _setup_enums(self, _conn):
        for _enum_type, _command in ENUMS.items():
            with _conn.cursor() as cur:
                try:
                    cur.execute(_command)
                except errors.DuplicateObject:                        
                    continue

    def _setup_tables(self, _conn):
        for _table_name, _comamnd in DATABASE_TABLES.items():
            if not self.table_exists(_conn, _table_name):
                with _conn.cursor() as cur:
                    cur.execute(_comamnd)
                    logging.info(f"{_table_name} Table Created.")
            else:
                logging.debug(f"Table name {_table_name} already exists.")

    def _setup_procedures(self, _conn):
        for _procedure_name, _command in PROCEDURES_CREATE_COMMANDS.items():
            with _conn.cursor() as cur:
                cur.execute(_command)

    def _setup_triggers(self, _conn):
        with _conn.cursor() as cur:
            cur.execute(TRIGGERS_FOR_GENERATE_CURRENT_COMMAND)

    def _setup_database(self):
        try:
            _db_connection = psycopg2.connect(self._conn_str)
            _db_connection.autocommit = True
            self._setup_enums(_db_connection)
            self._setup_tables(_db_connection)
            self._setup_procedures(_db_connection)
            self._setup_triggers(_db_connection)            
            _db_connection.close()
        except Error as e:
            logging.error(f"(f) _setup_db_cursor - An error occured: {e}")
            raise SystemExit(1)

    def _get_valid_connection_str(self, db_name, username, password, host='localhost', port='5432'):
        try:
            if db_name != 'postgres-db':
                """ Check if the database is already existed or not. If not create database """
                temp_db_connection = psycopg2.connect(f"user='{username}' password='{password}' host='{host}' port='{port}'")
                temp_db_connection.autocommit = True
                with temp_db_connection.cursor() as cur:
                    cur.execute("SELECT datname FROM pg_database;")
                    databases = [row[0] for row in cur.fetchall()]
                    logging.debug(f"existed databases - {databases}")
                    if not db_name in databases:
                        _command_str = "CREATE DATABASE %s;"%db_name
                        # cur.execute(f"CREATE DATABASE {db_name};")
                        cur.execute(_command_str)
                        logging.warning(f"Database {db_name} created successfully.")
            _conn_str = f"dbname='{db_name}' user='{username}' password='{password}' host='{host}' port='{port}'"
            db_connection = psycopg2.connect(_conn_str)
            db_connection.autocommit = True
            db_connection.close()
            return _conn_str
        except Error as e:
            logging.error(f"(f) _get_valid_connection_str - An error occured: {e}")
            raise SystemExit(1)

    def table_exists(self, _conn, table_name):
        with _conn.cursor() as cur:
            cur.execute(
                f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_name = '{table_name}'
                    );
                """
            )
            return cur.fetchone()[0]

    def _get_all_configs(self):
        ''' Pull all the rows from the table 
        and combine as one big dict by using config_name as key'''
        _combined_configs = {}
        # pull all data
        # append each indivadual to _combined_configs
        return _combined_configs
    
    def _fetchone_from_current_type_table(self, _conn, _table_name):
        try:
            with _conn.cursor(cursor_factory=DictCursor) as cur:
                cur.execute(f"SELECT * FROM {_table_name};")
                row = cur.fetchone()
            return row
        except Error as e:
            logging.error(f"(f) _fetchone_from_current_data - an error occure : {e}")
            
    def _check_panel_selection(self, _conn, _current_panel_selection):
        ''' fetchone from {CURRENT_PANEL_SELECTION} table 
        and check if the is_processed flog is true 
        if not read the panel selection and excute accordingly'''
        # fetch data
        _get_data = self._fetchone_from_current_type_table(_conn, CURRENT_PANEL_SELECTION)
        # check flag and excute accordingly
        if _get_data is not None and not(_get_data["is_processed"]):
            _new_panel_selection = _get_data["panel_selection"]
            _related_command = "ALL_START" if _new_panel_selection in ["aa","a","b","color"] else "ALL_STOP"
            # when panel_selection have to excute check the prev_panel_selection
            if _current_panel_selection != _new_panel_selection:
                # to prevent from going into below condition. eg- current(a) == new(a) do not have to stop to re-run
                if _current_panel_selection in ["aa","a","b","color"] and _related_command == "ALL_START":
                    # and if prev panel_selection is in [aa,a,b,color] generate stop command 
                    logging.warning("(f) _check_panel_selection - get new panel selection to start new batch while a batch is active")
                    logging.warning("(f) _check_panel_selection - generate local stop command to end current active batch")
                    self._command_generator(_conn, "ALL_STOP", json.dumps(self._machine_config), "local", self._machine_id, _new_panel_selection)
                    _delay_start_command = threading.Timer(
                        MIN_SEQUENCE_COMMANDS_WAIT_TIME, 
                        self._command_generator, 
                        [_conn, _related_command, json.dumps(self._machine_config), "local", self._machine_id, _new_panel_selection])
                    _delay_start_command.start()
                else:
                    self._command_generator(_conn, _related_command, json.dumps(self._machine_config), "local", self._machine_id, _new_panel_selection)
            else:
                self._command_generator(_conn, _related_command, json.dumps(self._machine_config), "local", self._machine_id, _new_panel_selection)
            try:
                with _conn.cursor() as cur:
                    cur.execute(f"""
                        UPDATE {CURRENT_PANEL_SELECTION}
                        SET is_processed = true
                        WHERE is_processed = false
                    """)
                    _conn.commit()
            except Error as e:
                logging.error(f"(f) _check_panel_selection - Error updating data : {e}")
                _conn.rollback()
            return _new_panel_selection
        return _current_panel_selection

    def _command_generator(self, _conn, _command, _config, _source, _commander, _panel_selection):
        try:
            with _conn.cursor(cursor_factory=DictCursor) as cur:
                cur.execute(f"""
                    INSERT INTO {COMMANDS_RECORD} (command, machine_config, source, commander_id, panel_selection)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING command_id;
                """, (_command, _config, _source, _commander, _panel_selection))
                _new_id = cur.fetchone()["command_id"]
                logging.info(f"new row with id - {_new_id} insert to table {COMMANDS_RECORD}")
                _conn.commit()
        except Error as e:
            logging.error(f"(f) _command_generator - Error inserting data : {e}")
            _conn.rollback()

if __name__=="__main__":
    testing = Testing()
    _conn = psycopg2.connect(testing._conn_str)
    _conn.autocommit = True
    _current_panel_selection = "off"
    for a in range(10):
        current_panel_selection = testing._check_panel_selection(_conn, _current_panel_selection)
        time.sleep(2)
    pass
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

VALID_SOURCE = "valid_source"
VALID_MODE = "valid_mode"
VALID_COMMAND_STATUS = "valid_command_status"
VALID_PANEL_SELECTION = "valid_panel_selection"
VALID_MACHINE_DISPLAY_STATUS = "valid_machine_display_status"
VALID_ALLOCATION_ACTION = "valid_allocation_action"

MACHINE_INFO = "machine_info"
MACHINE_PROPERTIES_INFO = "machine_properties_info"
TECHNICIAN_COMMANDS_RECORD = "technician_commands_record"
CALL_CENTER_COMMANDS_RECORD = "call_center_commands_record"
PANEL_SELECTIONS_RECORD = "panel_selections_record"
SELF_URGENT_STOP_COMMANDS_RECORD = "self_urgent_stop_commands_record"
COMMANDS_RECORD = "commands_record"
ROS_NODES_CONFIGS = "ros_nodes_configs"
MODES_RECORD = "modes_record"
COMMAND_MAP = "command_map"
MACHINE_DISABLE_ENABLE_RECORD = "machine_disable_enable_record"
REMOTE_CONTROL_RECORD = "remote_control_record"
MACHINE_REGISTRATION_RECORD = "machine_registration_record"
MACHINE_CONTROL_FLAGS = "machine_control_flags"
ROS_NODES_ERROR_RECORD = "ros_nodes_error_record"
ROS_NODES_WARNING_RECORD = "ros_nodes_warning_record"
CREDITS_ALLOCATION_RECORD = "credits_allocation_record"
BATCHES_RECORD = "batches_record"
LONGAN_LOT_INFO_RECORD = "longan_lot_info_record"

CURRENT_FACTORY_INFO = "current_factory_info"
CURRENT_MACHINE_CONTROL_FLAGS = "current_machine_control_flags"
CURRENT_COMMAND = "current_command"
CURRENT_PANEL_SELECTION = "current_panel_selection"
CURRENT_CREDITS_BALANCE = "current_credits_balance"
CURRENT_NODES_STATUS = "current_nodes_status"
CURRENT_SORTER_DISPLAY = "current_sorter_display"

''' Store Procedures '''
TURN_OFF_IS_LATEST_FLAG = "TURN_OFF_IS_LATEST_FLAG"
UPDATE_CURRENT_FACTORY_INFO = "update_current_factory_info"
UPDATE_CURRENT_MACHINE_CONTROL_FLAGS = "update_machine_control_flags"
UPDATE_CURRENT_COMMAND = "generate_current_command"
CHECK_TO_INSERT_REMOTE_CONTROL_FALSE_RECORD = "check_to_insert_remote_control_false_record"
INSERT_PANEL_SELECTIONS_RECORD = "insert_panel_selection_record"
INSERT_SELF_URGENT_STOP_COMMANDS_RECORD = "insert_self_urgent_stop_commands_record"
INSERT_COMMANDS_RECORD = "insert_commands_record"

''' Triggers '''
REGISTRATION_RECORD_BEFORE_INSERTED_TRIGGER = "registration_record_before_insert_trigger"
REGISTRATION_RECORD_INSERTED_TRIGGER_ONE = "registration_record_inserted_trigger_one"
REGISTRATION_RECORD_INSERTED_TRIGGER_TWO = "registration_record_inserted_trigger_two"
DISABLE_ENABLE_RECORD_BEFORE_INSERTED_TRIGGER = "disable_enable_record_before_insert_trigger"
DISABLE_ENABLE_RECORD_INSERTED_TRIGGER_ONE = "disable_enable_record_inserted_trigger_one"
DISABLE_ENABLE_RECORD_INSERTED_TRIGGER_TWO = "disable_enable_record_inserted_trigger_two"
REMOTE_CONTROL_RECORD_BEFORE_INSERTED_TRIGGER = "remote_control_record_before_insert_trigger"
REMOTE_CONTROL_RECORD_INSERTED_TRIGGER = "remote_control_record_inserted_trigger"
ROS_NODES_ERROR_RECORD_INSERTED_TRIGGER = "ros_nodes_error_record_inserted_trigger"
TECHNICIAN_COMMANDS_RECORD_BEFORE_INSERTED_TRIGGER = "technician_commands_record_before_inserted_trigger"
TECHNICIAN_COMMANDS_RECORD_INSERTED_TRIGGER = "technician_commands_record_inserted_trigger"
CALL_CENTER_COMMANDS_RECORD_BEFORE_INSERTED_TRIGGER = "call_center_commands_record_before_inserted_trigger"
CALL_CENTER_COMMANDS_RECORD_INSERTED_TRIGGER = "call_center_commands_record_inserted_trigger"
PANEL_SELECTIONS_RECORD_BEFORE_INSERTED_TRIGGER = "panel_selection_record_before_inserted_trigger"
PANEL_SELECTIONS_RECORD_INSERTED_TRIGGER = "panel_selection_record_inserted_trigger"
SELF_URGENT_STOP_COMMANDS_RECORD_BEFORE_INSERTED_TRIGGER = "self_urgent_stop_commands_record_before_inserted_trigger"
SELF_URGENT_STOP_COMMANDS_RECORD_INSERTED_TRIGGER = "self_urgent_stop_commands_record_inserted_trigger"
CURRENT_PANEL_SELECTION_UPDATED_TRIGGER = "current_panel_selection_updated_trigger"
COMMANDS_RECORD_INSERTED_TRIGGER = "commands_record_inserted_trigger"

TABLES_WITH_DEFAULT_ROW = [CURRENT_FACTORY_INFO, CURRENT_MACHINE_CONTROL_FLAGS, CURRENT_COMMAND, CURRENT_PANEL_SELECTION]

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
    VALID_SOURCE : f"""
        CREATE TABLE {VALID_SOURCE} (
            id SERIAL PRIMARY KEY,
            valid_value VARCHAR(56),
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
    VALID_MODE : f"""
        CREATE TABLE {VALID_MODE} (
            id SERIAL PRIMARY KEY,
            valid_value VARCHAR(56),
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
    VALID_COMMAND_STATUS : f"""
        CREATE TABLE {VALID_COMMAND_STATUS} (
            id SERIAL PRIMARY KEY,
            valid_value VARCHAR(56),
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
    VALID_PANEL_SELECTION : f"""
        CREATE TABLE {VALID_PANEL_SELECTION} (
            id SERIAL PRIMARY KEY,
            valid_value VARCHAR(56),
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
    VALID_MACHINE_DISPLAY_STATUS : f"""
        CREATE TABLE {VALID_MACHINE_DISPLAY_STATUS} (
            id SERIAL PRIMARY KEY,
            valid_value VARCHAR(56),
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
    VALID_ALLOCATION_ACTION : f"""
        CREATE TABLE {VALID_ALLOCATION_ACTION} (
            id SERIAL PRIMARY KEY,
            valid_value VARCHAR(56),
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
    MACHINE_INFO : f"""
        CREATE TABLE {MACHINE_INFO} (
            id SERIAL PRIMARY KEY,
            machine_uid VARCHAR(255),
            alias VARCHAR(255),
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """,
    MACHINE_PROPERTIES_INFO: f"""
        CREATE TABLE {MACHINE_PROPERTIES_INFO} (
            id SERIAL PRIMARY KEY,
            machine_uid VARCHAR(255),
            property_describtion TEXT,
            property_value VARCHAR(255),
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """,
    MACHINE_REGISTRATION_RECORD: f"""
        CREATE TABLE {MACHINE_REGISTRATION_RECORD} (
            id SERIAL PRIMARY KEY,
            is_registered BOOLEAN NOT NULL DEFAULT true,
            registered_by_id VARCHAR(56),
            registered_source_id INTEGER REFERENCES {VALID_SOURCE}(id),
            factory_id VARCHAR(56) NULL,
            factory_name VARCHAR(255) NULL,
            machine_id INTEGER REFERENCES {MACHINE_INFO}(id) NULL,
            is_latest BOOLEAN NOT NULL DEFAULT true,
            is_uploaded BOOLEAN NOT NULL DEFAULT false,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
    MACHINE_DISABLE_ENABLE_RECORD: f"""
        CREATE TABLE {MACHINE_DISABLE_ENABLE_RECORD} (
            id SERIAL PRIMARY KEY,
            is_disabled BOOLEAN NOT NULL DEFAULT true,
            disabled_by_id VARCHAR(56),
            disabled_source_id INTEGER REFERENCES {VALID_SOURCE}(id),
            factory_id VARCHAR(56),
            machine_id INTEGER REFERENCES {MACHINE_INFO}(id) NULL,
            is_latest BOOLEAN NOT NULL DEFAULT true,
            is_uploaded BOOLEAN NOT NULL DEFAULT false,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
    REMOTE_CONTROL_RECORD: f"""
        CREATE TABLE {REMOTE_CONTROL_RECORD} (
            id SERIAL PRIMARY KEY,
            is_remote BOOLEAN NOT NULL DEFAULT false,
            requested_time_minute INTEGER DEFAULT 15,
            requested_by_id VARCHAR(56),
            requested_source_id INTEGER REFERENCES {VALID_SOURCE}(id),
            factory_id VARCHAR(56),
            machine_id INTEGER REFERENCES {MACHINE_INFO}(id) NULL,
            session_expired_time TIMESTAMP DEFAULT NULL,
            is_expired BOOLEAN DEFAULT false,
            is_latest BOOLEAN NOT NULL DEFAULT true,
            is_uploaded BOOLEAN NOT NULL DEFAULT false,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """,
    TECHNICIAN_COMMANDS_RECORD: f"""
        CREATE TABLE {TECHNICIAN_COMMANDS_RECORD} (
            id SERIAL PRIMARY KEY,
            command_str VARCHAR(56),
            remote_id INTEGER REFERENCES {REMOTE_CONTROL_RECORD}(id) NULL,
            technician_id VARCHAR(128),
            command_config JSON,
            factory_id VARCHAR(56),
            machine_id INTEGER REFERENCES {MACHINE_INFO}(id) NULL,
            is_uploaded BOOLEAN NOT NULL DEFAULT false,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """,
    CALL_CENTER_COMMANDS_RECORD: f"""
        CREATE TABLE {CALL_CENTER_COMMANDS_RECORD} (
            id SERIAL PRIMARY KEY,
            command_str VARCHAR(56),
            remote_id INTEGER REFERENCES {REMOTE_CONTROL_RECORD}(id) NULL,
            agent_id VARCHAR(128),
            command_config JSON,
            factory_id VARCHAR(56),
            machine_id INTEGER REFERENCES {MACHINE_INFO}(id) NULL,
            is_uploaded BOOLEAN NOT NULL DEFAULT false,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """,
    PANEL_SELECTIONS_RECORD: f"""
        CREATE TABLE {PANEL_SELECTIONS_RECORD} (
            id SERIAL PRIMARY KEY,
            valid_panel_selection_id INTEGER REFERENCES {VALID_PANEL_SELECTION}(id),
            factory_id VARCHAR(56),
            machine_id INTEGER REFERENCES {MACHINE_INFO}(id) NULL,
            is_uploaded BOOLEAN NOT NULL DEFAULT false,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """,
    SELF_URGENT_STOP_COMMANDS_RECORD: f"""
        CREATE TABLE {SELF_URGENT_STOP_COMMANDS_RECORD} (
            id SERIAL PRIMARY KEY,
            command_str VARCHAR(16) NOT NULL DEFAULT 'self_stop' CHECK (command_str = 'self_stop'),
            disable_enable_id INTEGER NULL,
            error_id INTEGER NULL,
            invalid_command_record_id INTEGER NULL,
            factory_id VARCHAR(56),
            machine_id INTEGER REFERENCES {MACHINE_INFO}(id) NULL,
            is_uploaded BOOLEAN NOT NULL DEFAULT false,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """,
    COMMAND_MAP: f"""
        CREATE TABLE {COMMAND_MAP} (
            id SERIAL PRIMARY KEY,
            command_str VARCHAR(56),
            ros_command_str VARCHAR(56),
            eq_panel_selection_id INTEGER REFERENCES {VALID_PANEL_SELECTION}(id),
            mode_id INTEGER REFERENCES {VALID_MODE}(id),
            timeout_sec INTEGER DEFAULT -1,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """,
    COMMANDS_RECORD: f"""
        CREATE TABLE {COMMANDS_RECORD} (
            id SERIAL PRIMARY KEY,
            command_map_id INTEGER REFERENCES {COMMAND_MAP}(id) NULL,
            command_config JSON,
            technician_command_id INTEGER REFERENCES {TECHNICIAN_COMMANDS_RECORD}(id) NULL,
            call_center_command_id INTEGER REFERENCES {CALL_CENTER_COMMANDS_RECORD}(id) NULL,
            panel_selection_id INTEGER REFERENCES {PANEL_SELECTIONS_RECORD}(id) NULL,
            self_urgent_stop_id INTEGER REFERENCES {SELF_URGENT_STOP_COMMANDS_RECORD}(id) NULL,
            is_processed BOOLEAN NOT NULL DEFAULT false,
            is_activated BOOLEAN NOT NULL DEFAULT false,
            is_uploaded BOOLEAN NOT NULL DEFAULT false,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """,
    ROS_NODES_ERROR_RECORD: f"""
        CREATE TABLE {ROS_NODES_ERROR_RECORD} (
            id SERIAL PRIMARY KEY,
            node_type VARCHAR(56),
            node_name VARCHAR(56),
            error_msg VARCHAR(255),
            factory_id VARCHAR(56),
            machine_id INTEGER REFERENCES {MACHINE_INFO}(id) NULL,
            error_start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            error_end_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """,
    ROS_NODES_CONFIGS: f"""
        CREATE TABLE {ROS_NODES_CONFIGS} (
            id SERIAL PRIMARY KEY,
            node_type VARCHAR(56),
            config JSON,
            factory_id VARCHAR(56),
            machine_id INTEGER REFERENCES {MACHINE_INFO}(id) NULL,
            is_uploaded BOOLEAN NOT NULL DEFAULT false,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """,
    ROS_NODES_WARNING_RECORD: f"""
        CREATE TABLE {ROS_NODES_WARNING_RECORD} (
            id SERIAL PRIMARY KEY,
            node_type VARCHAR(56),
            node_name VARCHAR(56),
            warning_msg VARCHAR(255),
            factory_id VARCHAR(56),
            machine_id INTEGER REFERENCES {MACHINE_INFO}(id) NULL,
            warning_start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            warning_end_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """,
    CURRENT_FACTORY_INFO: f"""
        CREATE TABLE {CURRENT_FACTORY_INFO} (
            id SERIAL PRIMARY KEY,
            factory_id VARCHAR(56) NULL,
            factory_name VARCHAR(255) NULL,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
    CURRENT_MACHINE_CONTROL_FLAGS: f"""
        CREATE TABLE {CURRENT_MACHINE_CONTROL_FLAGS} (
            id SERIAL PRIMARY KEY,
            disable_enable_id INTEGER REFERENCES {MACHINE_DISABLE_ENABLE_RECORD}(id) NULL,
            remote_id INTEGER REFERENCES {REMOTE_CONTROL_RECORD}(id) NULL,
            registration_id INTEGER REFERENCES {MACHINE_REGISTRATION_RECORD}(id) NULL,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
    CURRENT_COMMAND: f"""
        CREATE TABLE {CURRENT_COMMAND} (
            id SERIAL PRIMARY KEY,
            command_record_id INTEGER REFERENCES {COMMANDS_RECORD}(id) NULL,
            command_status_id INTEGER REFERENCES {VALID_COMMAND_STATUS}(id) NULL,
            is_processed BOOLEAN NOT NULL DEFAULT false,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """,
    CURRENT_PANEL_SELECTION: f"""
        CREATE TABLE {CURRENT_PANEL_SELECTION} (
            id SERIAL PRIMARY KEY,
            valid_panel_selection_id INTEGER REFERENCES {VALID_PANEL_SELECTION}(id) NULL,
            is_uploaded BOOLEAN NOT NULL DEFAULT false,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """,
    # "CURRENT_SORTER_DISPLAY" : f"""
    #     CREATE TABLE {CURRENT_SORTER_DISPLAY} ();
    # """
}

PROCEDURES_CREATE_SQL_COMMANDS_DICT = {
    TURN_OFF_IS_LATEST_FLAG : f"""
        CREATE OR REPLACE FUNCTION {TURN_OFF_IS_LATEST_FLAG}()
        RETURNS TRIGGER AS $$
        DECLARE
            arg_table_name VARCHAR;
        BEGIN
            arg_table_name := TG_ARGV[0];
            -- deactive all the is_latest flag
            EXECUTE 'UPDATE ' || arg_table_name || ' SET is_latest = false';
            IF to_jsonb(NEW) ? 'session_expired_time' THEN
                IF NEW.requested_time_minute > 0 THEN
                    NEW.session_expired_time := CURRENT_TIMESTAMP + (NEW.requested_time_minute * INTERVAL '1 minutes');
                END IF;
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    """,
    CHECK_TO_INSERT_REMOTE_CONTROL_FALSE_RECORD : f"""
        CREATE OR REPLACE FUNCTION {CHECK_TO_INSERT_REMOTE_CONTROL_FALSE_RECORD}()
        RETURNS TRIGGER AS $$
        DECLARE
            _session_expired_time TIMESTAMP;
            _factory_id VARCHAR;
            machine_id INTEGER;
            _is_remote_id_col BOOLEAN;
            _is_command_str_col BOOLEAN;
            _is_panel_selection_id BOOLEAN;
            _latest_data_before_insert RECORD;
            _is_activated BOOLEAN;
            _is_remote_agent_col BOOLEAN;
            _panel_selection_id INTEGER;
        BEGIN
            SELECT factory_id INTO _factory_id FROM {CURRENT_FACTORY_INFO};
            SELECT id INTO machine_id FROM {MACHINE_INFO};
            SELECT session_expired_time INTO _session_expired_time FROM {REMOTE_CONTROL_RECORD}
            WHERE is_latest = true;
            IF _session_expired_time < CURRENT_TIMESTAMP THEN
                UPDATE {REMOTE_CONTROL_RECORD}
                SET is_expired = true WHERE is_latest = true;
                INSERT INTO {REMOTE_CONTROL_RECORD} (requested_by_id, requested_source_id, factory_id, machine_id)
                VALUES (machine_id, 1, _factory_id, machine_id);
            END IF;
            EXECUTE 'SELECT * FROM ' || TG_TABLE_NAME || ' ORDER BY timestamp DESC LIMIT 1' INTO _latest_data_before_insert;
            IF _latest_data_before_insert IS NULL THEN
                NEW.factory_id := _factory_id;
                NEW.machine_id := machine_id;
                RETURN NEW;
            ELSE
                SELECT true INTO _is_remote_id_col FROM information_schema.columns
                WHERE table_name = TG_TABLE_NAME AND column_name = 'remote_id';
                SELECT true INTO _is_command_str_col FROM information_schema.columns
                WHERE table_name = TG_TABLE_NAME AND column_name = 'command_str';
                SELECT true INTO _is_panel_selection_id FROM information_schema.columns
                WHERE table_name = TG_TABLE_NAME AND column_name = 'panel_selection_id';
                IF _is_remote_id_col AND _is_command_str_col THEN
                    NEW.factory_id := 'not sure';
                    IF (NEW.command_str = _latest_data_before_insert.command_str) 
                    AND (NEW.remote_id = _latest_data_before_insert.remote_id) THEN
                        SELECT true INTO _is_remote_agent_col FROM information_schema.columns
                        WHERE table_name = TG_TABLE_NAME AND column_name = 'agent_id';
                        IF _is_remote_agent_col THEN
                            SELECT is_activated INTO _is_activated FROM {COMMANDS_RECORD} 
                            WHERE call_center_command_id = _latest_data_before_insert.id;
                            -- _is_activated can be null if row not found with the condition provided
                            IF _is_activated IS NOT NULL THEN
                                IF _is_activated THEN
                                    RETURN NULL;
                                END IF;
                            END IF;
                            NEW.factory_id := 'it is agent';
                        ELSE
                            SELECT is_activated INTO _is_activated FROM {COMMANDS_RECORD} 
                            WHERE technician_command_id = _latest_data_before_insert.id;
                            -- _is_activated can be null if row not found with the condition provided
                            IF _is_activated IS NOT NULL THEN
                                IF _is_activated THEN
                                    RETURN NULL;
                                END IF;
                            END IF;
                            NEW.factory_id := 'it is technician';
                        END IF;
                    END IF;
                    NEW.machine_id := machine_id;
                    RETURN NEW;
                ELSIF _is_command_str_col THEN
                    -- condition for self_urgent_stop table where remote_id not exist
                    -- for self_urgent_stop table, just insert without any condition
                    NEW.factory_id := _factory_id;
                    NEW.machine_id := machine_id;
                    RETURN NEW;
                ELSIF _is_panel_selection_id THEN
                    IF NEW.panel_selection_id = _latest_data_before_insert.panel_selection_id THEN
                        SELECT panel_selection_id INTO _panel_selection_id FROM {COMMANDS_RECORD}
                        ORDER BY timestamp DESC LIMIT 1;
                        SELECT is_activated INTO _is_activated FROM {COMMANDS_RECORD} 
                        WHERE panel_selection_id = _latest_data_before_insert.id;
                        -- _is_activated can be null if row not found with the condition provided
                        -- _panel_selection_id will be null if not the latest, 
                        -- this condition is required to check, unlike the remote commands 
                        -- because it does not have session period indicator like remote_id
                        IF _is_activated IS NOT NULL AND _panel_selection_id IS NOT NULL THEN
                            IF _is_activated THEN
                                RETURN NULL;
                            END IF;
                        END IF;
                    END IF;
                    NEW.factory_id := _factory_id;
                    NEW.machine_id := machine_id;
                    RETURN NEW;
                ELSE
                    RETURN NULL;
                END IF;
            END IF;
        END;
        $$ LANGUAGE plpgsql;
    """,
    UPDATE_CURRENT_FACTORY_INFO : f"""
        CREATE OR REPLACE FUNCTION {UPDATE_CURRENT_FACTORY_INFO} ()
        RETURNS TRIGGER AS $$
        BEGIN
            UPDATE {CURRENT_FACTORY_INFO} 
            SET factory_id = NEW.factory_id, factory_name = NEW.factory_name;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    """,
    UPDATE_CURRENT_MACHINE_CONTROL_FLAGS : f"""
        CREATE OR REPLACE FUNCTION {UPDATE_CURRENT_MACHINE_CONTROL_FLAGS} ()
        RETURNS TRIGGER AS $$
        BEGIN
            IF TG_NAME = '{DISABLE_ENABLE_RECORD_INSERTED_TRIGGER_TWO}' THEN
                UPDATE {CURRENT_MACHINE_CONTROL_FLAGS} 
                SET disable_enable_id = NEW.id;
            ELSIF TG_NAME = '{REMOTE_CONTROL_RECORD_INSERTED_TRIGGER}' THEN
                UPDATE {CURRENT_MACHINE_CONTROL_FLAGS} 
                SET remote_id = NEW.id;
            ELSIF TG_NAME = '{REGISTRATION_RECORD_INSERTED_TRIGGER_TWO}' THEN
                UPDATE {CURRENT_MACHINE_CONTROL_FLAGS} 
                SET registration_id = NEW.id;
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    """,
    INSERT_SELF_URGENT_STOP_COMMANDS_RECORD : f"""
        CREATE OR REPLACE FUNCTION {INSERT_SELF_URGENT_STOP_COMMANDS_RECORD} ()
        RETURNS TRIGGER AS $$
        DECLARE
            current_panel_selection_id INTEGER;
        BEGIN
            IF TG_NAME = '{ROS_NODES_ERROR_RECORD_INSERTED_TRIGGER}' THEN
                INSERT INTO {SELF_URGENT_STOP_COMMANDS_RECORD} (error_id)
                VALUES (NEW.id);
            ELSIF TG_NAME = '{DISABLE_ENABLE_RECORD_INSERTED_TRIGGER_ONE}' THEN
                IF NEW.is_disabled THEN
                    INSERT INTO {SELF_URGENT_STOP_COMMANDS_RECORD} (disable_enable_id)
                    VALUES (NEW.id);
                END IF;
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    """,
    INSERT_PANEL_SELECTIONS_RECORD : f"""
        CREATE OR REPLACE FUNCTION {INSERT_PANEL_SELECTIONS_RECORD} ()
        RETURNS TRIGGER AS $$
        DECLARE
            eq_panel_selection_id INTEGER;
        BEGIN
            SELECT cm.eq_panel_selection_id INTO eq_panel_selection_id
            FROM {COMMANDS_RECORD} AS cr
            JOIN {COMMAND_MAP} AS cm ON cr.command_map_id = cm.id
            WHERE cr.call_center_command_id IS NOT NULL
            AND age(NOW(), cr.timestamp) < interval '1 minute'
            LIMIT 1;

            -- If no matching row is found
            IF eq_panel_selection_id IS NULL THEN
                INSERT INTO {PANEL_SELECTIONS_RECORD} (panel_selection_id)
                VALUES (NEW.panel_selection_id);
            ELSE
                IF eq_panel_selection_id <> NEW.panel_selection_id THEN
                    INSERT INTO {PANEL_SELECTIONS_RECORD} (panel_selection_id)
                    VALUES (NEW.panel_selection_id);
                END IF;
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    """,
    INSERT_COMMANDS_RECORD : f"""
        CREATE OR REPLACE FUNCTION {INSERT_COMMANDS_RECORD} ()
        RETURNS TRIGGER AS $$
        DECLARE
            _command_map_id INTEGER;
        BEGIN
            IF TG_NAME = '{PANEL_SELECTIONS_RECORD_INSERTED_TRIGGER}' THEN
                SELECT id INTO _command_map_id FROM {COMMAND_MAP} WHERE command_str = to_char(NEW.panel_selection_id, 'FM999');
                INSERT INTO {COMMANDS_RECORD} (command_map_id, panel_selection_id)
                VALUES (_command_map_id, NEW.id);
            ELSIF TG_NAME = '{TECHNICIAN_COMMANDS_RECORD_INSERTED_TRIGGER}' THEN
                IF EXISTS (SELECT 1 FROM {REMOTE_CONTROL_RECORD} WHERE id = NEW.remote_id AND is_latest = true) THEN
                    SELECT id INTO _command_map_id FROM {COMMAND_MAP} WHERE command_str = NEW.command_str;
                    INSERT INTO {COMMANDS_RECORD} (command_map_id, technician_command_id)
                    VALUES (_command_map_id, NEW.id);
                END IF;
            ELSIF TG_NAME = '{CALL_CENTER_COMMANDS_RECORD_INSERTED_TRIGGER}' THEN
                IF EXISTS (SELECT 1 FROM {REMOTE_CONTROL_RECORD} WHERE id = NEW.remote_id AND is_latest = true) THEN
                    SELECT id INTO _command_map_id FROM {COMMAND_MAP} WHERE command_str = NEW.command_str;
                    INSERT INTO {COMMANDS_RECORD} (command_map_id, call_center_command_id)
                    VALUES (_command_map_id, NEW.id);
                END IF;
            ELSIF TG_NAME = '{SELF_URGENT_STOP_COMMANDS_RECORD_INSERTED_TRIGGER}' THEN
                SELECT id INTO _command_map_id FROM {COMMAND_MAP} WHERE command_str = NEW.command_str;
                INSERT INTO {COMMANDS_RECORD} (command_map_id, self_urgent_stop_id)
                VALUES (_command_map_id, NEW.id);
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    """,

    UPDATE_CURRENT_COMMAND : f"""
        CREATE OR REPLACE FUNCTION {UPDATE_CURRENT_COMMAND} ()
        RETURNS TRIGGER AS $$
        DECLARE
            selected_command RECORD;
            _mode_type VARCHAR;
            _current_command_panel_selection VARCHAR;
            _command_record_panel_selection VARCHAR;
            _current_remote_id INTEGER;
            _command_remote_id INTEGER;
            _is_remote BOOLEAN;
            _is_register BOOLEAN;
            _is_disabled BOOLEAN;
            selected_command_id INTEGER;
        BEGIN
            SELECT vps.valid_value, vm.valid_value INTO _command_record_panel_selection, _mode_type FROM {COMMANDS_RECORD} AS cr
            JOIN {COMMAND_MAP} AS cm ON cr.command_map_id = cm.id
            JOIN {VALID_MODE} AS vm ON cm.mode_id = vm.id
            JOIN {VALID_PANEL_SELECTION} AS vps ON cm.eq_panel_selection_id = vps.id
            WHERE cr.id = NEW.id;

            SELECT vps.valid_value INTO _current_command_panel_selection FROM {CURRENT_COMMAND} as cc
            LEFT JOIN {COMMANDS_RECORD} AS cr ON cc.command_record_id = cr.id
            LEFT JOIN {COMMAND_MAP} AS cm ON cr.command_map_id = cm.id
            LEFT JOIN {VALID_PANEL_SELECTION} AS vps ON cm.eq_panel_selection_id = vps.id;

            SELECT cmc.remote_id, rc.is_remote, mr.is_registered, mde.is_disabled 
            INTO _current_remote_id, _is_remote, _is_register, _is_disabled
            FROM {CURRENT_MACHINE_CONTROL_FLAGS} AS cmc
            LEFT JOIN {REMOTE_CONTROL_RECORD} AS rc ON cmc.remote_id = rc.id
            LEFT JOIN {MACHINE_REGISTRATION_RECORD} AS mr ON cmc.registration_id = mr.id
            LEFT JOIN {MACHINE_DISABLE_ENABLE_RECORD} AS mde ON cmc.disable_enable_id = mde.id; 

            IF _mode_type = 'oper' THEN
                -- condition to check if the incoming command should be block or not when it comes to all_start         
                IF ((_command_record_panel_selection IN ('aa', 'a', 'b')) AND (_current_command_panel_selection NOT IN ('aa', 'a', 'b'))) 
                OR (_command_record_panel_selection = 'off') THEN
                    IF _is_register THEN
                        IF _is_remote THEN
                            IF EXISTS (SELECT 1 FROM {COMMANDS_RECORD} WHERE id = NEW.id AND call_center_command_id IS NOT NULL) THEN
                                SELECT ccc.remote_id INTO _command_remote_id FROM {COMMANDS_RECORD} AS cr
                                JOIN {CALL_CENTER_COMMANDS_RECORD} AS ccc ON cr.call_center_command_id = ccc.id
                                WHERE cr.id = NEW.id;
                                IF _command_remote_id = _current_remote_id THEN
                                    selected_command_id := NEW.id;   
                                END IF;
                            ELSE
                                IF EXISTS (SELECT 1 FROM {COMMANDS_RECORD} 
                                WHERE id = NEW.id AND self_urgent_stop_id IS NOT NULL) THEN
                                    selected_command_id := NEW.id;
                                END IF;
                            END IF;
                        ELSIF _is_disabled = false THEN
                            IF EXISTS (SELECT 1 FROM {COMMANDS_RECORD} 
                            WHERE id = NEW.id AND (panel_selection_id IS NOT NULL OR self_urgent_stop_id IS NOT NULL)) THEN
                                selected_command_id := NEW.id;
                            END IF;
                        ELSE
                            IF EXISTS (SELECT 1 FROM {COMMANDS_RECORD} 
                            WHERE id = NEW.id AND self_urgent_stop_id IS NOT NULL) THEN
                                selected_command_id := NEW.id;
                            END IF;
                        END IF;
                    END IF;
                ELSE
                    IF _is_register THEN
                        IF EXISTS (SELECT 1 FROM {COMMANDS_RECORD} WHERE id = NEW.id AND call_center_command_id IS NOT NULL) THEN
                            -- only response to remote invalid start command if the remote is true else do not response
                            IF _is_remote THEN
                                -- incoming command is invalid start command and get block, then self urgent stop will get generated
                                INSERT INTO {SELF_URGENT_STOP_COMMANDS_RECORD} (invalid_command_record_id) 
                                VALUES (NEW.id);
                            END IF;
                        ELSE
                            -- only response to local invalid start command if both is_disabled and _is_remote are false else do not reponse
                            IF _is_disabled = false AND _is_remote = false THEN
                                -- incoming command is invalid start command and get block, then self urgent stop will get generated
                                INSERT INTO {SELF_URGENT_STOP_COMMANDS_RECORD} (invalid_command_record_id) 
                                VALUES (NEW.id);
                            END IF;
                        END IF;
                    END IF;
                END IF;
            ELSE
                IF _is_remote THEN
                    IF EXISTS (SELECT 1 FROM {COMMANDS_RECORD} WHERE id = NEW.id AND call_center_command_id IS NOT NULL) THEN
                        SELECT ccc.remote_id INTO _command_remote_id FROM {COMMANDS_RECORD} AS cr
                        JOIN {CALL_CENTER_COMMANDS_RECORD} AS ccc ON cr.call_center_command_id = ccc.id
                        WHERE cr.id = NEW.id;
                        IF (_command_remote_id = _current_remote_id) THEN
                            IF _current_command_panel_selection IS NOT NULL THEN
                                IF _current_command_panel_selection NOT IN ('aa','a','b') THEN
                                    selected_command_id := NEW.id;
                                ELSE
                                    -- incoming command is invalid command and get block, then self urgent stop will get generated
                                    INSERT INTO {SELF_URGENT_STOP_COMMANDS_RECORD} (invalid_command_record_id) 
                                    VALUES (NEW.id);
                                END IF;
                            ELSE
                                selected_command_id := NEW.id;
                            END IF;
                        END IF;
                    ELSIF EXISTS (SELECT 1 FROM {COMMANDS_RECORD} WHERE id = NEW.id AND technician_command_id IS NOT NULL) THEN
                        SELECT tc.remote_id INTO _command_remote_id FROM {COMMANDS_RECORD} AS cr
                        JOIN {TECHNICIAN_COMMANDS_RECORD} AS tc ON cr.technician_command_id = tc.id
                        WHERE cr.id = NEW.id;
                        IF (_command_remote_id = _current_remote_id) THEN
                            IF _current_command_panel_selection IS NOT NULL THEN
                                IF _current_command_panel_selection NOT IN ('aa','a','b') THEN
                                    selected_command_id := NEW.id;
                                ELSE
                                    -- incoming command is invalid command and get block, then self urgent stop will get generated
                                    INSERT INTO {SELF_URGENT_STOP_COMMANDS_RECORD} (invalid_command_record_id) 
                                    VALUES (NEW.id);
                                END IF;
                            ELSE
                                selected_command_id := NEW.id;
                            END IF;
                        END IF;
                    END IF;
                ELSE
                    -- will only response to local setup mode if the remote is false
                    IF EXISTS (SELECT 1 FROM {COMMANDS_RECORD} WHERE id = NEW.id AND panel_selection_id IS NOT NULL) THEN
                        IF _mode_type = 'setup' THEN
                            selected_command_id := NEW.id;
                        END IF;
                    END IF;
                END IF;
            END IF;
            IF selected_command_id IS NOT NULL THEN
                UPDATE {CURRENT_COMMAND} 
                SET command_record_id = selected_command_id, 
                    command_status_id = 1;
            END IF;
            UPDATE {COMMANDS_RECORD} 
            SET is_processed = true, 
                is_activated = CASE WHEN id = selected_command_id THEN true ELSE is_activated END;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    """,
    # for backup purpose
    # UPDATE_CURRENT_COMMAND : f"""
    #     CREATE OR REPLACE FUNCTION {UPDATE_CURRENT_COMMAND} ()
    #     RETURNS TRIGGER AS $$
    #     DECLARE
    #         selected_command RECORD;
    #         _mode_type VARCHAR;
    #         _current_command_panel_selection VARCHAR;
    #         _command_record_panel_selection VARCHAR;
    #         _current_remote_id INTEGER;
    #         _command_remote_id INTEGER;
    #         _is_remote BOOLEAN;
    #         _is_register BOOLEAN;
    #         _is_disabled BOOLEAN;
    #         selected_command_id INTEGER;
    #     BEGIN
    #         SELECT vps.valid_value, vm.valid_value INTO _command_record_panel_selection, _mode_type FROM {COMMANDS_RECORD} AS cr
    #         JOIN {COMMAND_MAP} AS cm ON cr.command_map_id = cm.id
    #         JOIN {VALID_MODE} AS vm ON cm.mode_id = vm.id
    #         JOIN {VALID_PANEL_SELECTION} AS vps ON cm.eq_panel_selection_id = vps.id
    #         WHERE cr.id = NEW.id;

    #         SELECT vps.valid_value INTO _current_command_panel_selection FROM {CURRENT_COMMAND} as cc
    #         JOIN {COMMANDS_RECORD} AS cr ON cc.command_record_id = cr.id
    #         JOIN {COMMAND_MAP} AS cm ON cr.command_map_id = cm.id
    #         JOIN {VALID_PANEL_SELECTION} AS vps ON cm.eq_panel_selection_id = vps.id;

    #         SELECT cmc.remote_id, rc.is_remote, mr.is_registered, mde.is_disabled 
    #         INTO _current_remote_id, _is_remote, _is_register, _is_disabled
    #         FROM {CURRENT_MACHINE_CONTROL_FLAGS} AS cmc
    #         LEFT JOIN {REMOTE_CONTROL_RECORD} AS rc ON cmc.remote_id = rc.id
    #         LEFT JOIN {MACHINE_REGISTRATION_RECORD} AS mr ON cmc.registration_id = mr.id
    #         LEFT JOIN {MACHINE_DISABLE_ENABLE_RECORD} AS mde ON cmc.disable_enable_id = mde.id; 

    #         IF _mode_type = 'oper' THEN
    #             -- condition to check if the incoming command should be block or not when it comes to all_start         
    #             IF ((_command_record_panel_selection IN ('aa', 'a', 'b')) AND (_current_command_panel_selection NOT IN ('aa', 'a', 'b'))) 
    #             OR (_command_record_panel_selection = 'off') THEN
    #                 IF _is_register THEN
    #                     IF _is_remote THEN
    #                         IF EXISTS (SELECT 1 FROM {COMMANDS_RECORD} WHERE id = NEW.id AND call_center_command_id IS NOT NULL) THEN
    #                             SELECT ccc.remote_id INTO _command_remote_id FROM {COMMANDS_RECORD} AS cr
    #                             JOIN {CALL_CENTER_COMMANDS_RECORD} AS ccc ON cr.call_center_command_id = ccc.id
    #                             WHERE cr.id = NEW.id;
    #                             IF _command_remote_id = _current_remote_id THEN
    #                                 selected_command_id := NEW.id;   
    #                             END IF;                             
    #                         ELSIF _is_disabled = false THEN
    #                             IF EXISTS (SELECT 1 FROM {COMMANDS_RECORD} 
    #                             WHERE id = NEW.id AND (panel_selection_id IS NOT NULL OR self_urgent_stop_id IS NOT NULL)) THEN
    #                                 selected_command_id := NEW.id;
    #                             END IF;
    #                         ELSE
    #                             IF EXISTS (SELECT 1 FROM {COMMANDS_RECORD} 
    #                             WHERE id = NEW.id AND self_urgent_stop_id IS NOT NULL) THEN
    #                                 selected_command_id := NEW.id;
    #                             END IF;
    #                         END IF;
    #                     ELSIF _is_disabled = false THEN
    #                         IF EXISTS (SELECT 1 FROM {COMMANDS_RECORD} 
    #                         WHERE id = NEW.id AND (panel_selection_id IS NOT NULL OR self_urgent_stop_id IS NOT NULL)) THEN
    #                             selected_command_id := NEW.id;
    #                         END IF;
    #                     ELSE
    #                         IF EXISTS (SELECT 1 FROM {COMMANDS_RECORD} 
    #                         WHERE id = NEW.id AND self_urgent_stop_id IS NOT NULL) THEN
    #                             selected_command_id := NEW.id;
    #                         END IF;
    #                     END IF;
    #                 END IF;
    #             ELSE
    #                 IF _is_register THEN
    #                     IF EXISTS (SELECT 1 FROM {COMMANDS_RECORD} WHERE id = NEW.id AND call_center_command_id IS NOT NULL) THEN
    #                         -- only response to remote invalid start command if the remote is true else do not response
    #                         IF _is_remote THEN
    #                             -- incoming command is invalid start command and get block, then self urgent stop will get generated
    #                             INSERT INTO {SELF_URGENT_STOP_COMMANDS_RECORD} (invalid_command_record_id) 
    #                             VALUES (NEW.id);
    #                         END IF;
    #                     ELSE
    #                         -- only response to local invalid start command if the disable is false else do not reponse
    #                         IF _is_disabled = false THEN
    #                             -- incoming command is invalid start command and get block, then self urgent stop will get generated
    #                             INSERT INTO {SELF_URGENT_STOP_COMMANDS_RECORD} (invalid_command_record_id) 
    #                             VALUES (NEW.id);
    #                         END IF;
    #                     END IF;
    #                 END IF;
    #             END IF;
    #         ELSE
    #             IF _is_remote THEN
    #                 IF EXISTS (SELECT 1 FROM {COMMANDS_RECORD} WHERE id = NEW.id AND call_center_command_id IS NOT NULL) THEN
    #                     SELECT ccc.remote_id INTO _command_remote_id FROM {COMMANDS_RECORD} AS cr
    #                     JOIN {CALL_CENTER_COMMANDS_RECORD} AS ccc ON cr.call_center_command_id = ccc.id
    #                     WHERE cr.id = NEW.id;
    #                     IF (_command_remote_id = _current_remote_id) AND _current_command_panel_selection NOT IN ('aa','a','b') THEN
    #                         selected_command_id := NEW.id;
    #                     ELSE
    #                         -- incoming command is invalid command and get block, then self urgent stop will get generated
    #                         INSERT INTO {SELF_URGENT_STOP_COMMANDS_RECORD} (invalid_command_record_id) 
    #                         VALUES (NEW.id);
    #                     END IF;
    #                 ELSIF EXISTS (SELECT 1 FROM {COMMANDS_RECORD} WHERE id = NEW.id AND technician_command_id IS NOT NULL) THEN
    #                     SELECT tc.remote_id INTO _command_remote_id FROM {COMMANDS_RECORD} AS cr
    #                     JOIN {TECHNICIAN_COMMANDS_RECORD} AS tc ON cr.technician_command_id = tc.id
    #                     WHERE cr.id = NEW.id;
    #                     IF (_command_remote_id = _current_remote_id) AND _current_command_panel_selection NOT IN ('aa','a','b') THEN
    #                         selected_command_id := NEW.id;
    #                     ELSE
    #                         -- incoming command is invalid command and get block, then self urgent stop will get generated
    #                         INSERT INTO {SELF_URGENT_STOP_COMMANDS_RECORD} (invalid_command_record_id) 
    #                         VALUES (NEW.id);
    #                     END IF;                 
    #                 ELSIF EXISTS (SELECT 1 FROM {COMMANDS_RECORD} WHERE id = NEW.id AND panel_selection_id IS NOT NULL) THEN
    #                     -- for local command it will only response to setup mode
    #                     IF _mode_type = 'setup' THEN
    #                         selected_command_id := NEW.id;
    #                     END IF;
    #                 END IF;
    #             ELSE
    #                 -- will only response to local setup mode if the remote is false
    #                 IF EXISTS (SELECT 1 FROM {COMMANDS_RECORD} WHERE id = NEW.id AND panel_selection_id IS NOT NULL) THEN
    #                     IF _mode_type = 'setup' THEN
    #                         selected_command_id := NEW.id;
    #                     END IF;
    #                 END IF;
    #             END IF;
    #         END IF;
    #         IF selected_command_id IS NOT NULL THEN
    #             UPDATE {CURRENT_COMMAND} 
    #             SET command_record_id = selected_command_id, 
    #                 command_status_id = 1;
    #         END IF;
    #         UPDATE {COMMANDS_RECORD} 
    #         SET is_processed = true, 
    #             is_activated = CASE WHEN id = selected_command_id THEN true ELSE is_activated END;
    #         RETURN NEW;
    #     END;
    #     $$ LANGUAGE plpgsql;
    # """,

}

TRIGGERS_CREATE_SQL_COMMAND_STRING = f"""
        -- Trigger on new machine registration data inserted to update factory_info
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = '{REGISTRATION_RECORD_INSERTED_TRIGGER_ONE}') THEN
                CREATE TRIGGER {REGISTRATION_RECORD_INSERTED_TRIGGER_ONE}
                AFTER INSERT ON {MACHINE_REGISTRATION_RECORD}
                FOR EACH ROW
                EXECUTE FUNCTION {UPDATE_CURRENT_FACTORY_INFO}();
            END IF;
        END $$;

        -- Trigger on new machine registration data inserted to update current machine control flags
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = '{REGISTRATION_RECORD_INSERTED_TRIGGER_TWO}') THEN
                CREATE TRIGGER {REGISTRATION_RECORD_INSERTED_TRIGGER_TWO}
                AFTER INSERT ON {MACHINE_REGISTRATION_RECORD}
                FOR EACH ROW
                EXECUTE FUNCTION {UPDATE_CURRENT_MACHINE_CONTROL_FLAGS}();
            END IF;
        END $$;

        -- Trigger on before new machine registration data inserted to deactive all the is_latest flags
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = '{REGISTRATION_RECORD_BEFORE_INSERTED_TRIGGER}') THEN
                CREATE TRIGGER {REGISTRATION_RECORD_BEFORE_INSERTED_TRIGGER}
                BEFORE INSERT ON {MACHINE_REGISTRATION_RECORD}
                FOR EACH ROW
                EXECUTE FUNCTION {TURN_OFF_IS_LATEST_FLAG}('{MACHINE_REGISTRATION_RECORD}');
            END IF;
        END $$;

        -- Trigger on new machine disable enable data inserted to insert self urgent stop
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = '{DISABLE_ENABLE_RECORD_INSERTED_TRIGGER_ONE}') THEN
                CREATE TRIGGER {DISABLE_ENABLE_RECORD_INSERTED_TRIGGER_ONE}
                AFTER INSERT ON {MACHINE_DISABLE_ENABLE_RECORD}
                FOR EACH ROW
                EXECUTE FUNCTION {INSERT_SELF_URGENT_STOP_COMMANDS_RECORD}();
            END IF;
        END $$;

        -- Trigger on new machine disable enable data inserted to update current machine control flags
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = '{DISABLE_ENABLE_RECORD_INSERTED_TRIGGER_TWO}') THEN
                CREATE TRIGGER {DISABLE_ENABLE_RECORD_INSERTED_TRIGGER_TWO}
                AFTER INSERT ON {MACHINE_DISABLE_ENABLE_RECORD}
                FOR EACH ROW
                EXECUTE FUNCTION {UPDATE_CURRENT_MACHINE_CONTROL_FLAGS}();
            END IF;
        END $$;

        -- Trigger on before new machine disable enable data inserted to deactive all the is_latest flags
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = '{DISABLE_ENABLE_RECORD_BEFORE_INSERTED_TRIGGER}') THEN
                CREATE TRIGGER {DISABLE_ENABLE_RECORD_BEFORE_INSERTED_TRIGGER}
                BEFORE INSERT ON {MACHINE_DISABLE_ENABLE_RECORD}
                FOR EACH ROW
                EXECUTE FUNCTION {TURN_OFF_IS_LATEST_FLAG}('{MACHINE_DISABLE_ENABLE_RECORD}');
            END IF;
        END $$;

        -- Trigger on new machine remote control data inserted to update current machine control flags
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = '{REMOTE_CONTROL_RECORD_INSERTED_TRIGGER}') THEN
                CREATE TRIGGER {REMOTE_CONTROL_RECORD_INSERTED_TRIGGER}
                AFTER INSERT ON {REMOTE_CONTROL_RECORD}
                FOR EACH ROW
                EXECUTE FUNCTION {UPDATE_CURRENT_MACHINE_CONTROL_FLAGS}();
            END IF;
        END $$;

        -- Trigger on before new machine remote control data inserted to deactive all the is_latest flags
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = '{REMOTE_CONTROL_RECORD_BEFORE_INSERTED_TRIGGER}') THEN
                CREATE TRIGGER {REMOTE_CONTROL_RECORD_BEFORE_INSERTED_TRIGGER}
                BEFORE INSERT ON {REMOTE_CONTROL_RECORD}
                FOR EACH ROW
                EXECUTE FUNCTION {TURN_OFF_IS_LATEST_FLAG}('{REMOTE_CONTROL_RECORD}');
            END IF;
        END $$;

        -- Trigger on new ros nodes error data inserted to create new self urgent stop data 
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = '{ROS_NODES_ERROR_RECORD_INSERTED_TRIGGER}') THEN
                CREATE TRIGGER {ROS_NODES_ERROR_RECORD_INSERTED_TRIGGER}
                AFTER INSERT ON {ROS_NODES_ERROR_RECORD}
                FOR EACH ROW
                EXECUTE FUNCTION {INSERT_SELF_URGENT_STOP_COMMANDS_RECORD}();
            END IF;
        END $$;

        -- Trigger on after the change in current panel selection 
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = '{CURRENT_PANEL_SELECTION_UPDATED_TRIGGER}') THEN
                CREATE TRIGGER {CURRENT_PANEL_SELECTION_UPDATED_TRIGGER}
                AFTER UPDATE ON {CURRENT_PANEL_SELECTION}
                FOR EACH ROW
                EXECUTE FUNCTION {INSERT_PANEL_SELECTIONS_RECORD}();
            END IF;
        END $$;

        -- Trigger on before new panel_selection_command data inserted to check and insert remote control
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = '{PANEL_SELECTIONS_RECORD_BEFORE_INSERTED_TRIGGER}') THEN
                CREATE TRIGGER {PANEL_SELECTIONS_RECORD_BEFORE_INSERTED_TRIGGER}
                BEFORE INSERT ON {PANEL_SELECTIONS_RECORD}
                FOR EACH ROW
                EXECUTE FUNCTION {CHECK_TO_INSERT_REMOTE_CONTROL_FALSE_RECORD}();
            END IF;
        END $$;

        -- Trigger on new panel selection data inserted to create new commands_record data 
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = '{PANEL_SELECTIONS_RECORD_INSERTED_TRIGGER}') THEN
                CREATE TRIGGER {PANEL_SELECTIONS_RECORD_INSERTED_TRIGGER}
                AFTER INSERT ON {PANEL_SELECTIONS_RECORD}
                FOR EACH ROW
                EXECUTE FUNCTION {INSERT_COMMANDS_RECORD}();
            END IF;
        END $$;

        -- Trigger on before new self_urgent_stop_command data inserted to check and insert remote control
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = '{SELF_URGENT_STOP_COMMANDS_RECORD_BEFORE_INSERTED_TRIGGER}') THEN
                CREATE TRIGGER {SELF_URGENT_STOP_COMMANDS_RECORD_BEFORE_INSERTED_TRIGGER}
                BEFORE INSERT ON {SELF_URGENT_STOP_COMMANDS_RECORD}
                FOR EACH ROW
                EXECUTE FUNCTION {CHECK_TO_INSERT_REMOTE_CONTROL_FALSE_RECORD}();
            END IF;
        END $$;

        -- Trigger on new self urgent_stop data inserted to create new commands_record data 
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = '{SELF_URGENT_STOP_COMMANDS_RECORD_INSERTED_TRIGGER}') THEN
                CREATE TRIGGER {SELF_URGENT_STOP_COMMANDS_RECORD_INSERTED_TRIGGER}
                AFTER INSERT ON {SELF_URGENT_STOP_COMMANDS_RECORD}
                FOR EACH ROW
                EXECUTE FUNCTION {INSERT_COMMANDS_RECORD}();
            END IF;
        END $$;

        -- Trigger on before new technician command data inserted to check and insert remote control
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = '{TECHNICIAN_COMMANDS_RECORD_BEFORE_INSERTED_TRIGGER}') THEN
                CREATE TRIGGER {TECHNICIAN_COMMANDS_RECORD_BEFORE_INSERTED_TRIGGER}
                BEFORE INSERT ON {TECHNICIAN_COMMANDS_RECORD}
                FOR EACH ROW
                EXECUTE FUNCTION {CHECK_TO_INSERT_REMOTE_CONTROL_FALSE_RECORD}();
            END IF;
        END $$;

        -- Trigger on new technician command data inserted to create new commands_record data 
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = '{TECHNICIAN_COMMANDS_RECORD_INSERTED_TRIGGER}') THEN
                CREATE TRIGGER {TECHNICIAN_COMMANDS_RECORD_INSERTED_TRIGGER}
                AFTER INSERT ON {TECHNICIAN_COMMANDS_RECORD}
                FOR EACH ROW
                EXECUTE FUNCTION {INSERT_COMMANDS_RECORD}();
            END IF;
        END $$;

        -- Trigger on before new call center command data inserted to check and insert remote control
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = '{CALL_CENTER_COMMANDS_RECORD_BEFORE_INSERTED_TRIGGER}') THEN
                CREATE TRIGGER {CALL_CENTER_COMMANDS_RECORD_BEFORE_INSERTED_TRIGGER}
                BEFORE INSERT ON {CALL_CENTER_COMMANDS_RECORD}
                FOR EACH ROW
                EXECUTE FUNCTION {CHECK_TO_INSERT_REMOTE_CONTROL_FALSE_RECORD}();
            END IF;
        END $$;

        -- Trigger on new call center command data inserted to create new commands_record data 
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = '{CALL_CENTER_COMMANDS_RECORD_INSERTED_TRIGGER}') THEN
                CREATE TRIGGER {CALL_CENTER_COMMANDS_RECORD_INSERTED_TRIGGER}
                AFTER INSERT ON {CALL_CENTER_COMMANDS_RECORD}
                FOR EACH ROW
                EXECUTE FUNCTION {INSERT_COMMANDS_RECORD}();
            END IF;
        END $$;

        -- Trigger on new command record data inserted to update current command data 
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = '{COMMANDS_RECORD_INSERTED_TRIGGER}') THEN
                CREATE TRIGGER {COMMANDS_RECORD_INSERTED_TRIGGER}
                AFTER INSERT ON {COMMANDS_RECORD}
                FOR EACH ROW
                EXECUTE FUNCTION {UPDATE_CURRENT_COMMAND}();
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
        self._setup_database()
        # self._machine_config = self._get_all_configs()

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
        for _procedure_name, _command in PROCEDURES_CREATE_SQL_COMMANDS_DICT.items():
            with _conn.cursor() as cur:
                cur.execute(_command)

    def _setup_triggers(self, _conn):
        with _conn.cursor() as cur:
            cur.execute(TRIGGERS_CREATE_SQL_COMMAND_STRING)

    def _set_default_tables_row(self, _conn):
        for _table_name in TABLES_WITH_DEFAULT_ROW:            
            with _conn.cursor() as cur:
                try:
                    cur.execute(
                        f"""
                            INSERT INTO {_table_name} (id) VALUES (1);
                        """
                    )
                except errors.UniqueViolation as e:
                    logging.warning(f"(f) _set_default_tables_row - table {_table_name} : {e}")
                    continue
        with _conn.cursor() as cur:
            try:
                cur.execute(
                    f"""
                        INSERT INTO {MACHINE_INFO} (id, machine_uid) VALUES (1, 'MACHINE001');
                    """
                )
            except errors.UniqueViolation as e:
                logging.warning(f"(f) _set_default_tables_row - table {_table_name} : {e}")

    def _insert_default_valid_types(self, _conn):
        with _conn.cursor() as cur:
            try:
                cur.execute(
                    f"""
                        INSERT INTO {VALID_SOURCE} (id, valid_value) VALUES 
                        (1,'local'), (2,'remote'), (3,'cloud'), (4,'self');
                    """
                )
            except errors.UniqueViolation as e:
                logging.warning(f"(f) _insert_default_valid_types - table {VALID_SOURCE} : {e}")
            try:
                cur.execute(
                    f"""
                        INSERT INTO {VALID_COMMAND_STATUS} (id,valid_value) VALUES 
                        (1,'none'), (2,'inprogress'), (3,'satisfied'), (4,'error');
                    """
                )
            except errors.UniqueViolation as e:
                logging.warning(f"(f) _insert_default_valid_types - table {VALID_COMMAND_STATUS} : {e}")
            try:
                cur.execute(
                    f"""
                        INSERT INTO {VALID_PANEL_SELECTION} (id,valid_value) VALUES 
                        (1,'link'), (2,'service'), (3,'off'), (4,'aa'), (5,'a'), (6,'b'), (7,'color');
                    """
                )
            except errors.UniqueViolation as e:
                logging.warning(f"(f) _insert_default_valid_types - table {VALID_PANEL_SELECTION} : {e}")
    
    def _setup_database(self):
        try:
            _db_connection = psycopg2.connect(self._conn_str)
            _db_connection.autocommit = True
            self._setup_tables(_db_connection)
            self._setup_procedures(_db_connection)
            self._setup_triggers(_db_connection)       
            # self._insert_default_valid_types(_db_connection)
            # self._set_default_tables_row(_db_connection)
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

    def _insert_machine_registration_data(self, _conn, _is_registered,_factory_id, _factory_name, _registered_by_id, registered_source_id):
        with _conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(
                f"""
                    INSERT INTO {MACHINE_REGISTRATION_RECORD} (
                        is_registered, registered_by_id, registered_source_id, factory_id, factory_name, machine_id
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING id;
                """, (_is_registered, _registered_by_id, registered_source_id, _factory_id, _factory_name, 1)
            )
            _new_id = cur.fetchone()["id"]
            logging.info(f"new row with id - {_new_id} insert to table {MACHINE_REGISTRATION_RECORD}")
            _conn.commit()

    def _insert_machine_disable_enable_data(self, _conn, _is_disable,_factory_id, _disabled_by_id, disabled_source_id):
        with _conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(
                f"""
                    INSERT INTO {MACHINE_DISABLE_ENABLE_RECORD} (
                        is_disabled, disabled_by_id, disabled_source_id, factory_id, machine_id
                    ) VALUES (%s, %s, %s, %s, %s)
                    RETURNING id;
                """, (_is_disable, _disabled_by_id, disabled_source_id, _factory_id, 1)
            )
            _new_id = cur.fetchone()["id"]
            logging.info(f"new row with id - {_new_id} insert to table {MACHINE_DISABLE_ENABLE_RECORD}")
            _conn.commit()

    def _insert_machine_remote_control_data(self, _conn, _is_remote, _requested_time_minute, _factory_id, _requested_by_id, _requested_source_id):
        with _conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(
                f"""
                    INSERT INTO {REMOTE_CONTROL_RECORD} (
                        is_remote, requested_time_minute, requested_by_id, requested_source_id, factory_id, machine_id
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING id;
                """, (_is_remote, _requested_time_minute, _requested_by_id, _requested_source_id, _factory_id, 1)
            )
            _new_id = cur.fetchone()["id"]
            logging.info(f"new row with id - {_new_id} insert to table {REMOTE_CONTROL_RECORD}")
            _conn.commit()

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
        
    def _command_generator(self, _conn, _command, _config, _source, _commander, _panel_selection):
        try:
            with _conn.cursor(cursor_factory=DictCursor) as cur:
                cur.execute(f"""
                    INSERT INTO {COMMANDS_RECORD} (command, machine_config, source, commander_id, panel_selection)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING id;
                """, (_command, _config, _source, _commander, _panel_selection))
                _new_id = cur.fetchone()["id"]
                logging.info(f"new row with id - {_new_id} insert to table {COMMANDS_RECORD}")
                _conn.commit()
        except Error as e:
            logging.error(f"(f) _command_generator - Error inserting data : {e}")
            _conn.rollback()

if __name__=="__main__":
    testing = Testing()
    _conn = psycopg2.connect(testing._conn_str)
    # testing._insert_machine_registration_data(_conn, True, "FACTORY001","Longan Factory 1", "user001", 2)
    # testing._insert_machine_registration_data(_conn, False, None,None, "user001", 2)

    # testing._insert_machine_disable_enable_data(_conn, True, "FACTORY001", "user001", 2)
    # testing._insert_machine_disable_enable_data(_conn, False, "FACTORY001", "user001", 2)

    # testing._insert_machine_remote_control_data(_conn, True, 5, "FACTORY001", "user001", 2)
    # testing._insert_machine_disable_enable_data(_conn, False, "FACTORY001", "user001", 2)
    _conn.close()
    
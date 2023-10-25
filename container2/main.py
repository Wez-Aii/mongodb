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
# POSTGRES_SERVICE_NAME = os.getenv("POSTGRES_SERVICE_NAME","localhost")
POSTGRES_DB_IP = "localhost"
MIN_SEQUENCE_COMMANDS_WAIT_TIME = os.getenv("MIN_SEQUENCE_COMMANDS_WAIT_TIME", 10)

LOGGING_LEVEL_DICT = {
    "CRITICAL": logging.CRITICAL,
    "ERROR": logging.ERROR,
    "WARNING": logging.WARNING,
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
}


# VALID_SOURCE = "valid_source"
# VALID_MODE = "valid_mode"
# VALID_COMMAND_STATUS = "valid_command_status"
# VALID_PANEL_SELECTION = "valid_panel_selection"
# VALID_MACHINE_DISPLAY_STATUS = "valid_machine_display_status"
# VALID_ALLOCATION_ACTION = "valid_allocation_action"

VALID_TXN_TYPE = "valid_txn_type"

# MACHINE_INFO = "machine_info"
# MACHINE_PROPERTIES_INFO = "machine_properties_info"
# TECHNICIAN_COMMANDS_RECORD = "technician_commands_record"
# CALL_CENTER_COMMANDS_RECORD = "call_center_commands_record"
# PANEL_SELECTIONS_RECORD = "panel_selections_record"
# SELF_URGENT_STOP_COMMANDS_RECORD = "self_urgent_stop_commands_record"
# COMMANDS_RECORD = "commands_record"
# ROS_NODES_CONFIGS = "ros_nodes_configs"
# COMMAND_MAP = "command_map"
# COMMAND_MAP_NODE_CONFIG_MAP = "command_map_node_config_map"
# MACHINE_DISABLE_ENABLE_RECORD = "machine_disable_enable_record"
# REMOTE_CONTROL_RECORD = "remote_control_record"
# MACHINE_REGISTRATION_RECORD = "machine_registration_record"
# MACHINE_CONTROL_FLAGS = "machine_control_flags"
# ROS_NODES_ERROR_RECORD = "ros_nodes_error_record"
# ROS_NODES_WARNING_RECORD = "ros_nodes_warning_record"
# CREDITS_ALLOCATION_RECORD = "credits_allocation_record"
# BATCHES_RECORD = "batches_record"
# LONGAN_LOT_INFO_RECORD = "longan_lot_info_record"

USER_ACCOUNT_DM = "user_account_dm"
FACTORY_INFO_DM = "factory_info_dm"
FACTORY_ACCOUNT_DM = "factory_account_dm"
MACHINE_INFO_DM = "machine_info_dm"
MACHINE_PROPERTIES_INFO_DM = "machine_properties_info_dm"
PROMO_RECORD_DM = "promo_record_dm"
BATCH_RECORD_DM = "batch_record_dm"
LONGAN_LOT_INFO_DM = "longan_lot_info_dm"
EJECT_TYPE_INFO_DM = "eject_type_info_dm"
TOKEN_EXCHANGE_RATE_DM = "token_exchange_rate_dm"
TOKEN_LONGAN_RATE_DM = "token_longan_rate_dm"
TOKEN_PURCHASE_TXN_DM = "token_purchase_txn_dm"
TOKEN_GIFT_TXN_DM = "token_gift_txn_dm"
TOKEN_ADJUSTMENT_TXN_DM = "token_adjustment_txn_dm"
TOKEN_LONGAN_LOT_USAGE_TXN_DM = "token_longan_lot_usage_txn_dm"
TOKEN_TXN_JOURNAL_DM = "token_txn_journal_dm"
TOKEN_BALANCE_DM = "token_balance_dm"
TOKEN_USAGE_DM = "token_usage_dm"
TOKEN_PURCHASE_DM = "token_purchase_dm"
TOKEN_GIFT_DM = "token_gift_dm"
TOKEN_ADJUSTMENT_DM = "token_adjustment_dm"


# CURRENT_FACTORY_INFO = "current_factory_info"
# CURRENT_MACHINE_CONTROL_FLAGS = "current_machine_control_flags"
# CURRENT_COMMAND = "current_command"
# CURRENT_PANEL_SELECTION = "current_panel_selection"
# CURRENT_CREDITS_BALANCE = "current_credits_balance"
# CURRENT_NODES_STATUS = "current_nodes_status"
# CURRENT_SORTER_DISPLAY = "current_sorter_display"

''' Store Procedures '''
INSERT_TO_CREATE_RELATED_ACCOUNTS_FOR_FACTORY = "insert_to_create_related_accounts_for_factory"
UPDATE_LATEST_TOKEN_EXCHANGE_RATE_END_DATE = "update_latest_token_exchange_rate_end_date"
UPDATE_LATEST_TOKEN_TO_LONGAN_RATE_END_DATE = "update_latest_token_to_longan_rate_end_date"
INSERT_TOKEN_LONGAN_LOT_USAGE_TXN = "insert_token_longan_lot_usage_txn"
INSERT_TOKEN_TXN_JOURNAL = "insert_token_txn_journal"
INSERT_OR_UPDATE_TOKEN_TXN_TYPES = "insert_or_update_token_txn_types"


# TURN_OFF_IS_LATEST_FLAG = "turn_off_is_latest_flag"
# TURN_OFF_IS_ACTIVE_FLAG = "turn_off_is_active_flag"
# UPDATE_CURRENT_FACTORY_INFO = "update_current_factory_info"
# UPDATE_CURRENT_MACHINE_CONTROL_FLAGS = "update_machine_control_flags"
# UPDATE_CURRENT_COMMAND = "generate_current_command"
# CHECK_TO_INSERT_REMOTE_CONTROL_FALSE_RECORD = "check_to_insert_remote_control_false_record"
# INSERT_ROS_NODES_ERROR_RECORD = "insert_ros_nodes_error_record"
# INSERT_PANEL_SELECTIONS_RECORD = "insert_panel_selection_record"
# INSERT_SELF_URGENT_STOP_COMMANDS_RECORD = "insert_self_urgent_stop_commands_record"
# INSERT_COMMANDS_RECORD = "insert_commands_record"
# CHECK_COMMAND_DURATION_TO_STOP = "check_command_duration_to_stop"
# GET_TOWERLIGHT_INDICATOR_FLAGS = "get_towerlight_indication_flags"

''' Triggers '''
FACTORY_INFO_INSERTED_TRIGGER = "factory_info_inserted_trigger"
TOKEN_EXCHANGE_RATE_BEFORE_INSERTED_TRIGGER = "token_exchange_rate_before_inserted_trigger"
TOKEN_TO_LONGAN_RATE_BEFORE_INSERTED_TRIGGER = "token_to_longan_rate_before_inserted_trigger"
LONGAN_LOT_INFO_INSERTED_TRIGGER = "longan_lot_info_inserted_trigger"
TOKEN_PURCHASE_TXN_INSERTED_TRIGGER = "token_purchase_txn_inserted_trigger"
TOKEN_GIFT_TXN_INSERTED_TRIGGER = "token_gift_txn_inserted_trigger"
TOKEN_ADJUSTMENT_TXN_INSERTED_TRIGGER = "token_adjustment_txn_inserted_trigger"
TOKEN_LONGAN_LOT_USGAE_TXN_INSERTED_TRIGGER = "token_longan_lot_usage_txn_inserted_trigger"
TOKEN_TXN_JOURNAL_INSERTED_TRIGGER = "token_txn_journal_inserted_trigger"


# REGISTRATION_RECORD_BEFORE_INSERTED_TRIGGER = "registration_record_before_insert_trigger"
# REGISTRATION_RECORD_INSERTED_TRIGGER_ONE = "registration_record_inserted_trigger_one"
# REGISTRATION_RECORD_INSERTED_TRIGGER_TWO = "registration_record_inserted_trigger_two"
# DISABLE_ENABLE_RECORD_BEFORE_INSERTED_TRIGGER = "disable_enable_record_before_insert_trigger"
# DISABLE_ENABLE_RECORD_INSERTED_TRIGGER_ONE = "disable_enable_record_inserted_trigger_one"
# DISABLE_ENABLE_RECORD_INSERTED_TRIGGER_TWO = "disable_enable_record_inserted_trigger_two"
# REMOTE_CONTROL_RECORD_BEFORE_INSERTED_TRIGGER = "remote_control_record_before_insert_trigger"
# REMOTE_CONTROL_RECORD_INSERTED_TRIGGER = "remote_control_record_inserted_trigger"
# ROS_NODES_ERROR_RECORD_BEFORE_INSERTED_TRIGGER = "ros_nodes_error_record_before_inserted_trigger"
# ROS_NODES_ERROR_RECORD_INSERTED_TRIGGER = "ros_nodes_error_record_inserted_trigger"
# TECHNICIAN_COMMANDS_RECORD_BEFORE_INSERTED_TRIGGER = "technician_commands_record_before_inserted_trigger"
# TECHNICIAN_COMMANDS_RECORD_INSERTED_TRIGGER = "technician_commands_record_inserted_trigger"
# CALL_CENTER_COMMANDS_RECORD_BEFORE_INSERTED_TRIGGER = "call_center_commands_record_before_inserted_trigger"
# CALL_CENTER_COMMANDS_RECORD_INSERTED_TRIGGER = "call_center_commands_record_inserted_trigger"
# PANEL_SELECTIONS_RECORD_BEFORE_INSERTED_TRIGGER = "panel_selection_record_before_inserted_trigger"
# PANEL_SELECTIONS_RECORD_INSERTED_TRIGGER = "panel_selection_record_inserted_trigger"
# SELF_URGENT_STOP_COMMANDS_RECORD_BEFORE_INSERTED_TRIGGER = "self_urgent_stop_commands_record_before_inserted_trigger"
# SELF_URGENT_STOP_COMMANDS_RECORD_INSERTED_TRIGGER = "self_urgent_stop_commands_record_inserted_trigger"
# CURRENT_PANEL_SELECTION_UPDATED_TRIGGER = "current_panel_selection_updated_trigger"
# COMMANDS_RECORD_INSERTED_TRIGGER = "commands_record_inserted_trigger"
# COMMAND_MAP_NODE_CONFIG_MAP_BEFORE_INSERT_TRIGGER = "command_map_node_config_map_before_insert_trigger"


DATABASE_TABLES = {
    VALID_TXN_TYPE : f"""
        CREATE TABLE {VALID_TXN_TYPE} (
            id SERIAL PRIMARY KEY,
            valid_value VARCHAR(56),
            timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
    """,
    USER_ACCOUNT_DM: f"""
        CREATE TABLE {USER_ACCOUNT_DM} (
            id SERIAL PRIMARY KEY,
            username VARCHAR(255) NOT NULL UNIQUE,
            user_role VARCHAR(16),
            account_registered_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
    """,
    FACTORY_INFO_DM : f"""
        CREATE TABLE {FACTORY_INFO_DM} (
            id SERIAL PRIMARY KEY,
            factory_name VARCHAR(255),
            factory_address VARCHAR(255),
            factory_location VARCHAR(255),
            created_by INTEGER REFERENCES {USER_ACCOUNT_DM}(id),
            created_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
    """,
    FACTORY_ACCOUNT_DM: f"""
        CREATE TABLE {FACTORY_ACCOUNT_DM} (
            id SERIAL PRIMARY KEY,
            factory_id INTEGER REFERENCES {FACTORY_INFO_DM}(id),
            account_name VARCHAR(255),
            timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
    """,
    MACHINE_INFO_DM : f"""
        CREATE TABLE {MACHINE_INFO_DM} (
            id SERIAL PRIMARY KEY,
            machine_uid VARCHAR(255) NOT NULL UNIQUE,
            alias VARCHAR(255),
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """,
    MACHINE_PROPERTIES_INFO_DM: f"""
        CREATE TABLE {MACHINE_PROPERTIES_INFO_DM} (
            id SERIAL PRIMARY KEY,
            machine_uid VARCHAR(255) NOT NULL UNIQUE,
            property_describtion TEXT,
            property_value VARCHAR(255),
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """,
    PROMO_RECORD_DM: f"""
        CREATE TABLE {PROMO_RECORD_DM} (
            id SERIAL PRIMARY KEY,
            update_by INTEGER REFERENCES {USER_ACCOUNT_DM}(id),
            approved_by INTEGER REFERENCES {USER_ACCOUNT_DM}(id),
            promo_name VARCHAR(56) NOT NULL UNIQUE,
            promo_description TEXT,
            promo_constraint_function_name VARCHAR(255),
            promo_available_for_group TEXT,
            available_promo_number INTEGER DEFAULT -1,
            promo_used_count INTEGER DEFAULT 0,
            promo_start_date TIMESTAMP NOT NULL,
            promo_end_date TIMESTAMP NOT NULL,
            timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
    """,
    BATCH_RECORD_DM: f"""
        CREATE TABLE {BATCH_RECORD_DM} (
            id SERIAL PRIMARY KEY,
            factory_id INTEGER REFERENCES {FACTORY_INFO_DM}(id),
            machine_id INTEGER REFERENCES {MACHINE_INFO_DM}(id),
            command_record_id VARCHAR(56),
            total_longan INTEGER NOT NULL,
            total_unejected_longan INTEGER NOT NULL,
            total_ejected_longan INTEGER NOT NULL,
            batch_end_note TEXT,
            batch_started_at TIMESTAMP NOT NULL,
            batch_ended_at TIMESTAMP DEFAULT NULL,
            timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
    """,
    LONGAN_LOT_INFO_DM: f"""
        CREATE TABLE {LONGAN_LOT_INFO_DM} (
            id SERIAL PRIMARY KEY,
            batch_id INTEGER REFERENCES {BATCH_RECORD_DM}(id),
            factory_id INTEGER REFERENCES {FACTORY_INFO_DM}(id),
            machine_id INTEGER REFERENCES {MACHINE_INFO_DM}(id),
            lot_total_longan INTEGER NOT NULL,
            lot_unejected_longan INTEGER NOT NULL,
            lot_ejected_longan INTEGER NOT NULL,
            is_processed BOOLEAN NOT NULL DEFAULT false,
            timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
    """,
    EJECT_TYPE_INFO_DM: f"""
        CREATE TABLE {EJECT_TYPE_INFO_DM} (
            id SERIAL PRIMARY KEY,
            batch_id INTEGER REFERENCES {BATCH_RECORD_DM}(id),
            lot_id INTEGER REFERENCES {LONGAN_LOT_INFO_DM}(id),
            eject_type VARCHAR(16),
            lot_ejected_longan_count INTEGER NOT NULL,
            lot_ejected_percentage DECIMAL(6,3),
            timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP            
        );
    """,
    TOKEN_EXCHANGE_RATE_DM: f"""
        CREATE TABLE {TOKEN_EXCHANGE_RATE_DM} (
            id SERIAL PRIMARY KEY,
            update_by INTEGER REFERENCES {USER_ACCOUNT_DM}(id),
            approved_by INTEGER REFERENCES {USER_ACCOUNT_DM}(id),
            token_to_bath_exchange_rate DECIMAL(7,2) NOT NULL,
            start_apply_date DATE NOT NULL DEFAULT CURRENT_DATE,
            end_date DATE DEFAULT NULL,
            is_active BOOLEAN NOT NULL DEFAULT false,
            timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
    """,
    TOKEN_LONGAN_RATE_DM: f"""
        CREATE TABLE {TOKEN_LONGAN_RATE_DM} (
            id SERIAL PRIMARY KEY,
            update_by INTEGER REFERENCES {USER_ACCOUNT_DM}(id),
            approved_by INTEGER REFERENCES {USER_ACCOUNT_DM}(id),
            token_to_longan_rate DECIMAL(6,3) NOT NULL,
            start_apply_date DATE NOT NULL DEFAULT CURRENT_DATE,
            end_date DATE DEFAULT NULL,
            is_active BOOLEAN NOT NULL DEFAULT false,
            timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
    """,
    TOKEN_PURCHASE_TXN_DM: f"""
        CREATE TABLE {TOKEN_PURCHASE_TXN_DM} (
            id SERIAL PRIMARY KEY,
            update_by INTEGER REFERENCES {USER_ACCOUNT_DM}(id),
            approved_by INTEGER REFERENCES {USER_ACCOUNT_DM}(id),
            factory_id INTEGER REFERENCES {FACTORY_INFO_DM}(id),
            txn_reference_number VARCHAR(255),
            total_token_amt DECIMAL(12,2) NOT NULL,
            purchase_token_amt DECIMAL(12,2) NOT NULL,
            bath_amt DECIMAL(12,2) NOT NULL,
            promo_token_amt DECIMAL(12,2) DEFAULT NULL,
            promo_name VARCHAR(56) DEFAULT NULL,
            is_processed BOOLEAN NOT NULL DEFAULT false,
            timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
    """,
    TOKEN_GIFT_TXN_DM: f"""
        CREATE TABLE {TOKEN_GIFT_TXN_DM} (
            id SERIAL PRIMARY KEY,
            update_by INTEGER REFERENCES {USER_ACCOUNT_DM}(id),
            approved_by INTEGER REFERENCES {USER_ACCOUNT_DM}(id),
            factory_id INTEGER REFERENCES {FACTORY_INFO_DM}(id),
            note TEXT,
            token_amt DECIMAL(12,2) NOT NULL,
            is_processed BOOLEAN NOT NULL DEFAULT false,
            timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
    """,
    TOKEN_ADJUSTMENT_TXN_DM: f"""
        CREATE TABLE {TOKEN_ADJUSTMENT_TXN_DM} (
            id SERIAL PRIMARY KEY,
            update_by INTEGER REFERENCES {USER_ACCOUNT_DM}(id),
            approved_by INTEGER REFERENCES {USER_ACCOUNT_DM}(id),
            factory_id INTEGER REFERENCES {FACTORY_INFO_DM}(id),
            note TEXT,
            token_amt DECIMAL(12,2) NOT NULL,
            adjustment_type VARCHAR(16) CHECK (adjustment_type IN ('add', 'subtract')),
            is_processed BOOLEAN NOT NULL DEFAULT false,
            timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
    """,
    TOKEN_LONGAN_LOT_USAGE_TXN_DM: f"""
        CREATE TABLE {TOKEN_LONGAN_LOT_USAGE_TXN_DM} (
            id SERIAL PRIMARY KEY,
            lot_id INTEGER REFERENCES {LONGAN_LOT_INFO_DM}(id),
            token_longan_rate_id INTEGER REFERENCES {TOKEN_LONGAN_RATE_DM}(id),
            factory_id INTEGER REFERENCES {FACTORY_INFO_DM}(id),
            machine_id INTEGER REFERENCES {MACHINE_INFO_DM}(id),
            longan_amt INTEGER NOT NULL,
            token_to_longan_rate DECIMAL(6,3) NOT NULL,
            token_usage_amt DECIMAL(10,3) NOT NULL,
            is_processed BOOLEAN NOT NULL DEFAULT false,
            timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
    """,
    TOKEN_TXN_JOURNAL_DM: f"""
        CREATE TABLE {TOKEN_TXN_JOURNAL_DM} (
            id SERIAL PRIMARY KEY,
            factory_id INTEGER REFERENCES {FACTORY_INFO_DM}(id),
            machine_id INTEGER REFERENCES {MACHINE_INFO_DM}(id),
            account_id INTEGER REFERENCES {FACTORY_ACCOUNT_DM}(id),
            txn_type_id INTEGER REFERENCES {VALID_TXN_TYPE}(id),
            txn_id VARCHAR(56) NOT NULL,
            txn_token_amt DECIMAL(10,3) NOT NULL,
            txn_entry_type VARCHAR(16) CHECK (txn_entry_type IN ('credit', 'debit')),
            is_processed BOOLEAN NOT NULL DEFAULT false,
            date DATE NOT NULL DEFAULT CURRENT_DATE,  
            timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
    """,
    TOKEN_BALANCE_DM: f"""
        CREATE TABLE {TOKEN_BALANCE_DM} (
            id SERIAL PRIMARY KEY,
            factory_id INTEGER REFERENCES {FACTORY_INFO_DM}(id),
            last_token_journal_id INTEGER REFERENCES {TOKEN_TXN_JOURNAL_DM}(id),    
            account_id INTEGER REFERENCES {FACTORY_ACCOUNT_DM}(id),
            token_balance_amt DECIMAL(10,3) NOT NULL,
            date DATE NOT NULL DEFAULT CURRENT_DATE,  
            timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
    """,
    TOKEN_USAGE_DM: f"""
        CREATE TABLE {TOKEN_USAGE_DM} (
            id SERIAL PRIMARY KEY,
            factory_id INTEGER REFERENCES {FACTORY_INFO_DM}(id),
            machine_id INTEGER REFERENCES {MACHINE_INFO_DM}(id),
            last_token_journal_id INTEGER REFERENCES {TOKEN_TXN_JOURNAL_DM}(id),    
            account_id INTEGER REFERENCES {FACTORY_ACCOUNT_DM}(id),
            token_usage_amt DECIMAL(10,3) NOT NULL,
            date DATE NOT NULL DEFAULT CURRENT_DATE,  
            timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
    """,
    TOKEN_PURCHASE_DM: f"""
        CREATE TABLE {TOKEN_PURCHASE_DM} (
            id SERIAL PRIMARY KEY,
            factory_id INTEGER REFERENCES {FACTORY_INFO_DM}(id),
            last_token_journal_id INTEGER REFERENCES {TOKEN_TXN_JOURNAL_DM}(id),    
            account_id INTEGER REFERENCES {FACTORY_ACCOUNT_DM}(id),
            token_purchase_amt DECIMAL(10,3) NOT NULL,
            date DATE NOT NULL DEFAULT CURRENT_DATE,  
            timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
    """,
    TOKEN_GIFT_DM: f"""
        CREATE TABLE {TOKEN_GIFT_DM} (
            id SERIAL PRIMARY KEY,
            factory_id INTEGER REFERENCES {FACTORY_INFO_DM}(id),
            last_token_journal_id INTEGER REFERENCES {TOKEN_TXN_JOURNAL_DM}(id),    
            account_id INTEGER REFERENCES {FACTORY_ACCOUNT_DM}(id),
            token_gift_amt DECIMAL(10,3) NOT NULL,
            date DATE NOT NULL DEFAULT CURRENT_DATE,  
            timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
    """,
    TOKEN_ADJUSTMENT_DM: f"""
        CREATE TABLE {TOKEN_ADJUSTMENT_DM} (
            id SERIAL PRIMARY KEY,
            factory_id INTEGER REFERENCES {FACTORY_INFO_DM}(id),
            last_token_journal_id INTEGER REFERENCES {TOKEN_TXN_JOURNAL_DM}(id),    
            account_id INTEGER REFERENCES {FACTORY_ACCOUNT_DM}(id),
            token_adjust_amt DECIMAL(10,3) NOT NULL,
            date DATE NOT NULL DEFAULT CURRENT_DATE,  
            timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
    """,
    # "CURRENT_SORTER_DISPLAY" : f"""
    #     CREATE TABLE {CURRENT_SORTER_DISPLAY} ();
    # """
}

PROCEDURES_CREATE_SQL_COMMANDS_DICT = {
    INSERT_TO_CREATE_RELATED_ACCOUNTS_FOR_FACTORY : f"""
        CREATE OR REPLACE FUNCTION {INSERT_TO_CREATE_RELATED_ACCOUNTS_FOR_FACTORY}()
        RETURNS TRIGGER AS $$
        BEGIN
            INSERT INTO {FACTORY_ACCOUNT_DM} (factory_id, account_name)
            VALUES 
                (NEW.id, 'wallet account'),
                (NEW.id, 'purchase account'),
                (NEW.id, 'usage account'),
                (NEW.id, 'gift account'),
                (NEW.id, 'adjustment account');
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    """,
    INSERT_TOKEN_LONGAN_LOT_USAGE_TXN : F"""
        CREATE OR REPLACE FUNCTION {INSERT_TOKEN_LONGAN_LOT_USAGE_TXN}()
        RETURNS TRIGGER AS $$
        DECLARE
            _token_longan_rate_id INT;
            _token_to_longan_rate DECIMAL;
            _token_usage_amt DECIMAL;
        BEGIN
            IF EXISTS (SELECT 1 FROM {TOKEN_LONGAN_RATE_DM} WHERE is_active = true and end_date >= CURRENT_DATE) THEN
                UPDATE {TOKEN_LONGAN_RATE_DM} SET is_active = false;
                UPDATE {TOKEN_LONGAN_RATE_DM} SET is_active = true WHERE id = (
                    SELECT id FROM {TOKEN_LONGAN_RATE_DM}
                    WHERE CURRENT_DATE >= start_apply_date AND (end_date IS NULL OR CURRENT_DATE < end_date)
                    ORDER BY timestamp DESC
                    LIMIT 1
                );
            ELSIF NOT EXISTS (SELECT 1 FROM {TOKEN_LONGAN_RATE_DM} WHERE is_active = true) THEN
                UPDATE {TOKEN_LONGAN_RATE_DM} SET is_active = true WHERE id = (
                    SELECT id FROM {TOKEN_LONGAN_RATE_DM}
                    WHERE CURRENT_DATE >= start_apply_date AND (end_date IS NULL OR CURRENT_DATE < end_date)
                    ORDER BY timestamp DESC
                    LIMIT 1
                );
            END IF;
            SELECT id, token_to_longan_rate INTO _token_longan_rate_id, _token_to_longan_rate 
            FROM {TOKEN_LONGAN_RATE_DM} WHERE is_active = true;
            IF _token_longan_rate_id IS NOT NULL AND _token_to_longan_rate IS NOT NULL THEN
                _token_usage_amt := _token_to_longan_rate * NEW.lot_total_longan;
                INSERT INTO {TOKEN_LONGAN_LOT_USAGE_TXN_DM} (
                    lot_id, token_longan_rate_id, 
                    factory_id, machine_id, longan_amt, 
                    token_to_longan_rate, token_usage_amt
                    ) VALUES (
                    NEW.id, _token_longan_rate_id, NEW.factory_id,
                    NEW.machine_id, NEW.lot_total_longan,
                    _token_to_longan_rate, _token_usage_amt
                    );
            ELSE
                -- if no token_longan_rate or token_longan_rate_id, 
                -- still keep record but not calculate the token usage and set to 0
                _token_usage_amt := 0.0;
                INSERT INTO {TOKEN_LONGAN_LOT_USAGE_TXN_DM} (
                    lot_id, token_longan_rate_id, 
                    factory_id, machine_id, longan_amt, 
                    token_to_longan_rate, token_usage_amt
                    ) VALUES (
                    NEW.id, _token_longan_rate_id, NEW.factory_id,
                    NEW.machine_id, NEW.lot_total_longan,
                    _token_to_longan_rate, _token_usage_amt
                    );
            END IF;
            UPDATE {LONGAN_LOT_INFO_DM} SET is_processed = true WHERE id = NEW.id;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    """,
    UPDATE_LATEST_TOKEN_EXCHANGE_RATE_END_DATE : f"""
        CREATE OR REPLACE FUNCTION {UPDATE_LATEST_TOKEN_EXCHANGE_RATE_END_DATE}()
        RETURNS TRIGGER AS $$
        BEGIN
            UPDATE {TOKEN_EXCHANGE_RATE_DM} SET end_date = NEW.start_apply_date
            WHERE id = (
                SELECT id FROM {TOKEN_EXCHANGE_RATE_DM} ORDER BY timestamp DESC LIMIT 1
            );
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    """,
    UPDATE_LATEST_TOKEN_TO_LONGAN_RATE_END_DATE : f"""
        CREATE OR REPLACE FUNCTION {UPDATE_LATEST_TOKEN_TO_LONGAN_RATE_END_DATE}()
        RETURNS TRIGGER AS $$
        BEGIN
            UPDATE {TOKEN_LONGAN_RATE_DM} SET end_date = NEW.start_apply_date
            WHERE id = (
                SELECT id FROM {TOKEN_LONGAN_RATE_DM} ORDER BY timestamp DESC LIMIT 1
            );
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    """,    
    INSERT_TOKEN_TXN_JOURNAL : f"""
        CREATE OR REPLACE FUNCTION {INSERT_TOKEN_TXN_JOURNAL}()
        RETURNS TRIGGER AS $$
        DECLARE
            _wallet_account_id INTEGER;
            _account_id INTEGER;
            _txn_type_id INTEGER;
            _first_txn_entry_type VARCHAR;
            _second_txn_entry_type VARCHAR;
            _txn_token_amt DECIMAL;
            _machine_id INTEGER;
        BEGIN
            SELECT id INTO _wallet_account_id FROM {FACTORY_ACCOUNT_DM}
            WHERE account_name = 'wallet account' AND factory_id = NEW.factory_id;
            -- check which txn trigger the function
            IF TG_NAME = '{TOKEN_PURCHASE_TXN_INSERTED_TRIGGER}' THEN
                -- txn_type_id 1 is purchase_type_txn
                _txn_type_id := 1;
                _txn_token_amt := NEW.total_token_amt;
                SELECT id INTO _account_id FROM {FACTORY_ACCOUNT_DM}
                WHERE account_name = 'purchase account' AND factory_id = NEW.factory_id;
                _first_txn_entry_type := 'credit';
                _second_txn_entry_type := 'debit';
                UPDATE {TOKEN_PURCHASE_TXN_DM} SET is_processed = true;
            ELSIF TG_NAME = '{TOKEN_LONGAN_LOT_USGAE_TXN_INSERTED_TRIGGER}' THEN
                -- txn_type_id 2 is usage_type_txn
                _txn_type_id := 2;
                _txn_token_amt := NEW.token_usage_amt;
                _machine_id := NEW.machine_id;
                SELECT id INTO _account_id FROM {FACTORY_ACCOUNT_DM}
                WHERE account_name = 'usage account' AND factory_id = NEW.factory_id;
                _first_txn_entry_type := 'debit';
                _second_txn_entry_type := 'credit';
                UPDATE {TOKEN_LONGAN_LOT_USAGE_TXN_DM} SET is_processed = true;
            ELSEIF TG_NAME = '{TOKEN_GIFT_TXN_INSERTED_TRIGGER}' THEN
                -- txn_type_id 3 is gift_type_txn
                _txn_type_id := 3;
                _txn_token_amt := NEW.token_amt;
                SELECT id INTO _account_id FROM {FACTORY_ACCOUNT_DM}
                WHERE account_name = 'gift account' AND factory_id = NEW.factory_id;
                _first_txn_entry_type := 'credit';
                _second_txn_entry_type := 'debit';
                UPDATE {TOKEN_GIFT_TXN_DM} SET is_processed = true;
            ELSIF TG_NAME = '{TOKEN_ADJUSTMENT_TXN_INSERTED_TRIGGER}' THEN
                -- txn_type_id 4 is adjustment_type_txn
                _txn_type_id := 4;
                _txn_token_amt := NEW.token_amt;
                SELECT id INTO _account_id FROM {FACTORY_ACCOUNT_DM}
                WHERE account_name = 'adjustment account' AND factory_id = NEW.factory_id;
                IF NEW.adjustment_type = 'add' THEN
                    _first_txn_entry_type := 'credit';
                    _second_txn_entry_type := 'debit';
                ELSE
                    _first_txn_entry_type := 'debit';
                    _second_txn_entry_type := 'credit';
                UPDATE {TOKEN_ADJUSTMENT_TXN_DM} SET is_processed = true;
                END IF;
            END IF;
            IF _account_id IS NOT NULL THEN
                INSERT INTO {TOKEN_TXN_JOURNAL_DM}(
                factory_id, machine_id, account_id, txn_type_id, 
                txn_id, txn_token_amt, txn_entry_type
                ) VALUES 
                (
                    NEW.factory_id, _machine_id, _account_id, _txn_type_id,
                    NEW.id, _txn_token_amt, _first_txn_entry_type
                ),
                (
                    NEW.factory_id, _machine_id, _wallet_account_id, _txn_type_id,
                    NEW.id, _txn_token_amt, _second_txn_entry_type
                );
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    """,
    INSERT_OR_UPDATE_TOKEN_TXN_TYPES : f"""
        CREATE OR REPLACE FUNCTION {INSERT_OR_UPDATE_TOKEN_TXN_TYPES}()
        RETURNS TRIGGER AS $$
        DECLARE
            _account_name VARCHAR;
            _latest_token_balance_amt DECIMAL;
        BEGIN
            SELECT account_name INTO _account_name FROM {FACTORY_ACCOUNT_DM} 
            WHERE id = NEW.account_id;
            IF _account_name = 'wallet account' THEN
                IF NEW.txn_entry_type = 'credit' THEN
                    NEW.txn_token_amt := -NEW.txn_token_amt;
                END IF;
                IF EXISTS (SELECT 1  FROM {TOKEN_BALANCE_DM} WHERE date = CURRENT_DATE AND factory_id = NEW.factory_id) THEN
                    UPDATE {TOKEN_BALANCE_DM} 
                    SET last_token_journal_id = NEW.id, 
                    token_balance_amt = token_balance_amt + NEW.txn_token_amt, 
                    timestamp = CURRENT_TIMESTAMP 
                    WHERE date = CURRENT_DATE AND factory_id = NEW.factory_id;
                ELSE
                    SELECT token_balance_amt INTO _latest_token_balance_amt FROM {TOKEN_BALANCE_DM}
                    WHERE factory_id = NEW.factory_id AND account_id = NEW.account_id ORDER BY timestamp DESC LIMIT 1;
                    IF _latest_token_balance_amt IS NULL THEN
                        _latest_token_balance_amt := 0;
                    END IF;
                    INSERT INTO {TOKEN_BALANCE_DM}(
                        factory_id, last_token_journal_id, account_id, token_balance_amt
                        ) VALUES (
                            NEW.factory_id, NEW.id, NEW.account_id,_latest_token_balance_amt + NEW.txn_token_amt 
                            );
                END IF;
            ELSIF _account_name = 'purchase account' THEN
                IF EXISTS (SELECT 1  FROM {TOKEN_PURCHASE_DM} WHERE date = CURRENT_DATE AND factory_id = NEW.factory_id) THEN
                    UPDATE {TOKEN_PURCHASE_DM} 
                    SET last_token_journal_id = NEW.id, 
                    token_purchase_amt = token_purchase_amt + NEW.txn_token_amt, 
                    timestamp = CURRENT_TIMESTAMP 
                    WHERE date = CURRENT_DATE AND factory_id = NEW.factory_id;
                ELSE
                    INSERT INTO {TOKEN_PURCHASE_DM}(
                        factory_id, last_token_journal_id, account_id, token_purchase_amt
                        ) VALUES (
                            NEW.factory_id, NEW.id, NEW.account_id, NEW.txn_token_amt 
                            );
                END IF;
            ELSIF _account_name = 'usage account' THEN
                IF EXISTS (SELECT 1  FROM {TOKEN_USAGE_DM} WHERE date = CURRENT_DATE AND factory_id = NEW.factory_id AND machine_id = NEW.machine_id) THEN
                    UPDATE {TOKEN_USAGE_DM} 
                    SET last_token_journal_id = NEW.id, 
                    token_usage_amt = token_usage_amt + NEW.txn_token_amt, 
                    timestamp = CURRENT_TIMESTAMP 
                    WHERE date = CURRENT_DATE AND factory_id = NEW.factory_id;
                ELSE
                    INSERT INTO {TOKEN_USAGE_DM}(
                        factory_id, machine_id, last_token_journal_id, account_id, token_usage_amt
                        ) VALUES (
                            NEW.factory_id, NEW.machine_id, NEW.id, NEW.account_id, NEW.txn_token_amt 
                            );
                END IF;            
            ELSIF _account_name = 'gift account' THEN
                IF EXISTS (SELECT 1  FROM {TOKEN_GIFT_DM} WHERE date = CURRENT_DATE AND factory_id = NEW.factory_id) THEN
                    UPDATE {TOKEN_GIFT_DM} 
                    SET last_token_journal_id = NEW.id, 
                    token_gift_amt = token_gift_amt + NEW.txn_token_amt, 
                    timestamp = CURRENT_TIMESTAMP 
                    WHERE date = CURRENT_DATE AND factory_id = NEW.factory_id;
                ELSE
                    INSERT INTO {TOKEN_GIFT_DM}(
                        factory_id, last_token_journal_id, account_id, token_gift_amt
                        ) VALUES (
                            NEW.factory_id, NEW.id, NEW.account_id, NEW.txn_token_amt 
                            );
                END IF;            
            ELSIF _account_name = 'adjustment account' THEN
                IF NEW.txn_entry_type = 'debit' THEN
                    NEW.txn_token_amt := -NEW.txn_token_amt;
                END IF;
                IF EXISTS (SELECT 1  FROM {TOKEN_ADJUSTMENT_DM} WHERE date = CURRENT_DATE AND factory_id = NEW.factory_id) THEN
                    UPDATE {TOKEN_ADJUSTMENT_DM} 
                    SET last_token_journal_id = NEW.id, 
                    token_adjust_amt = token_adjust_amt + NEW.txn_token_amt, 
                    timestamp = CURRENT_TIMESTAMP 
                    WHERE date = CURRENT_DATE AND factory_id = NEW.factory_id;
                ELSE
                    INSERT INTO {TOKEN_ADJUSTMENT_DM}(
                        factory_id, last_token_journal_id, account_id, token_adjust_amt
                        ) VALUES (
                            NEW.factory_id, NEW.id, NEW.account_id, NEW.txn_token_amt 
                            );
                END IF;
            END IF;
            UPDATE {TOKEN_TXN_JOURNAL_DM} SET is_processed = true;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    """,    
}

TRIGGERS_CREATE_SQL_COMMAND_STRING = f"""
        -- Trigger on new factory info data inserted to create related factory accounts
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = '{FACTORY_INFO_INSERTED_TRIGGER}') THEN
                CREATE TRIGGER {FACTORY_INFO_INSERTED_TRIGGER}
                AFTER INSERT ON {FACTORY_INFO_DM}
                FOR EACH ROW
                EXECUTE FUNCTION {INSERT_TO_CREATE_RELATED_ACCOUNTS_FOR_FACTORY}();
            END IF;
        END $$;

        -- Trigger on new token exchange rate data before inserted to update token exchange rate end date
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = '{TOKEN_EXCHANGE_RATE_BEFORE_INSERTED_TRIGGER}') THEN
                CREATE TRIGGER {TOKEN_EXCHANGE_RATE_BEFORE_INSERTED_TRIGGER}
                BEFORE INSERT ON {TOKEN_EXCHANGE_RATE_DM}
                FOR EACH ROW
                EXECUTE FUNCTION {UPDATE_LATEST_TOKEN_EXCHANGE_RATE_END_DATE}();
            END IF;
        END $$;

        -- Trigger on new token longan rate data before inserted to update token longan rate end date
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = '{TOKEN_TO_LONGAN_RATE_BEFORE_INSERTED_TRIGGER}') THEN
                CREATE TRIGGER {TOKEN_TO_LONGAN_RATE_BEFORE_INSERTED_TRIGGER}
                BEFORE INSERT ON {TOKEN_LONGAN_RATE_DM}
                FOR EACH ROW
                EXECUTE FUNCTION {UPDATE_LATEST_TOKEN_TO_LONGAN_RATE_END_DATE}();
            END IF;
        END $$;

        -- Trigger on new longan lot info data inserted to create token longan lot usage 
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = '{LONGAN_LOT_INFO_INSERTED_TRIGGER}') THEN
                CREATE TRIGGER {LONGAN_LOT_INFO_INSERTED_TRIGGER}
                AFTER INSERT ON {LONGAN_LOT_INFO_DM}
                FOR EACH ROW
                EXECUTE FUNCTION {INSERT_TOKEN_LONGAN_LOT_USAGE_TXN}();
            END IF;
        END $$;

        -- Trigger on new token purchase txn data inserted to create token txn journal
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = '{TOKEN_PURCHASE_TXN_INSERTED_TRIGGER}') THEN
                CREATE TRIGGER {TOKEN_PURCHASE_TXN_INSERTED_TRIGGER}
                AFTER INSERT ON {TOKEN_PURCHASE_TXN_DM}
                FOR EACH ROW
                EXECUTE FUNCTION {INSERT_TOKEN_TXN_JOURNAL}();
            END IF;
        END $$;

        -- Trigger on new token gift txn data inserted to create token txn journal
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = '{TOKEN_GIFT_TXN_INSERTED_TRIGGER}') THEN
                CREATE TRIGGER {TOKEN_GIFT_TXN_INSERTED_TRIGGER}
                AFTER INSERT ON {TOKEN_GIFT_TXN_DM}
                FOR EACH ROW
                EXECUTE FUNCTION {INSERT_TOKEN_TXN_JOURNAL}();
            END IF;
        END $$;

        -- Trigger on new token adjustment txn data inserted to create token txn journal
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = '{TOKEN_ADJUSTMENT_TXN_INSERTED_TRIGGER}') THEN
                CREATE TRIGGER {TOKEN_ADJUSTMENT_TXN_INSERTED_TRIGGER}
                AFTER INSERT ON {TOKEN_ADJUSTMENT_TXN_DM}
                FOR EACH ROW
                EXECUTE FUNCTION {INSERT_TOKEN_TXN_JOURNAL}();
            END IF;
        END $$;

        -- Trigger on new token longan lot usage txn data inserted to create token txn journal
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = '{TOKEN_LONGAN_LOT_USGAE_TXN_INSERTED_TRIGGER}') THEN
                CREATE TRIGGER {TOKEN_LONGAN_LOT_USGAE_TXN_INSERTED_TRIGGER}
                AFTER INSERT ON {TOKEN_LONGAN_LOT_USAGE_TXN_DM}
                FOR EACH ROW
                EXECUTE FUNCTION {INSERT_TOKEN_TXN_JOURNAL}();
            END IF;
        END $$;
        
        -- Trigger on new token txn journal data inserted to create token txn types record
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = '{TOKEN_TXN_JOURNAL_INSERTED_TRIGGER}') THEN
                CREATE TRIGGER {TOKEN_TXN_JOURNAL_INSERTED_TRIGGER}
                AFTER INSERT ON {TOKEN_TXN_JOURNAL_DM}
                FOR EACH ROW
                EXECUTE FUNCTION {INSERT_OR_UPDATE_TOKEN_TXN_TYPES}();
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
        self._conn_str = self._get_valid_connection_str('aii_sortermachine', 'postgres', 'entersecretpassword', POSTGRES_DB_IP)
        self._setup_database()
        
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

    def _setup_database(self):
        try:
            _db_connection = psycopg2.connect(self._conn_str)
            _db_connection.autocommit = True
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

    def _fetchone_from_current_type_table(self, _conn, _table_name):
        try:
            with _conn.cursor(cursor_factory=DictCursor) as cur:
                cur.execute(f"SELECT * FROM {_table_name};")
                row = cur.fetchone()
            return row
        except Error as e:
            logging.error(f"(f) _fetchone_from_current_data - an error occure : {e}")
    

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
    
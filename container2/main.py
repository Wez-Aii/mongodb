import logging
import json
import threading
import os
import time
import pytz
import psycopg2

from psycopg2 import OperationalError, Error
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
        self._conn_str = f"dbname='testing' user='postgres' password='entersecretpassword' host='{POSTGRES_SERVICE_NAME}'"
        self._command_start_time = None
        self._worker_status = 0

    def _fetchone_from_current_type_table(self, _conn, _table_name):
        try:
            with _conn.cursor(cursor_factory=DictCursor) as cur:
                cur.execute(f"SELECT * FROM {_table_name};")
                row = cur.fetchone()
            return row
        except Error as e:
            logging.error(f"(f) _fetchone_from_current_data - an error occure : {e}")
             
    def _check_current_command(self, _conn):
        ''' First pull the current_command data and check it is not None
        Only if the data is not None and command_status is in [none, inprogress] 
        then Check if the command and current nodes status match
        if not match keep sending the command paylaod 
        and let command_status be inprogress or error depend on nodes error status
        else do not have to send command payload and use the command_status complete'''
        _get_data = self._fetchone_from_current_type_table(_conn, CURRENT_COMMAND)
        # check command_status and excute accordingly
        if _get_data is not None and _get_data["command_status"] in ["none", "inprogress"]:
            if _get_data["command_status"] == "none":
                self._command_start_time = _get_data["timestamp"]
                logging.debug(f"(f) _check_current_command - _command_start_time : {self._command_start_time}")
            else:
                self._command_start_time = _get_data["timestamp"] if self._command_start_time is None else self._command_start_time
            try:
                _command_id = _get_data["command_id"]
                with _conn.cursor(cursor_factory=DictCursor) as cur:
                    cur.execute(
                        f"""
                        SELECT * FROM {COMMANDS_RECORD} WHERE command_id = %s;
                        """ %_command_id
                    )
                    _command_payload = cur.fetchone()    
                    _command_action_required = self._compare_command_target_nodes_status(_command_payload)            
                    if _command_action_required == "inprogress":
                        self._command_payload_handler(_command_payload)
                        cur.execute(
                            f"""
                            UPDATE {CURRENT_COMMAND}
                            SET command_status = %s, timestamp = NOW()
                            WHERE id = %s;
                            """, ("inprogress", _get_data["id"])
                        )
                    else:
                        cur.execute(
                            f"""
                            UPDATE {CURRENT_COMMAND}
                            SET command_status = %s, timestamp = NOW()
                            WHERE id = %s;
                            """, (_command_action_required, _get_data["id"])
                        )
                    _conn.commit()
            except Error as e:
                logging.error(f"(f) _check_current_command - error occured : {e}")
                _conn.rollback()
                pass
            except Exception as e:
                logging.error(f"(f) _check_current_command - error occured : {e}")
                pass
        

    def _command_payload_handler(self, _payload:dict):
        logging.debug(f"(f)_command_payload_handler - get payload \n {_payload}")
        _current_datatime = datetime.now()
        _time_difference = (_current_datatime -_payload["timestamp"]).total_seconds()
        logging.debug(f"(f)_compare_command_target_nodes_status - time differenc now : {_time_difference}")
        if _time_difference > 100:
            # when timeout will not process and excute the payload, instead raise error 
            self._worker_status = -3
        else:
            if self._worker_status != -3:
                # excute the command according below is the example
                if (_payload["command"] == "ALL_START") and self._worker_status <= 5:
                    self._worker_status += 1
                elif (_payload["command"] == "ALL_STOP") and (self._worker_status >= 0):
                    self._worker_status -= 1
            else:
                # wait status to be ok - dummpy process example
                self._worker_status += 1
        pass

    def _compare_command_target_nodes_status(self, _payload:dict):
        logging.debug(f"(f) _compare_command_target_nodes_status - get payload : {_payload}")
        if (0 < self._worker_status < 5):
                return "inprogress"
        elif self._worker_status == 0:
            return "satisfied" if _payload["command"] == "ALL_STOP" else "inprogress"
        elif self._worker_status == 5:
            return "satisfied" if _payload["command"] == "ALL_START" else "inprogress"
        else:
            return "error"

    def _record_latest_panel_selection(self, _conn, _panel_selection):
        try:
            with _conn.cursor() as cur:
                cur.execute(f"""
                    INSERT INTO {CURRENT_PANEL_SELECTION} (id, panel_selection)
                    VALUES (%s, %s)
                    ON CONFLICT (id)
                    DO UPDATE SET panel_selection = %s, is_processed = false, timestamp = NOW();
                """, (1, _panel_selection, _panel_selection))
                logging.info("(f) _record_latest_panel_selection - Successfully upserted data")
                _conn.commit()
        except Error as e:
            logging.error(f"(f) _record_latest_panel_selection - Error upserting data: {e}")
            _conn.rollback()

    def _record_current_machine_control_statuses(self, _is_unregister=True, _is_disabled=True, _is_remote=False, is_service=False):
        try:
            with _conn.cursor() as cur:
                cur.execute(f"""
                    INSERT INTO {CURRENT_MACHINE_CONTROL_STATUSES} (id, is_unregister, is_disabled, is_remote, is_service)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (id)
                    DO UPDATE SET is_unregister = %s, is_disabled = %s, is_remote = %s, is_service = %s, timestamp = NOW();
                """, (1, _is_unregister, _is_disabled, _is_remote, is_service, _is_unregister, _is_disabled, _is_remote, is_service,))
                logging.info("(f) _record_current_machine_control_statuses - Successfully upserted data")
                _conn.commit()
        except Error as e:
            logging.error(f"(f) _record_current_machine_control_statuses - Error upserting data: {e}")
            _conn.rollback()

if __name__=="__main__":
    try:
        testing = Testing()
        _conn = psycopg2.connect(testing._conn_str)
        _conn.autocommit = True
        _current_panel_selection = "off"
        testing._record_current_machine_control_statuses(False, False, False, False)
        testing._record_latest_panel_selection(_conn, "aa")
        for a in range(10):
            testing._check_current_command(_conn)
            # current_panel_selection = testing._check_panel_selection(_conn, _current_panel_selection)
            time.sleep(2)
        pass
    except Error as e:
        logging.error("ERRORRRRRR")
        pass
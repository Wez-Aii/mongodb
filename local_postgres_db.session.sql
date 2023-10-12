INSERT INTO machine_info (id, machine_uid) VALUES (1, 'AI-M-a-001');

INSERT INTO valid_source (id,valid_value) VALUES 
(1,'local'),
(2,'remote'),
(3,'cloud'),
(4,'self');

INSERT INTO valid_mode (id,valid_value) VALUES
(1,'setup'),
(2,'oper'),
(3,'node_diag'),
(4,'sys_diag'),
(5,'op_diag');

INSERT INTO valid_command_status (id,valid_value) VALUES
(1,'none'),
(2,'inprogress'),
(3,'satisfied'),
(4,'error');

INSERT INTO valid_panel_selection (id, valid_value) VALUES
(1, 'off'),
(2, 'b'),
(3, 'a'),
(4, 'aa'),
(5, 'service'),
(6, 'link'),
(7, 'color');

INSERT INTO ros_nodes_configs (id, node_type, config) VALUES 
(1, 'vision', '{"default": 7}'),
(2, 'ejector', '{"default": 1}'),
(3, 'vision', '{"b":15}'),
(4, 'vision', '{"a":15}'),
(5, 'vision', '{"aa":15}'),
(6, 'ejector', '{"b":15}'),
(7, 'ejector', '{"a":15}'),
(8, 'ejector', '{"aa":15}'),
(9, 'conveyor', '{"speed":15}');

INSERT INTO command_map (id, command_str, ros_command_str, eq_panel_selection_id, mode_id, timeout_sec) VALUES
(1, 'cloud_b', 'ALL_START', 2, 2, -1),
(2, 'cloud_a', 'ALL_START', 3, 2, -1),
(3, 'cloud_aa', 'ALL_START', 4, 2, -1),
(4, 'cloud_color', 'ALL_START', 7, 2, -1),
(5, 'node_diag_start_all', 'ALL_START', 5, 3, 60),
(6, 'sys_diag_start_all', 'ALL_START', 5, 4, 60),
(7, 'op_diag_start_all', 'ALL_START', 5, 5, 60),
(8, 'cloud_off', 'ALL_STOP', 1, 2, -1),
(9, 'node_diag_stop_all', 'ALL_STOP', 5, 3, -1),
(10, 'sys_diag_stop_all', 'ALL_STOP', 5, 4, -1),
(11, 'op_diag_stop_all', 'ALL_STOP', 5, 5, -1),
(12, 'self_stop', 'ALL_STOP', 1, 2, -1),
(13, '1', 'ALL_STOP', 1, 2, -1),
(14, '2', 'ALL_START', 2, 2, -1),
(15, '3', 'ALL_START', 3, 2, -1),
(16, '4', 'ALL_START', 4, 2, -1),
(17, '5', 'ALL_STOP', 5, 1, -1),
(18, '6', 'ALL_STOP', 6, 1, -1),
(19, '7', 'ALL_START', 7, 2, -1);

INSERT INTO command_map_node_config_map (command_map_id, ros_node_config_id) VALUES
(14, 3), (14, 6), (14, 9),
(15, 4), (15, 7), (15, 9),
(16, 5), (16, 8), (16, 9),
(18, 1), (16, 2), (16, 9);

INSERT INTO current_factory_info (id) VALUES (1);

INSERT INTO current_command (id) VALUES (1);

INSERT INTO current_panel_selection (id) VALUES (1);

INSERT INTO current_machine_control_flags (id) VALUES (1);



INSERT INTO machine_registration_record (is_registered, registered_by_id, registered_source_id, factory_id, factory_name, machine_id)
VALUES (true, '123', 3, '101', 'longan factory 101', 1);

INSERT INTO machine_disable_enable_record (is_disabled, disabled_by_id, disabled_source_id, factory_id, machine_id)
VALUES (false, '123', 3, '101', 1);

INSERT INTO remote_control_record (is_remote, requested_time_minute, requested_by_id, requested_source_id, factory_id, machine_id)
VALUES (true, 30, '11', 3, '101', 1);

INSERT INTO technician_commands_record (command_str, remote_id, technician_id)
VALUES ('sys_diag_start_all', 6, '123');

INSERT INTO call_center_commands_record (command_str, remote_id, agent_id)
VALUES ('cloud_b', 17, '101');

INSERT INTO ros_nodes_error_record (node_type, node_name, error_msg)
VALUES ('controller', 'controller', 'testing_error');

SELECT * FROM ros_nodes_error_record ORDER BY error_start_time DESC LIMIT 10;

SELECT * FROM valid_source;

SELECT * FROM valid_panel_selection;

SELECT * FROM machine_registration_record;

SELECT * FROM machine_disable_enable_record;

SELECT * FROM remote_control_record;

SELECT * FROM current_command;

SELECT * FROM current_machine_control_flags;

SELECT * FROM current_panel_selection;

SELECT * FROM current_factory_info;

SELECT * FROM panel_selections_record ORDER BY timestamp DESC LIMIT 10;

SELECT * FROM technician_commands_record;

SELECT * FROM call_center_commands_record ORDER BY timestamp DESC limit 10;

SELECT * FROM self_urgent_stop_commands_record;

SELECT * FROM commands_record ORDER BY timestamp DESC LIMIT 10;

SELECT * FROM command_map;

SELECT * FROM modes_record;

SELECT * FROM self_urgent_stop_commands_record ORDER BY timestamp DESC LIMIT 10;

SELECT * FROM ros_nodes_configs;


DO
$$
BEGIN
    
    IF NOT EXISTS (
        SELECT 1 FROM current_command 
        WHERE EXTRACT(EPOCH FROM age(current_timestamp, timestamp)) < 60
    ) THEN
        UPDATE current_command SET command_status_id = 3, consecutive_failed_command_count = 0;
    END IF;
END;
$$;

SELECT rc.is_remote, mr.is_registered, mde.is_disabled FROM current_machine_control_flags as cmc
JOIN remote_control_record AS rc ON cmc.remote_id = rc.id
JOIN machine_registration_record AS mr ON cmc.registration_id = mr.id
JOIN machine_disable_enable_record AS mde ON cmc.disable_enable_id = mde.id;

SELECT cmc.remote_id, rc.is_remote, mr.is_registered FROM current_machine_control_flags as cmc
JOIN remote_control_record AS rc ON cmc.remote_id = rc.id
JOIN machine_registration_record AS mr ON cmc.registration_id = mr.id;

UPDATE current_panel_selection SET valid_panel_selection_id = 1;


SELECT cm.eq_panel_selection_id FROM commands_record AS cr 
JOIN command_map AS cm ON cr.command_map_id = cm.id
WHERE cr.call_center_command_id IS NOT NULL
AND age(NOW(), cr.timestamp) < INTERVAL '1 minute';

DROP SCHEMA public CASCADE;

DROP TABLE IF EXISTS commands_record CASCADE;

SELECT vps.valid_value, vm.valid_value FROM commands_record AS cr
JOIN command_map AS cm ON cr.command_map_id = cm.id
JOIN valid_mode AS vm ON cm.mode_id = vm.id
JOIN valid_panel_selection AS vps ON cm.eq_panel_selection_id = vps.id
WHERE cr.id = 7;

SELECT cmc.remote_id, rc.is_remote, mr.is_registered, mde.is_disabled
FROM current_machine_control_flags as cmc
LEFT JOIN remote_control_record AS rc ON cmc.remote_id = rc.id
LEFT JOIN machine_registration_record AS mr ON cmc.registration_id = mr.id
LEFT JOIN machine_disable_enable_record AS mde ON cmc.disable_enable_id = mde.id;

SELECT cmp.ros_command_str, vps.valid_value AS panel_selection, 
vm.valid_value AS mode, cmp.timeout_sec, cr.command_config, 
vcs.valid_value AS command_status, ccmd.is_processed
FROM current_command AS ccmd
LEFT JOIN commands_record AS cr ON cr.id = ccmd.command_record_id
LEFT JOIN command_map AS cmp ON cmp.id = cr.command_map_id
LEFT JOIN valid_panel_selection AS vps ON vps.id = cmp.eq_panel_selection_id
LEFT JOIN valid_mode AS vm ON vm.id = cmp.mode_id
LEFT JOIN valid_command_status AS vcs ON vcs.id = ccmd.command_status_id;

SELECT rnc.node_type, rnc.config FROM mode_start_config AS msc
LEFT JOIN ros_nodes_configs AS rnc ON rnc.id = msc.vision_node_config_id OR rnc.id = msc.ejector_node_config_id OR rnc.id = msc.conveyor_node_config_id
WHERE msc.id = 4;

SELECT * FROM mode_start_config WHERE id = 4;

INSERT INTO valid_txn_type (id,valid_value) VALUES 
(1,'purchase'),
(2,'usage'),
(3,'gift'),
(4,'adjustment');

INSERT INTO user_account_dm (id, username, user_role) VALUES
(1,'fnamelname@01', 'admin'),
(2,'fnamelname@99', 'agent'),
(3,'fnamelname@85', 'technician'),
(4,'fnamelname@87', 'client'),
(5,'fnamelname@88', 'client');

INSERT INTO factory_info_dm (id, factory_name, created_by) VALUES
(1, 'LYC Longan Factory', 2),
(2, 'Lampung 168 Longan', 2);

INSERT INTO machine_info_dm (id, machine_uid, alias) VALUES
(1, 'MACHINE2023001', 'LYC machine 1'),
(2, 'MACHINE2023002', 'LYC machine 2'),
(3, 'MACHINE2023003', '168 machine 1'),
(4, 'MACHINE2023004', '168 machine 1');

INSERT INTO batch_record_dm (id, factory_id, machine_id, total_longan, total_unejected_longan, total_ejected_longan, batch_started_at) VALUES
(1, 1, 1, 3, 2, 1, CURRENT_TIMESTAMP),
(2, 1, 2, 3, 2, 1, CURRENT_TIMESTAMP),
(3, 2, 3, 3, 2, 1, CURRENT_TIMESTAMP),
(4, 2, 4, 3, 2, 1, CURRENT_TIMESTAMP);

INSERT INTO token_exchange_rate_dm (id, update_by, approved_by, token_to_bath_exchange_rate, start_apply_date) VALUES
(1, 1, 1, 1000.00, CURRENT_DATE);

INSERT INTO token_longan_rate_dm (id, update_by, approved_by, token_to_longan_rate, start_apply_date) VALUES
(2, 1, 1, 0.05, CURRENT_DATE);



INSERT INTO token_purchase_txn_dm (update_by, approved_by, factory_id, total_token_amt, purchase_token_amt, bath_amt) VALUES
(2, 2, 1, 100000.00, 100000.00, 1000.00);

INSERT INTO longan_lot_info_dm (batch_id, factory_id, machine_id, lot_total_longan, lot_unejected_longan, lot_ejected_longan) VALUES
(1, 1, 1, 8490, 5090, 3400);

INSERT INTO token_gift_txn_dm (update_by, approved_by, factory_id, token_amt) VALUES
(2, 2, 1, 10000.00);

INSERT INTO token_adjustment_txn_dm (update_by, approved_by, factory_id, token_amt, adjustment_type) VALUES
(2, 2, 1, 5000.00, 'subtract');



SELECT * FROM valid_txn_type;

SELECT * FROM user_account_dm;

SELECT * FROM factory_info_dm;

SELECT * FROM factory_account_dm;

SELECT * FROM machine_info_dm;

SELECT * FROM token_exchange_rate_dm;

SELECT * FROM token_longan_rate_dm;

SELECT * FROM batch_record_dm;

SELECT * FROM longan_lot_info_dm;

SELECT * FROM token_purchase_txn_dm ORDER BY timestamp DESC LIMIT 10;

SELECT * FROM token_gift_txn_dm ORDER BY timestamp DESC LIMIT 10;

SELECT * FROM token_adjustment_txn_dm ORDER BY timestamp DESC LIMIT 10;

SELECT * FROM token_longan_lot_usage_txn_dm ORDER BY timestamp DESC LIMIT 10;

SELECT * FROM token_txn_journal_dm ORDER BY timestamp DESC LIMIT 10;

SELECT * FROM token_balance_dm;

SELECT * FROM token_purchase_dm;

SELECT * FROM token_usage_dm;

SELECT * FROm token_gift_dm;

SELECT * FROM token_adjustment_dm;


SELECT id FROM token_longan_rate_dm
WHERE CURRENT_DATE >= start_apply_date AND (end_date IS NULL OR CURRENT_DATE < end_date)
ORDER BY timestamp DESC
LIMIT 1;

SELECT * FROM token_longan_rate_dm WHERE is_active = true and end_date >= CURRENT_DATE
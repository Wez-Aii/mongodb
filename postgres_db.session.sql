INSERT INTO valid_txn_type (id,valid_value) VALUES 
(1,'purchase'),
(2,'usage'),
(3,'gift'),
(4,'adjustment');

INSERT INTO user_dm (id, username, user_role) VALUES
(1,'fnamelname@01', 'admin'),
(2,'fnamelname@99', 'agent'),
(3,'fnamelname@85', 'technician'),
(4,'fnamelname@87', 'client'),
(5,'fnamelname@88', 'client');

INSERT INTO token_exchange_rate_dm (id, update_by, approved_by, token_to_bath_exchange_rate, start_apply_date) VALUES
(1, 1, 1, 1000.00, CURRENT_DATE - 1);
-- INSERT INTO token_exchange_rate_dm (id, update_by, approved_by, token_to_bath_exchange_rate, start_apply_date) VALUES
-- (2, 1, 1, 2000.00, CURRENT_DATE + 2);
-- INSERT INTO token_exchange_rate_dm (id, update_by, approved_by, token_to_bath_exchange_rate, start_apply_date) VALUES
-- (3, 1, 1, 1.00, CURRENT_DATE - 2);

INSERT INTO token_longan_rate_dm (id, update_by, approved_by, token_to_longan_rate, start_apply_date) VALUES
(1, 1, 1, 0.2, CURRENT_DATE);
-- INSERT INTO token_longan_rate_dm (id, update_by, approved_by, token_to_longan_rate, start_apply_date) VALUES
-- (2, 1, 1, 2.0, CURRENT_DATE - 14);

INSERT INTO factory_dm (id, factory_name, created_by) VALUES
(1, 'LYC Longan Factory', 2),
(2, 'Lampung 168 Longan', 2);

INSERT INTO machine_dm (id, machine_uid, alias) VALUES
(1, 'MACHINE2023001', 'LYC machine 1'),
(2, 'MACHINE2023002', 'LYC machine 2'),
(3, 'MACHINE2023003', '168 machine 1'),
(4, 'MACHINE2023004', '168 machine 1');

INSERT INTO batch_record_dm (id, factory_id, machine_id, total_longan, total_unejected_longan, total_ejected_longan, batch_started_at) VALUES
(1, 1, 1, 3, 2, 1, CURRENT_TIMESTAMP),
(2, 1, 2, 3, 2, 1, CURRENT_TIMESTAMP),
(3, 2, 3, 3, 2, 1, CURRENT_TIMESTAMP),
(4, 2, 4, 3, 2, 1, CURRENT_TIMESTAMP);



INSERT INTO token_purchase_txn_dm (update_by, approved_by, factory_id, total_token_amt, purchase_token_amt, bath_amt, date) VALUES
(2, 2, 1, 50000.00, 50000.00, 500.00, CURRENT_DATE) RETURNING id;
INSERT INTO token_purchase_txn_dm (update_by, approved_by, factory_id, token_to_bath_exchange_rate_id, total_token_amt, purchase_token_amt, bath_amt, date) VALUES
(2, 2, 1, 2, 40000.00, 40000.00, 20.00, CURRENT_DATE + 2) RETURNING id;

INSERT INTO longan_lot_info_dm (batch_id, factory_id, machine_id, lot_total_longan, lot_unejected_longan, lot_ejected_longan, date) VALUES
(1, 1, 1, 100000, 60000, 40000, CURRENT_DATE - 5) RETURNING id;

INSERT INTO token_gift_txn_dm (update_by, approved_by, factory_id, token_amt, date) VALUES
(2, 2, 1, 5000.00, CURRENT_DATE) RETURNING id;

INSERT INTO token_adjustment_txn_dm (update_by, approved_by, factory_id, token_amt, adjustment_type, date) VALUES
(2, 2, 1, 5000.00, 'subtract', CURRENT_DATE) RETURNING id;


SELECT * FROM debuging_table;

SELECT * FROM valid_txn_type;

SELECT * FROM user_dm;

SELECT * FROM factory_dm;

-- SELECT * FROM factory_account_dm;
SELECT fa.id, f.factory_name, fa.account_name, fa.timestamp FROM factory_account_dm AS fa JOIN factory_dm AS f ON f.id = fa.factory_id;

SELECT * FROM machine_dm;


SELECT * FROM longan_lot_info_dm;



-- SELECT * FROM token_exchange_rate_dm;
SELECT ter.id, u.username AS update_by, u.username AS approved_by, ter.token_to_bath_exchange_rate, ter.start_apply_date, ter.end_date, ter.timestamp FROM token_exchange_rate_dm AS ter JOIN user_dm AS u ON u.id = ter.approved_by OR u.id = ter.update_by;

-- SELECT * FROM token_longan_rate_dm;
SELECT tlr.id, u.username AS update_by, u.username AS approved_by, tlr.token_to_longan_rate, tlr.start_apply_date, tlr.end_date, tlr.timestamp FROM token_longan_rate_dm AS tlr JOIN user_dm AS u ON u.id = tlr.approved_by OR u.id = tlr.update_by;



-- SELECT * FROM batch_record_dm;
SELECT br.id, f.factory_name, m.alias AS machine_name, br.command_record_id, br.total_longan, br.total_unejected_longan, br.total_ejected_longan, br.batch_started_at, br.batch_ended_at, br.batch_end_note, br.timestamp FROM batch_record_dm AS br JOIN factory_dm AS f ON f.id = br.factory_id JOIN machine_dm AS m ON m.id = br.machine_id;

-- SELECT * FROM longan_lot_info_dm;
SELECT ll.id, ll.batch_id, f.factory_name, m.alias AS machine_name, ll.lot_total_longan, ll.lot_unejected_longan, ll.lot_ejected_longan, ll.is_processed, ll.date, ll.timestamp FROM longan_lot_info_dm AS ll JOIN factory_dm AS f ON f.id = ll.factory_id JOIN machine_dm AS m ON m.id = ll.machine_id;



-- SELECT * FROM token_purchase_txn_dm ORDER BY timestamp DESC LIMIT 10;
SELECT tpt.id, u.username AS update_by, u.username AS approved_by, f.factory_name, tpt.txn_reference_number, tpt.total_token_amt, tpt.purchase_token_amt, tpt.bath_amt, tpt.promo_token_amt, tpt.promo_name, tpt.is_processed, tpt.date, tpt.timestamp FROM token_purchase_txn_dm AS tpt JOIN user_dm AS u ON u.id = tpt.update_by OR u.id = tpt.approved_by JOIN factory_dm AS f ON f.id = tpt.factory_id;

-- SELECT * FROM token_gift_txn_dm ORDER BY timestamp DESC LIMIT 10;
SELECT tgt.id, u.username AS update_by, u.username AS approved_by, f.factory_name, tgt.note, tgt.token_amt, tgt.is_processed, tgt.date, tgt.timestamp FROM token_gift_txn_dm AS tgt JOIN user_dm AS u ON u.id = tgt.update_by OR u.id = tgt.approved_by JOIN factory_dm AS f ON f.id = tgt.factory_id ORDER BY timestamp DESC LIMIT 10;

-- SELECT * FROM token_adjustment_txn_dm ORDER BY timestamp DESC LIMIT 10;
SELECT tat.id, u.username AS update_by, u.username AS approved_by, f.factory_name, tat.note, tat.token_amt, tat.adjustment_type, tat.is_processed, tat.date, tat.timestamp FROM token_adjustment_txn_dm AS tat JOIN user_dm AS u ON u.id = tat.update_by OR u.id = tat.approved_by JOIN factory_dm AS f ON f.id = tat.factory_id ORDER BY timestamp DESC LIMIT 10;

-- SELECT * FROM token_longan_lot_usage_txn_dm ORDER BY timestamp DESC LIMIT 10;
SELECT tll.id, tll.lot_id, f.factory_name, m.alias AS machine_name, tll.longan_amt, tll.token_longan_rate_id, tll.token_to_longan_rate, tll.token_usage_amt, tll.is_processed, tll.date, tll.timestamp FROM token_longan_lot_usage_txn_dm AS tll JOIN factory_dm AS f ON f.id = tll.factory_id JOIN machine_dm AS m ON m.id = tll.machine_id ORDER BY timestamp DESC LIMIT 10;

-- SELECT * FROM token_txn_journal_dm ORDER BY timestamp DESC LIMIT 10;
SELECT ttj.id, f.factory_name, m.alias AS machine_name, fa.account_name, vtt.valid_value, ttj.txn_id, ttj.txn_token_amt, ttj.txn_entry_type, ttj.is_processed, ttj.date, ttj.timestamp FROM token_txn_journal_dm AS ttj JOIN factory_dm AS f ON f.id = ttj.factory_id LEFT JOIN machine_dm AS m ON m.id = ttj.machine_id JOIN factory_account_dm AS fa ON fa.id = ttj.account_id JOIN valid_txn_type AS vtt ON vtt.id = ttj.txn_type_id ORDER BY timestamp DESC LIMIT 10;



-- SELECT * FROM token_balance_account_dm;
SELECT tba.id, f.factory_name, tba.last_token_journal_id, fa.account_name, tba.token_balance_amt, tba.date, tba.timestamp FROM token_balance_account_dm AS tba JOIN factory_dm AS f ON f.id = tba.factory_id JOIN factory_account_dm AS fa ON fa.id = tba.account_id;

-- SELECT * FROM token_purchase_account_dm;
SELECT tpa.id, f.factory_name, tpa.last_token_journal_id, fa.account_name, tpa.token_purchase_amt, tpa.date, tpa.timestamp FROM token_purchase_account_dm AS tpa JOIN factory_dm AS f ON f.id = tpa.factory_id JOIN factory_account_dm AS fa ON fa.id = tpa.account_id;

SELECT * FROM token_usage_account_dm;
SELECT tua.id, f.factory_name, m.alias AS machine_name, tua.last_token_journal_id, fa.account_name, tua.token_usage_amt, tua.date, tua.timestamp FROM token_usage_account_dm AS tua JOIN factory_dm AS f ON f.id = tua.factory_id JOIN factory_account_dm AS fa ON fa.id = tua.account_id JOIN machine_dm AS m ON m.id = tua.machine_id;

-- SELECT * FROm token_gift_account_dm;
SELECT tga.id, f.factory_name, tga.last_token_journal_id, fa.account_name, tga.token_gift_amt, tga.date, tga.timestamp FROM token_gift_account_dm AS tga JOIN factory_dm AS f ON f.id = tga.factory_id JOIN factory_account_dm AS fa ON fa.id = tga.account_id;

-- SELECT * FROM token_adjustment_account_dm;
SELECT taa.id, f.factory_name, taa.last_token_journal_id, fa.account_name, taa.token_adjust_amt, taa.date, taa.timestamp FROM token_adjustment_account_dm AS taa JOIN factory_dm AS f ON f.id = taa.factory_id JOIN factory_account_dm AS fa ON fa.id = taa.account_id;


SELECT id FROM token_longan_rate_dm
WHERE CURRENT_DATE >= start_apply_date AND (end_date IS NULL OR CURRENT_DATE < end_date)
ORDER BY timestamp DESC
LIMIT 1;

SELECT * FROM token_longan_rate_dm WHERE is_active = true and end_date >= CURRENT_DATE
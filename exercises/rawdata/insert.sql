-- ============================================
-- BigQuery Scripting：サンプル大量データ投入テンプレ
-- bank_src.* （Raw/Landing）に対して実行
-- ============================================

-- ▼ 生成パラメータ（必要に応じて調整）
DECLARE p_customer_cnt     INT64 DEFAULT 500;   -- 顧客数
DECLARE p_txn_per_account  INT64 DEFAULT 40;    -- 口座あたり取引件数（勘定系）
DECLARE p_atm_txn_per_acct INT64 DEFAULT 12;    -- 口座あたりATM取引件数
DECLARE p_app_ev_per_cust  INT64 DEFAULT 30;    -- 顧客あたりアプリイベント件数
DECLARE p_start_date       DATE  DEFAULT DATE '2025-07-01';
DECLARE p_days_span        INT64 DEFAULT 35;    -- 生成する日付の幅（日）

-- ▼（任意）既存データを消す：何度も生成するなら有効化
-- DELETE FROM `bank_src.src_core_customer` WHERE TRUE;
-- DELETE FROM `bank_src.src_core_customer_address_hist` WHERE TRUE;
-- DELETE FROM `bank_src.src_core_account` WHERE TRUE;
-- DELETE FROM `bank_src.src_core_account_status_hist` WHERE TRUE;
-- DELETE FROM `bank_src.src_core_transaction` WHERE TRUE;
-- DELETE FROM `bank_src.src_kyc_customer` WHERE TRUE;
-- DELETE FROM `bank_src.src_kyc_verification` WHERE TRUE;
-- DELETE FROM `bank_src.src_atm_location` WHERE TRUE;
-- DELETE FROM `bank_src.src_atm_transaction` WHERE TRUE;
-- DELETE FROM `bank_src.src_transfer_order` WHERE TRUE;
-- DELETE FROM `bank_src.src_app_event` WHERE TRUE;
-- DELETE FROM `bank_src.src_ref_currency` WHERE TRUE;
-- DELETE FROM `bank_src.src_ref_account_type` WHERE TRUE;
-- DELETE FROM `bank_src.src_ref_account_status` WHERE TRUE;
-- DELETE FROM `bank_src.src_ref_channel` WHERE TRUE;
-- DELETE FROM `bank_src.src_ref_prefecture` WHERE TRUE;
-- DELETE FROM `bank_src.src_ref_city` WHERE TRUE;
-- DELETE FROM `bank_src.src_ref_bank` WHERE TRUE;

-- ============= 参照マスタを先に投入 =============
INSERT INTO `bank_src.src_ref_currency` (currency_code, currency_name, minor_unit)
SELECT * FROM UNNEST([
  STRUCT('JPY' AS currency_code, '日本円' AS currency_name, 0 AS minor_unit)
]);

INSERT INTO `bank_src.src_ref_account_type` (account_type_code, account_type_name)
SELECT * FROM UNNEST([
  STRUCT('ORD','普通預金'), STRUCT('SAV','貯蓄預金')
]);

INSERT INTO `bank_src.src_ref_account_status` (status_code, status_name)
SELECT * FROM UNNEST([
  STRUCT('ACTIVE','有効'), STRUCT('DORMANT','休眠'), STRUCT('CLOSED','解約')
]);

INSERT INTO `bank_src.src_ref_channel` (channel_code, channel_name, channel_type, platform_code, active_flag, _ingested_at)
SELECT * FROM UNNEST([
  STRUCT('APP','スマホアプリ','アプリ','-',TRUE, CURRENT_TIMESTAMP()),
  STRUCT('API','オンラインAPI','API','-',TRUE, CURRENT_TIMESTAMP()),
  STRUCT('ATM','ATM','ATM','-',TRUE, CURRENT_TIMESTAMP())
]);

INSERT INTO `bank_src.src_ref_prefecture` (prefecture_code, prefecture_name)
SELECT * FROM UNNEST([STRUCT('13','東京都'), STRUCT('40','福岡県')]);

INSERT INTO `bank_src.src_ref_city` (city_code, city_name, prefecture_code)
SELECT * FROM UNNEST([STRUCT('13101','千代田区','13'), STRUCT('40132','博多区','40')]);

INSERT INTO `bank_src.src_ref_bank` (bank_code, bank_name, branch_code, branch_name)
SELECT * FROM UNNEST([
  STRUCT('0033','セブン銀行','001','本店'),
  STRUCT('0001','みずほ銀行','001','本店営業部')
]);

-- ============= 顧客 =============
INSERT INTO `bank_src.src_core_customer`
(customer_id, legal_name, birth_date, gender_code, segment_code, status_code, created_at, updated_at, _ingested_at, _source_system)
SELECT
  FORMAT('CUST%06d', i) AS customer_id,
  FORMAT('顧客_%06d', i) AS legal_name,
  DATE_ADD(DATE '1985-01-01', INTERVAL CAST(2000*RAND() AS INT64) DAY) AS birth_date,
  IF(RAND() < 0.5, 'M', 'F') AS gender_code,
  'PERSONAL' AS segment_code,
  'ACTIVE' AS status_code,
  TIMESTAMP(DATETIME(p_start_date, TIME  '09:00:00')) AS created_at,
  TIMESTAMP_ADD(TIMESTAMP(DATETIME(p_start_date, TIME '09:00:00')),
                INTERVAL CAST(RAND()*p_days_span*24 AS INT64) HOUR) AS updated_at,
  CURRENT_TIMESTAMP() AS _ingested_at,
  'core' AS _source_system
FROM UNNEST(GENERATE_ARRAY(1, p_customer_cnt)) AS i;

-- 住所履歴（8割は1件、2割は2件：途中で引越し）
INSERT INTO `bank_src.src_core_customer_address_hist`
(customer_id, address_line, city_name, prefecture_code, postal_code, country_code, valid_from, valid_to, is_current, _ingested_at, _source_system)
WITH base AS (
  SELECT FORMAT('CUST%06d', i) AS customer_id,
         IF(RAND()<0.5, '13101', '40132') AS city_code
  FROM UNNEST(GENERATE_ARRAY(1, p_customer_cnt)) AS i
)
SELECT
  customer_id,
  IF(city_code='13101','東京都千代田区丸の内1-1-1','福岡県福岡市博多区博多駅前2-2-2') AS address_line,
  IF(city_code='13101','千代田区','博多区') AS city_name,
  IF(city_code='13101','13','40') AS prefecture_code,
  IF(city_code='13101','1000005','8120011') AS postal_code,
  'JP' AS country_code,
  TIMESTAMP(p_start_date) AS valid_from,
  IF(RAND()<0.2, TIMESTAMP_ADD(TIMESTAMP(p_start_date), INTERVAL CAST(7 + RAND()*10 AS INT64) DAY), NULL) AS valid_to,
  IFNULL(valid_to IS NULL, TRUE) AS is_current,
  CURRENT_TIMESTAMP(), 'core'
FROM base;

-- ============= 口座 =============
-- 1顧客=1口座（必要なら UNION ALL で2口座に拡張）
INSERT INTO `bank_src.src_core_account`
(account_id, customer_id, account_type_code, currency_code, opened_at, closed_at, status_code, _ingested_at, _source_system)
SELECT
  FORMAT('ACCT%06d', i) AS account_id,
  FORMAT('CUST%06d', i) AS customer_id,
  IF(RAND()<0.8,'ORD','SAV') AS account_type_code,
  'JPY' AS currency_code,
  TIMESTAMP_ADD(TIMESTAMP(p_start_date), INTERVAL CAST(RAND()*7 AS INT64) DAY) AS opened_at,
  NULL AS closed_at,
  'ACTIVE' AS status_code,
  CURRENT_TIMESTAMP(), 'core'
FROM UNNEST(GENERATE_ARRAY(1, p_customer_cnt)) AS i;

-- 口座ステータス履歴（現行のみ）
INSERT INTO `bank_src.src_core_account_status_hist`
(account_id, status_code, valid_from, valid_to, is_current, _ingested_at, _source_system)
SELECT
  FORMAT('ACCT%06d', i), 'ACTIVE',
  TIMESTAMP_ADD(TIMESTAMP(p_start_date), INTERVAL CAST(RAND()*7 AS INT64) DAY),
  NULL, TRUE, CURRENT_TIMESTAMP(), 'core'
FROM UNNEST(GENERATE_ARRAY(1, p_customer_cnt)) AS i;

-- ============= 勘定系トランザクション（入出金・送金） =============
INSERT INTO `bank_src.src_core_transaction`
(txn_id, account_id, customer_id, txn_type_code, amount, fee_amount, currency_code, channel_code,
 counterparty_bank_code, counterparty_account, event_ts, posted_ts, _ingested_at, _source_system, _batch_id)
WITH acts AS (
  SELECT FORMAT('ACCT%06d', i) AS account_id,
         FORMAT('CUST%06d', i) AS customer_id
  FROM UNNEST(GENERATE_ARRAY(1, p_customer_cnt)) AS i
),
seq AS (
  SELECT n FROM UNNEST(GENERATE_ARRAY(1, p_txn_per_account)) AS n
)
SELECT
  FORMAT('TXN%s_%06d_%03d',
         CASE WHEN RAND()<0.33 THEN 'DEP'
              WHEN RAND()<0.66 THEN 'WDR'
              ELSE 'TRF' END,
         CAST(SUBSTR(a.account_id,5) AS INT64),
         n) AS txn_id,
  a.account_id,
  a.customer_id,
  CASE WHEN RAND()<0.4 THEN 'DEPOSIT'
       WHEN RAND()<0.8 THEN 'WITHDRAWAL'
       ELSE 'TRANSFER_OUT' END AS txn_type_code,
  -- 金額分布：入金は大きめ、出金は小〜中
  CASE
    WHEN txn_type_code='DEPOSIT'     THEN CAST(1000 + RAND()*90000 AS NUMERIC)
    WHEN txn_type_code='WITHDRAWAL'  THEN CAST(1000 + RAND()*40000 AS NUMERIC)
    ELSE CAST(3000 + RAND()*60000 AS NUMERIC)
  END AS amount,
  CAST( (CASE WHEN txn_type_code='DEPOSIT' THEN 110 ELSE 220 END) AS NUMERIC) AS fee_amount,
  'JPY' AS currency_code,
  -- チャネル分布：ATM 50%、APP 40%、API 10%
  (SELECT channel FROM UNNEST(['ATM','APP','API']) channel WITH OFFSET off
   WHERE off = CAST(FLOOR(RAND()*3) AS INT64)) AS channel_code,
  IF(txn_type_code='TRANSFER_OUT', '0001', NULL) AS counterparty_bank_code,
  IF(txn_type_code='TRANSFER_OUT', '1234567', NULL) AS counterparty_account,
  -- イベント日時（期間内ランダム）
  TIMESTAMP_ADD(TIMESTAMP(p_start_date), INTERVAL CAST(RAND()*p_days_span*24*60 AS INT64) MINUTE) AS event_ts,
  TIMESTAMP_ADD(event_ts, INTERVAL CAST(1 + RAND()*30 AS INT64) MINUTE) AS posted_ts,
  CURRENT_TIMESTAMP(), 'core', 'BATCH_SYNTH'
FROM acts a CROSS JOIN seq;

-- ============= KYC =============
INSERT INTO `bank_src.src_kyc_customer`
(customer_id, kyc_status_code, risk_score, last_reviewed_at, id_doc_type, id_doc_masked, _ingested_at, _source_system)
SELECT
  FORMAT('CUST%06d', i),
  IF(RAND()<0.97,'APPROVED','PENDING') AS kyc_status_code,
  CAST(10 + RAND()*30 AS INT64) AS risk_score,
  TIMESTAMP_ADD(TIMESTAMP(p_start_date), INTERVAL CAST(RAND()*10 AS INT64) DAY),
  'DRIVER',
  FORMAT('****-%04d', CAST(1000 + RAND()*8999 AS INT64)),
  CURRENT_TIMESTAMP(), 'kyc'
FROM UNNEST(GENERATE_ARRAY(1, p_customer_cnt)) AS i;

INSERT INTO `bank_src.src_kyc_verification`
(verification_id, customer_id, method_code, selfie_score, ocr_score, status_code, created_at, _ingested_at, _source_system)
SELECT
  FORMAT('KYCVER%06d', i),
  FORMAT('CUST%06d', i),
  'eKYC',
  ROUND(0.85 + RAND()*0.14, 2),
  ROUND(0.85 + RAND()*0.14, 2),
  'PASSED',
  TIMESTAMP_ADD(TIMESTAMP(p_start_date), INTERVAL CAST(RAND()*5 AS INT64) DAY),
  CURRENT_TIMESTAMP(), 'kyc'
FROM UNNEST(GENERATE_ARRAY(1, p_customer_cnt)) AS i;

-- ============= ATM ロケーション & 取引 =============
INSERT INTO `bank_src.src_atm_location`
(atm_location_id, provider_name, prefecture_code, city_name, latitude, longitude, active_flag, _ingested_at, _source_system)
SELECT * FROM UNNEST([
  STRUCT('ATMLOC001','セブン銀行','13','千代田区', 35.6812, 139.7671, TRUE, CURRENT_TIMESTAMP(), 'atm'),
  STRUCT('ATMLOC002','セブン銀行','40','博多区'  , 33.5902, 130.4017, TRUE, CURRENT_TIMESTAMP(), 'atm')
]);

INSERT INTO `bank_src.src_atm_transaction`
(atm_txn_id, account_id, customer_id, partner_code, atm_terminal_id, atm_location_id,
 atm_txn_type_code, amount, fee_amount, currency_code, atm_txn_ts, _ingested_at, _source_system)
WITH acts AS (
  SELECT FORMAT('ACCT%06d', i) AS account_id,
         FORMAT('CUST%06d', i) AS customer_id
  FROM UNNEST(GENERATE_ARRAY(1, p_customer_cnt)) AS i
),
seq AS (SELECT n FROM UNNEST(GENERATE_ARRAY(1, p_atm_txn_per_acct)) AS n)
SELECT
  FORMAT('ATX%06d_%03d', CAST(SUBSTR(a.account_id,5) AS INT64), n) AS atm_txn_id,
  a.account_id, a.customer_id,
  'SEVEN_BANK' AS partner_code,
  FORMAT('TERM%03d', CAST(1 + RAND()*50 AS INT64)) AS atm_terminal_id,
  IF(RAND()<0.5,'ATMLOC001','ATMLOC002') AS atm_location_id,
  IF(RAND()<0.5,'CASH_DEPOSIT','CASH_WITHDRAWAL') AS atm_txn_type_code,
  CAST(1000 + RAND()*80000 AS NUMERIC) AS amount,
  CAST(110 + RAND()*110 AS NUMERIC) AS fee_amount,
  'JPY' AS currency_code,
  TIMESTAMP_ADD(TIMESTAMP(p_start_date), INTERVAL CAST(RAND()*p_days_span*24*60 AS INT64) MINUTE) AS atm_txn_ts,
  CURRENT_TIMESTAMP(), 'atm'
FROM acts a CROSS JOIN seq;

-- ============= 振込ゲートウェイ（送金依頼） =============
INSERT INTO `bank_src.src_transfer_order`
(transfer_id, debit_account_id, customer_id, amount, fee_amount, currency_code,
 dest_bank_code, dest_branch_code, dest_account_id, dest_account_name,
 channel_code, initiated_ts, settled_ts, status_code, _ingested_at, _source_system)
WITH acts AS (
  SELECT FORMAT('ACCT%06d', i) AS account_id,
         FORMAT('CUST%06d', i) AS customer_id
  FROM UNNEST(GENERATE_ARRAY(1, p_customer_cnt)) AS i
),
seq AS (SELECT n FROM UNNEST(GENERATE_ARRAY(1, GREATEST(1, p_txn_per_account/4))) AS n)
SELECT
  FORMAT('TRF%06d_%03d', CAST(SUBSTR(a.account_id,5) AS INT64), n) AS transfer_id,
  a.account_id, a.customer_id,
  CAST(3000 + RAND()*70000 AS NUMERIC) AS amount,
  CAST(220 + RAND()*110 AS NUMERIC) AS fee_amount,
  'JPY' AS currency_code,
  '0001' AS dest_bank_code, '001' AS dest_branch_code, '1234567' AS dest_account_id, 'ミズホ タロウ' AS dest_account_name,
  'APP' AS channel_code,
  TIMESTAMP_ADD(TIMESTAMP(p_start_date), INTERVAL CAST(RAND()*p_days_span*24*60 AS INT64) MINUTE) AS initiated_ts,
  TIMESTAMP_ADD(initiated_ts, INTERVAL CAST(1 + RAND()*20 AS INT64) MINUTE) AS settled_ts,
  'SETTLED' AS status_code,
  CURRENT_TIMESTAMP(), 'transfer'
FROM acts a CROSS JOIN seq;

-- ============= アプリイベント（残高照会 等） =============
INSERT INTO `bank_src.src_app_event`
(event_id, customer_id, account_id, event_name, event_ts, channel_code, platform_code,
 app_version, device_id, session_id, screen_name, _ingested_at, _source_system)
WITH base AS (
  SELECT FORMAT('CUST%06d', i) AS customer_id,
         FORMAT('ACCT%06d', i) AS account_id
  FROM UNNEST(GENERATE_ARRAY(1, p_customer_cnt)) AS i
),
seq AS (SELECT n FROM UNNEST(GENERATE_ARRAY(1, p_app_ev_per_cust)) AS n)
SELECT
  FORMAT('EVT%06d_%03d', CAST(SUBSTR(b.account_id,5) AS INT64), n) AS event_id,
  b.customer_id, b.account_id,
  (SELECT ev FROM UNNEST(['BALANCE_VIEW','TXN_LIST_VIEW','LOGIN']) ev WITH OFFSET off
   WHERE off = CAST(FLOOR(RAND()*3) AS INT64)) AS event_name,
  TIMESTAMP_ADD(TIMESTAMP(p_start_date), INTERVAL CAST(RAND()*p_days_span*24*60 AS INT64) MINUTE) AS event_ts,
  'APP' AS channel_code,
  IF(RAND()<0.5,'iOS','Android') AS platform_code,
  FORMAT('%d.%d.%d', 3, CAST(1 + RAND()*4 AS INT64), CAST(0 + RAND()*9 AS INT64)) AS app_version,
  FORMAT('DEV-%06d', CAST(RAND()*999999 AS INT64)) AS device_id,
  FORMAT('SESS-%06d', CAST(RAND()*999999 AS INT64)) AS session_id,
  'BalanceScreen' AS screen_name,
  CURRENT_TIMESTAMP(), 'app'
FROM base b CROSS JOIN seq;

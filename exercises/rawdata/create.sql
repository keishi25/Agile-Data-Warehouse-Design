-- ✅ 推奨：専用データセット
CREATE SCHEMA IF NOT EXISTS `bank_src`
OPTIONS (location="asia-northeast1", description="みんなの銀行: ソース系(Raw/Staging)");

/* ================================
   1) 勘定系：顧客・口座・取引
================================ */

-- 顧客マスタ（自然キー：customer_id）
CREATE TABLE IF NOT EXISTS `bank_src.src_core_customer` (
  customer_id           STRING NOT NULL,         -- 自然キー
  legal_name            STRING,                  -- 氏名（KYCと突合）
  birth_date            DATE,
  gender_code           STRING,                  -- M/F/...
  segment_code          STRING,                  -- PERSONAL/VIP 等（初期判定）
  status_code           STRING,                  -- ACTIVE/INACTIVE 等
  created_at            TIMESTAMP,
  updated_at            TIMESTAMP,
  _ingested_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  _source_system        STRING,
  _batch_id             STRING,
  _file_name            STRING
)
PARTITION BY DATE(_ingested_at)
CLUSTER BY customer_id;

-- 顧客住所（履歴管理：SCD2の元データ）
CREATE TABLE IF NOT EXISTS `bank_src.src_core_customer_address_hist` (
  customer_id           STRING NOT NULL,
  address_line          STRING,
  city_name             STRING,
  prefecture_code       STRING,
  postal_code           STRING,
  country_code          STRING,
  valid_from            TIMESTAMP NOT NULL,
  valid_to              TIMESTAMP,              -- NULL=現行
  is_current            BOOL,
  _ingested_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  _source_system        STRING
)
PARTITION BY DATE(valid_from)
CLUSTER BY customer_id;

-- 口座マスタ（自然キー：account_id）
CREATE TABLE IF NOT EXISTS `bank_src.src_core_account` (
  account_id            STRING NOT NULL,        -- 自然キー
  customer_id           STRING NOT NULL,
  account_type_code     STRING,                 -- ORD/SAV 等
  currency_code         STRING,                 -- JPY 等
  opened_at             TIMESTAMP,
  closed_at             TIMESTAMP,
  status_code           STRING,                 -- ACTIVE/DORMANT/CLOSED
  _ingested_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  _source_system        STRING
)
PARTITION BY DATE(opened_at)
CLUSTER BY customer_id, account_id;

-- 口座ステータス履歴（SCD2の元データ）
CREATE TABLE IF NOT EXISTS `bank_src.src_core_account_status_hist` (
  account_id            STRING NOT NULL,
  status_code           STRING,
  valid_from            TIMESTAMP NOT NULL,
  valid_to              TIMESTAMP,
  is_current            BOOL,
  _ingested_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  _source_system        STRING
)
PARTITION BY DATE(valid_from)
CLUSTER BY account_id;

-- 取引トランザクション（勘定系：入出金/振替/手数料 等）
CREATE TABLE IF NOT EXISTS `bank_src.src_core_transaction` (
  txn_id                STRING NOT NULL,        -- 自然キー
  account_id            STRING NOT NULL,
  customer_id           STRING,
  txn_type_code         STRING,                 -- DEPOSIT/WITHDRAWAL/TRANSFER_IN/TRANSFER_OUT/...
  amount                NUMERIC,                -- 38,9 相当
  fee_amount            NUMERIC,
  currency_code         STRING,
  channel_code          STRING,                 -- APP/API/ATM 等
  counterparty_bank_code STRING,                -- 他行振込時
  counterparty_account  STRING,
  event_ts              TIMESTAMP,              -- 発生時刻
  posted_ts             TIMESTAMP,              -- 計上時刻
  _ingested_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  _source_system        STRING,
  _batch_id             STRING
)
PARTITION BY DATE(event_ts)
CLUSTER BY account_id, customer_id, txn_type_code;

/* ================================
   2) KYC
================================ */

CREATE TABLE IF NOT EXISTS `bank_src.src_kyc_customer` (
  customer_id           STRING NOT NULL,
  kyc_status_code       STRING,                 -- PENDING/APPROVED/REJECTED
  risk_score            INT64,
  last_reviewed_at      TIMESTAMP,
  id_doc_type           STRING,                 -- PASSPORT/DRIVER 等
  id_doc_masked         STRING,
  _ingested_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  _source_system        STRING
)
PARTITION BY DATE(_ingested_at)
CLUSTER BY customer_id;

CREATE TABLE IF NOT EXISTS `bank_src.src_kyc_verification` (
  verification_id       STRING NOT NULL,
  customer_id           STRING NOT NULL,
  method_code           STRING,                 -- eKYC/Manual 等
  selfie_score          NUMERIC,
  ocr_score             NUMERIC,
  status_code           STRING,                 -- PASSED/FAILED
  created_at            TIMESTAMP,
  _ingested_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  _source_system        STRING
)
PARTITION BY DATE(created_at)
CLUSTER BY customer_id, status_code;

/* ================================
   3) セブン銀行ATM 連携
================================ */

-- ATM 取引（入金/出金）
CREATE TABLE IF NOT EXISTS `bank_src.src_atm_transaction` (
  atm_txn_id            STRING NOT NULL,
  account_id            STRING NOT NULL,
  customer_id           STRING,
  partner_code          STRING,                 -- SEVEN_BANK 固定など
  atm_terminal_id       STRING,
  atm_location_id       STRING,                 -- 位置情報キー
  atm_txn_type_code     STRING,                 -- CASH_DEPOSIT/CASH_WITHDRAWAL
  amount                NUMERIC,
  fee_amount            NUMERIC,
  currency_code         STRING,
  atm_txn_ts            TIMESTAMP,
  _ingested_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  _source_system        STRING
)
PARTITION BY DATE(atm_txn_ts)
CLUSTER BY account_id, atm_txn_type_code;

-- ATM ロケーション辞書
CREATE TABLE IF NOT EXISTS `bank_src.src_atm_location` (
  atm_location_id       STRING NOT NULL,
  provider_name         STRING,                 -- セブン銀行 等
  prefecture_code       STRING,
  city_name             STRING,
  latitude              NUMERIC,
  longitude             NUMERIC,
  active_flag           BOOL,
  _ingested_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  _source_system        STRING
);

/* ================================
   4) 振込ゲートウェイ
================================ */

CREATE TABLE IF NOT EXISTS `bank_src.src_transfer_order` (
  transfer_id           STRING NOT NULL,
  debit_account_id      STRING NOT NULL,        -- 送金元
  customer_id           STRING,
  amount                NUMERIC,
  fee_amount            NUMERIC,
  currency_code         STRING,
  dest_bank_code        STRING,
  dest_branch_code      STRING,
  dest_account_id       STRING,
  dest_account_name     STRING,
  channel_code          STRING,                 -- APP/API
  initiated_ts          TIMESTAMP,              -- 依頼時刻
  settled_ts            TIMESTAMP,              -- 決済完了
  status_code           STRING,                 -- PENDING/SENT/SETTLED/FAILED
  _ingested_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  _source_system        STRING
)
PARTITION BY DATE(initiated_ts)
CLUSTER BY debit_account_id, customer_id, status_code;

/* ================================
   5) アプリイベント（残高照会 など）
================================ */

CREATE TABLE IF NOT EXISTS `bank_src.src_app_event` (
  event_id              STRING NOT NULL,
  customer_id           STRING,
  account_id            STRING,
  event_name            STRING,                 -- BALANCE_VIEW 等
  event_ts              TIMESTAMP,
  channel_code          STRING,                 -- APP 固定可
  platform_code         STRING,                 -- iOS/Android
  app_version           STRING,
  device_id             STRING,
  session_id            STRING,
  screen_name           STRING,
  _ingested_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  _source_system        STRING
)
PARTITION BY DATE(event_ts)
CLUSTER BY customer_id, event_name;

/* ================================
   6) 参照マスタ（適合Dの元）
================================ */

-- チャネル辞書（チャネルDの元）
CREATE TABLE IF NOT EXISTS `bank_src.src_ref_channel` (
  channel_code          STRING NOT NULL,        -- APP/API/ATM...
  channel_name          STRING,
  channel_type          STRING,                 -- アプリ/オンラインAPI/ATM
  platform_code         STRING,                 -- iOS/Android/-
  active_flag           BOOL,
  _ingested_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- 通貨辞書（通貨Dの元）
CREATE TABLE IF NOT EXISTS `bank_src.src_ref_currency` (
  currency_code         STRING NOT NULL,        -- JPY 等
  currency_name         STRING,
  minor_unit            INT64                   -- 小数桁
);

-- 口座種別辞書
CREATE TABLE IF NOT EXISTS `bank_src.src_ref_account_type` (
  account_type_code     STRING NOT NULL,        -- ORD/SAV
  account_type_name     STRING
);

-- 口座ステータス辞書
CREATE TABLE IF NOT EXISTS `bank_src.src_ref_account_status` (
  status_code           STRING NOT NULL,        -- ACTIVE/DORMANT/CLOSED
  status_name           STRING
);

-- （任意）銀行コード辞書（振込先）
CREATE TABLE IF NOT EXISTS `bank_src.src_ref_bank` (
  bank_code             STRING NOT NULL,
  bank_name             STRING,
  branch_code           STRING,
  branch_name           STRING
);

-- （任意）日本の地域辞書
CREATE TABLE IF NOT EXISTS `bank_src.src_ref_prefecture` (
  prefecture_code       STRING NOT NULL,
  prefecture_name       STRING
);
CREATE TABLE IF NOT EXISTS `bank_src.src_ref_city` (
  city_code             STRING NOT NULL,
  city_name             STRING,
  prefecture_code       STRING
);

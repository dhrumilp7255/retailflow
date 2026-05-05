-- ============================================
-- RETAIL OLTP DATABASE SCHEMA
-- ============================================

CREATE TABLE IF NOT EXISTS store (
    store_key        INTEGER PRIMARY KEY,
    store_num        INTEGER,
    store_desc       VARCHAR(100),
    addr             VARCHAR(200),
    city             VARCHAR(100),
    region           VARCHAR(100),
    cntry_cd         VARCHAR(10),
    cntry_nm         VARCHAR(100),
    postal_zip_cd    VARCHAR(20),
    prov_state_desc  VARCHAR(100),
    prov_state_cd    VARCHAR(10),
    store_type_cd    VARCHAR(10),
    store_type_desc  VARCHAR(100),
    frnchs_flg       VARCHAR(5),
    store_size       VARCHAR(20),
    market_key       INTEGER,
    market_name      VARCHAR(100),
    submarket_key    INTEGER,
    submarket_name   VARCHAR(100),
    latitude         NUMERIC(9,6),
    longitude        NUMERIC(9,6)
);

CREATE TABLE IF NOT EXISTS product (
    prod_key          INTEGER PRIMARY KEY,
    prod_name         VARCHAR(200),
    vol               NUMERIC(10,2),
    wgt               NUMERIC(10,2),
    brand_name        VARCHAR(100),
    status_code       INTEGER,
    status_code_name  VARCHAR(50),
    category_key      INTEGER,
    category_name     VARCHAR(100),
    subcategory_key   INTEGER,
    subcategory_name  VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS calendar (
    cal_dt          DATE PRIMARY KEY,
    cal_type_desc   VARCHAR(50),
    day_of_wk_num   INTEGER,
    day_of_wk_desc  VARCHAR(20),
    yr_num          INTEGER,
    wk_num          INTEGER,
    yr_wk_num       INTEGER,
    mnth_num        INTEGER,
    yr_mnth_num     INTEGER,
    qtr_num         INTEGER,
    yr_qtr_num      INTEGER
);

CREATE TABLE IF NOT EXISTS sales (
    trans_id     INTEGER PRIMARY KEY,
    prod_key     INTEGER REFERENCES product(prod_key),
    store_key    INTEGER REFERENCES store(store_key),
    trans_dt     DATE REFERENCES calendar(cal_dt),
    trans_time   INTEGER,
    sales_qty    NUMERIC(10,2),
    sales_price  NUMERIC(10,2),
    sales_amt    NUMERIC(10,2),
    discount     NUMERIC(10,2),
    cost_amt     NUMERIC(10,2)
);

CREATE TABLE IF NOT EXISTS inventory (
    cal_dt              DATE REFERENCES calendar(cal_dt),
    store_key           INTEGER REFERENCES store(store_key),
    prod_key            INTEGER REFERENCES product(prod_key),
    inventory_on_hand_qty  NUMERIC(10,2),
    inventory_on_order_qty NUMERIC(10,2),
    out_of_stock_flg    BOOLEAN,
    waste_qty           NUMERIC(10,2),
    promotion_flg       BOOLEAN,
    low_stock_flg       BOOLEAN,
    PRIMARY KEY (cal_dt, store_key, prod_key)
);
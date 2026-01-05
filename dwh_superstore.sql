CREATE SCHEMA IF NOT EXISTS stg;

CREATE TABLE if NOT EXISTS stg.superstore (
    ship_mode     TEXT,
    segment       TEXT,
    country       TEXT,
    city          TEXT,
    state         TEXT,
    postal_code   TEXT,
    region        TEXT,
    category      TEXT,
    sub_category  TEXT,
    sales         NUMERIC,
    quantity      INTEGER,
    discount      NUMERIC,
    profit        NUMERIC,
    -- технические поля
    load_dts      TIMESTAMP DEFAULT now(),          -- когда строка попала в STG
    record_source TEXT      DEFAULT 'kaggle_superstore_csv'  -- откуда данные
);

COPY stg.superstore (
    ship_mode,
    segment,
    country,
    city,
    state,
    postal_code,
    region,
    category,
    sub_category,
    sales,
    quantity,
    discount,
    profit
)
FROM '/var/lib/postgresql/SampleSuperstore.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',');

SELECT * FROM stg.superstore limit 5;


--- DDS ---

CREATE SCHEMA IF NOT EXISTS dds;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

DROP TABLE IF EXISTS dds.h_subcategory;

CREATE TABLE dds.h_subcategory (
    h_subcategory_hk  BYTEA       NOT NULL,  -- hash key
    category          TEXT        NOT NULL,
    sub_category      TEXT        NOT NULL,
    load_dts          TIMESTAMP   NOT NULL,
    record_source     TEXT        NOT NULL,
    CONSTRAINT pk_h_subcategory PRIMARY KEY (h_subcategory_hk)
);

INSERT INTO dds.h_subcategory (
    h_subcategory_hk,
    category,
    sub_category,
    load_dts,
    record_source
)
SELECT DISTINCT
    digest(
        upper(trim(coalesce(category, ''))) || '|' ||
        upper(trim(coalesce(sub_category, ''))),
        'md5'
    )                       AS h_subcategory_hk,
    category,
    sub_category,
    now()                   AS load_dts,
    'stg.superstore'        AS record_source
FROM stg.superstore;


DROP TABLE IF EXISTS dds.h_segment;

CREATE TABLE dds.h_segment (
    h_segment_hk   BYTEA      NOT NULL,
    segment        TEXT       NOT NULL,
    load_dts       TIMESTAMP  NOT NULL,
    record_source  TEXT       NOT NULL,
    CONSTRAINT pk_h_segment PRIMARY KEY (h_segment_hk)
);

INSERT INTO dds.h_segment (
    h_segment_hk,
    segment,
    load_dts,
    record_source
)
SELECT DISTINCT
    digest(
        upper(trim(coalesce(segment, ''))),
        'md5'
    )                  AS h_segment_hk,
    segment,
    now()              AS load_dts,
    'stg.superstore'   AS record_source
FROM stg.superstore;



DROP TABLE IF EXISTS dds.h_city;

CREATE TABLE dds.h_city (
    h_city_hk      BYTEA      NOT NULL,
    country        TEXT       NOT NULL,
    state          TEXT       NOT NULL,
    city           TEXT       NOT NULL,
    load_dts       TIMESTAMP  NOT NULL,
    record_source  TEXT       NOT NULL,
    CONSTRAINT pk_h_city PRIMARY KEY (h_city_hk)
);

INSERT INTO dds.h_city (
    h_city_hk,
    country,
    state,
    city,
    load_dts,
    record_source
)
SELECT DISTINCT
    digest(
        upper(trim(coalesce(country, ''))) || '|' ||
        upper(trim(coalesce(state,   ''))) || '|' ||
        upper(trim(coalesce(city,    ''))),
        'md5'
    )                  AS h_city_hk,
    country,
    state,
    city,
    now()              AS load_dts,
    'stg.superstore'   AS record_source
FROM stg.superstore;



SELECT COUNT(*) FROM dds.h_subcategory;
SELECT * FROM dds.h_segment;
SELECT COUNT(*) FROM dds.h_city;


DROP TABLE IF EXISTS dds.l_sale;

CREATE TABLE dds.l_sale (
    l_sale_hk        BYTEA      NOT NULL,
    h_city_hk        BYTEA      NOT NULL,
    h_segment_hk     BYTEA      NOT NULL,
    h_subcategory_hk BYTEA      NOT NULL,
    load_dts         TIMESTAMP  NOT NULL,
    record_source    TEXT       NOT NULL,
    CONSTRAINT pk_l_sale PRIMARY KEY (l_sale_hk),
    CONSTRAINT fk_l_sale_h_city
        FOREIGN KEY (h_city_hk) REFERENCES dds.h_city (h_city_hk),
    CONSTRAINT fk_l_sale_h_segment
        FOREIGN KEY (h_segment_hk) REFERENCES dds.h_segment (h_segment_hk),
    CONSTRAINT fk_l_sale_h_subcategory
        FOREIGN KEY (h_subcategory_hk) REFERENCES dds.h_subcategory (h_subcategory_hk)
);

INSERT INTO dds.l_sale (
    l_sale_hk,
    h_city_hk,
    h_segment_hk,
    h_subcategory_hk,
    load_dts,
    record_source
)
SELECT DISTINCT
    digest(
        hci.h_city_hk::text || '|' ||
        hse.h_segment_hk::text || '|' ||
        hsu.h_subcategory_hk::text,
        'md5'
    )              AS l_sale_hk,
    hci.h_city_hk,
    hse.h_segment_hk,
    hsu.h_subcategory_hk,
    now()          AS load_dts,
    'stg.superstore' AS record_source
FROM stg.superstore stg
JOIN dds.h_city        hci
  ON hci.country = stg.country
 AND hci.state   = stg.state
 AND hci.city    = stg.city
JOIN dds.h_segment     hse
  ON hse.segment = stg.segment
JOIN dds.h_subcategory hsu
  ON hsu.category     = stg.category
 AND hsu.sub_category = stg.sub_category;


DROP TABLE IF EXISTS dds.s_l_sale_metrics;

CREATE TABLE dds.s_l_sale_metrics (
    l_sale_hk      BYTEA      NOT NULL,   -- FK на l_sale
    load_dts       TIMESTAMP  NOT NULL,   -- время загрузки satellite
    record_source  TEXT       NOT NULL,
    sales          NUMERIC,
    quantity       INTEGER,
    discount       NUMERIC,
    profit         NUMERIC,
    ship_mode      TEXT       NOT NULL,
    hashdiff       BYTEA,
    CONSTRAINT pk_s_l_sale_metrics PRIMARY KEY (l_sale_hk, load_dts),
    CONSTRAINT fk_s_l_sale_metrics_l_sale
        FOREIGN KEY (l_sale_hk) REFERENCES dds.l_sale (l_sale_hk)
);


INSERT INTO dds.s_l_sale_metrics (
    l_sale_hk,
    load_dts,
    record_source,
    sales,
    quantity,
    discount,
    profit,
    ship_mode,
    hashdiff
)
SELECT
    ls.l_sale_hk,
    now()              AS load_dts,
    'stg.superstore'   AS record_source,
    SUM(stg.sales)     AS sales,
    SUM(stg.quantity)  AS quantity,
    AVG(stg.discount)  AS discount,
    SUM(stg.profit)    AS profit,
    MIN(stg.ship_mode) AS ship_mode,
    digest(
        coalesce(SUM(stg.sales)::text, '')    || '|' ||
        coalesce(SUM(stg.quantity)::text, '') || '|' ||
        coalesce(AVG(stg.discount)::text, '') || '|' ||
        coalesce(SUM(stg.profit)::text, '')   || '|' ||
        coalesce(MIN(stg.ship_mode), ''),
        'md5'
    ) AS hashdiff
FROM stg.superstore stg
JOIN dds.h_city        hci
  ON hci.country = stg.country
 AND hci.state   = stg.state
 AND hci.city    = stg.city
JOIN dds.h_segment     hse
  ON hse.segment = stg.segment
JOIN dds.h_subcategory hsu
  ON hsu.category     = stg.category
 AND hsu.sub_category = stg.sub_category
JOIN dds.l_sale        ls
  ON ls.h_city_hk        = hci.h_city_hk
 AND ls.h_segment_hk     = hse.h_segment_hk
 AND ls.h_subcategory_hk = hsu.h_subcategory_hk
GROUP BY
    ls.l_sale_hk;


DROP TABLE IF EXISTS dds.s_h_city_attrs;

CREATE TABLE dds.s_h_city_attrs (
    h_city_hk      BYTEA      NOT NULL,
    load_dts       TIMESTAMP  NOT NULL,
    record_source  TEXT       NOT NULL,
    region         TEXT,
    postal_code    TEXT,
    hashdiff       BYTEA,
    CONSTRAINT pk_s_h_city_attrs PRIMARY KEY (h_city_hk, load_dts),
    CONSTRAINT fk_s_h_city_attrs_h_city
        FOREIGN KEY (h_city_hk) REFERENCES dds.h_city (h_city_hk)
);

INSERT INTO dds.s_h_city_attrs (
    h_city_hk,
    load_dts,
    record_source,
    region,
    postal_code,
    hashdiff
)
SELECT
    hci.h_city_hk,
    now()             AS load_dts,
    'stg.superstore'  AS record_source,
    MIN(stg.region)       AS region,
    MIN(stg.postal_code)  AS postal_code,
    digest(
        coalesce(MIN(stg.region), '') || '|' ||
        coalesce(MIN(stg.postal_code), ''),
        'md5'
    ) AS hashdiff
FROM stg.superstore stg
JOIN dds.h_city hci
  ON hci.country = stg.country
 AND hci.state   = stg.state
 AND hci.city    = stg.city
GROUP BY
    hci.h_city_hk;

-- CDM --

CREATE SCHEMA IF NOT EXISTS cdm;

CREATE OR REPLACE VIEW cdm.v_fact_sales AS
SELECT
    ls.l_sale_hk                    AS sale_key,
    hc.h_city_hk                    AS city_key,
    hs.h_segment_hk                 AS segment_key,
    hsc.h_subcategory_hk            AS subcategory_key,
    hc.country,
    hc.state,
    hc.city,
    sc.region,
    sc.postal_code,
    hs.segment,
    hsc.category,
    hsc.sub_category,
    sm.sales,
    sm.quantity,
    sm.discount,
    sm.profit,
    sm.ship_mode,
    sm.load_dts                     AS fact_load_dts
FROM dds.l_sale              ls
JOIN dds.h_city              hc  ON hc.h_city_hk = ls.h_city_hk
LEFT JOIN dds.s_h_city_attrs sc  ON sc.h_city_hk = hc.h_city_hk
JOIN dds.h_segment           hs  ON hs.h_segment_hk = ls.h_segment_hk
JOIN dds.h_subcategory       hsc ON hsc.h_subcategory_hk = ls.h_subcategory_hk
JOIN dds.s_l_sale_metrics    sm  ON sm.l_sale_hk = ls.l_sale_hk;

-- Продажи и прибыль по региону и сегменту
CREATE OR REPLACE VIEW cdm.v_sales_profit_by_region_segment AS
SELECT
    region,
    segment,
    SUM(sales)                    AS total_sales,
    SUM(quantity)                 AS total_quantity,
    SUM(profit)                   AS total_profit,
    AVG(discount)                 AS avg_discount,
    SUM(profit) / NULLIF(SUM(sales), 0) AS profit_margin
FROM cdm.v_fact_sales
GROUP BY
    region,
    segment;

-- Влияние способа доставки на прибыль
CREATE OR REPLACE VIEW cdm.v_profit_by_ship_mode AS
SELECT
    ship_mode,
    SUM(sales)                    AS total_sales,
    SUM(profit)                   AS total_profit,
    SUM(quantity)                 AS total_quantity,
    AVG(discount)                 AS avg_discount,
    SUM(profit) / NULLIF(SUM(sales), 0) AS profit_margin
FROM cdm.v_fact_sales
GROUP BY
    ship_mode;

-- Скидки по сегментам и подкатегориям
CREATE OR REPLACE VIEW cdm.v_discount_by_segment_subcategory AS
SELECT
    segment,
    category,
    sub_category,
    AVG(discount) AS avg_discount,
    SUM(sales)    AS total_sales,
    SUM(profit)   AS total_profit
FROM cdm.v_fact_sales
GROUP BY
    segment,
    category,
    sub_category;

-- Продажи по городам внутри региона
CREATE OR REPLACE VIEW cdm.v_sales_by_region_city AS
SELECT
    region,
    country,
    state,
    city,
    SUM(sales)    AS total_sales,
    SUM(quantity) AS total_quantity,
    SUM(profit)   AS total_profit
FROM cdm.v_fact_sales
GROUP BY
    region,
    country,
    state,
    city;



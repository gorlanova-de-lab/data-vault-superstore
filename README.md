# Data Vault DWH Superstore (PostgreSQL)

Учебный проект хранилища данных по методологии **Data Vault 2.0** на основе датасета продаж супермаркета (Kaggle, Retail Supermarket ) 
Показывает полный путь: STG → DDS (Hubs/Links/Satellites) → CDM (витрины), с акцентом на архитектуру и SQL‑реализацию.

---

## Архитектура

Проект реализован в PostgreSQL и разделён на три слоя:

- **STG** – сырые данные из CSV + техполя загрузки.  
- **DDS (Data Vault)** – модель Hub–Link–Satellite с hash‑ключами.  
- **CDM (витрины)** – плоские представления (звёздные схемы) для аналитики.

Основные файлы:

- `SampleSuperstore.csv` – исходный датасет.  
- `dwh_superstore.sql` – полный SQL: создание схем, таблиц, загрузка и витрины.

---

## Слой STG

Источник – датасет Superstore с полями: Ship Mode, Segment, Country, City, State, Postal Code, Region, Category, Sub-Category, Sales, Quantity, Discount, Profit.

Особенности STG:

- Таблица `stg.superstore` повторяет структуру CSV.  
- Добавлены техполя:
  - `load_dts` – время загрузки в STG.  
  - `record_source` – идентификатор источника (`kaggle_superstore_csv`).

Загрузка выполняется через DBeaver (Import Data) или `COPY` (см. `dwh_superstore.sql`).

---

## Слой DDS (Data Vault)

В DDS реализованы:

### Хабы (h_)

Три бизнес‑сущности:

- `dds.h_city` – география, бизнес‑ключ `(country, state, city)`.  
- `dds.h_segment` – сегмент клиента (`segment`).  
- `dds.h_subcategory` – товарная подкатегория, бизнес‑ключ `(category, sub_category)`.

Во всех хабах:

- Hash‑ключи (`h_*_hk`) рассчитываются через `pgcrypto.digest(..., 'md5')`.  
- Есть техполя `load_dts`, `record_source`.

### Линк (l_)

Факт продаж моделируется как связка трёх хабов:

- `dds.l_sale`:
  - `l_sale_hk` – hash‑ключ линка.  
  - `h_city_hk`, `h_segment_hk`, `h_subcategory_hk` – ссылки на хабы.  
  - `load_dts`, `record_source`.

Hash‑ключ линка считается уже **из hash‑ключей хабов**, а сами `*_hk` берутся из таблиц хабов, а не пересчитываются.

### Сателлиты (s_)

Используются два основных satellite:

- `dds.s_l_sale_metrics` – метрики продажи:
  - `sales`, `quantity`, `discount`, `profit`, `ship_mode`.  
  - Ключ: `(l_sale_hk, load_dts)`.  
  - `hashdiff` для набора метрик.

- `dds.s_h_city_attrs` – атрибуты города:
  - `region`, `postal_code`.  
  - Ключ: `(h_city_hk, load_dts)`.  
  - `hashdiff` для набора атрибутов.

DDL и INSERT для всех h_/l_/s_ находятся в `dwh_superstore.sql`.

---

## Слой CDM (витрины)

CDM построен поверх DDS, базовый факт – представление `cdm.v_fact_sales`, объединяющее:

- `dds.l_sale` (факт‑связка).  
- `dds.h_city`, `dds.h_segment`, `dds.h_subcategory`.  
- `dds.s_l_sale_metrics`, `dds.s_h_city_attrs`.

Поверх `v_fact_sales` реализованы витрины (примерный набор):

1. **Продажи и прибыль по региону и сегменту** – `cdm.v_sales_profit_by_region_segment`  
   - total_sales, total_profit, total_quantity, avg_discount, profit_margin.

2. **Влияние способа доставки на маржинальность** – `cdm.v_profit_by_ship_mode`  
   - сравнение Ship Mode по выручке, прибыли и скидкам.

3. **Скидки по сегментам и подкатегориям** – `cdm.v_discount_by_segment_subcategory`  
   - где даются наибольшие скидки и как это влияет на прибыль.

4. **Продажи по городам внутри региона** – `cdm.v_sales_by_region_city`  
   - рейтинг городов по продажам и прибыли.

Полные определения `VIEW` см. в `dwh_superstore.sql`.

---

## Как запустить

1. Развернуть PostgreSQL (например, через Docker).  
2. Создать базу данных (например, `dv_demo`).  
3. Выполнить `dwh_superstore.sql` по шагам:
   - создание схем `stg`, `dds`, `cdm`;  
   - создание и заполнение `stg.superstore`;  
   - загрузка DDS (хабы, линк, сателлиты);  
   - создание витрин `cdm.*`.  
4. Использовать представления `cdm.*` в BI‑инструментах или обычными SQL‑запросами.

---

## Что показывает этот проект

- Умение применять методологию **Data Vault 2.0** на реальном датасете.[web:123][web:161]  
- Построение хабов, линков и сателлитов с hash‑ключами и hashdiff.  
- Проектирование витрин CDM под реальные бизнес‑вопросы (продажи, прибыль, скидки, география, сегменты, доставка).
- Практика с PostgreSQL, Docker, WSL, DBeaver и GitHub как частью data engineering‑пайплайна.

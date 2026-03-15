# Dashboard -- Looker Studio

## Overview

The project dashboard is built with [Google Looker Studio](https://lookerstudio.google.com/), connected directly to BigQuery mart tables in the `dvf_analytics` dataset. It provides interactive visualizations of French real estate transactions (DVF+ data).

**Shareable URL**: [https://lookerstudio.google.com/reporting/b0b00d24-9d2f-4164-86f2-79e72340f4ac](https://lookerstudio.google.com/reporting/b0b00d24-9d2f-4164-86f2-79e72340f4ac)

## Data Sources

All tiles connect to BigQuery tables in the `dvf_analytics` dataset:

| Table | Description | Key Columns |
|-------|-------------|-------------|
| `fct_transactions` | Fact table -- one row per transaction | transaction_year, department_code, property_type_code, property_type_label, transaction_price_eur, built_area_sqm, price_per_sqm |
| `dim_communes` | Commune dimension | commune_code, commune_name, department_code |
| `dim_property_types` | Property type dimension | property_type_code, property_type_label, property_type_level1_label |
| `dim_geography` | Geographic boundaries | geo_code, geo_name, geo_level, department_code, geometry |
| `dim_dates` | Date dimension | date_key, full_date, year, quarter, month, month_name |

## Dashboard Tiles

### Page 1: Overview

**Tile 1: Transaction Count by Property Type (Bar Chart)**

- **Chart type**: Bar chart
- **Dimension**: `property_type_label`
- **Metric**: `Record Count`
- **Sort**: Descending by metric
- **Insight**: Distribution of transactions across property types

**Tile 2: Total Transaction Value by Property Type (Bar Chart)**

- **Chart type**: Bar chart
- **Dimension**: `property_type_label`
- **Metric**: `SUM(transaction_price_eur)`
- **Sort**: Descending by metric
- **Insight**: Total market value per property type

### Page 2: Trends

**Tile 3: Average Price Evolution by Year (Line Chart)**

- **Chart type**: Line chart
- **Dimension**: `transaction_year`
- **Metric**: `AVG(transaction_price_eur)`
- **Sort**: Ascending by year
- **Insight**: Reveals price trends in the real estate market (2014-2024)

**Tile 4: Transaction Volume by Year (Line Chart)**

- **Chart type**: Line chart
- **Dimension**: `transaction_year`
- **Metric**: `Record Count`
- **Sort**: Ascending by year
- **Insight**: Shows market activity trends, COVID-19 impact (2020 dip), and recovery patterns

### Interactive Filters (both pages)

**Year filter**:
- **Type**: Drop-down list
- **Field**: `transaction_year`
- **Applies to**: All tiles on the page

**Property type filter**:
- **Type**: Drop-down list
- **Field**: `property_type_label`
- **Applies to**: All tiles on the page

## Setup Instructions

### Step 1: Open Looker Studio

1. Go to [lookerstudio.google.com](https://lookerstudio.google.com/)
2. Sign in with the Google account that has access to the `valeurs-foncieres-analytics` BigQuery project
3. Click **Create** > **Report**

### Step 2: Add BigQuery Data Source

1. In the data source picker, select **BigQuery**
2. Select project: `valeurs-foncieres-analytics`
3. Select dataset: `dvf_analytics`
4. Select table: `fct_transactions`
5. Click **Add**
6. Repeat for any additional tables if needed (dimension tables for joins)

### Step 3: Create Tile 1 (Transaction Count by Property Type)

1. Click **Add a chart** > **Bar chart** (horizontal)
2. Set dimension to `property_type_label`
3. Set metric to `Record Count`
4. Set sort to metric descending
5. Add a title: "Transaction Count by Property Type"

### Step 4: Create Tile 2 (Transaction Volume by Year)

1. Click **Add a chart** > **Time series** or **Line chart**
2. Set dimension to `transaction_year`
3. Set metric to `Record Count`
4. Optionally add a second metric: `AVG(transaction_price_eur)`
5. Set sort to dimension ascending
6. Add a title: "Transaction Volume by Year"

### Step 5: Create Tile 3 (Median Price per m2 by Department)

1. Click **Add a chart** > **Bar chart** (horizontal) or **Google Maps geo chart**
2. Set dimension to `department_code`
3. Set metric to `AVG(price_per_sqm)`
4. Add a filter: `price_per_sqm IS NOT NULL`
5. Set sort to metric descending
6. Add a title: "Average Price per m2 by Department"

### Step 6: Add Filter Control

1. Click **Add a control** > **Drop-down list**
2. Set control field to `department_code`
3. Position it at the top of the report

### Step 7: Add Dashboard Title and Metadata

1. Add a text box with title: "French Real Estate Transactions (DVF+)"
2. Add subtitle: "Source: DVF+ Open Data (Cerema) | Data: 2014-2025"
3. Add the project name and data refresh date

### Step 8: Share the Dashboard

1. Click **File** > **Share**
2. Click **Manage access**
3. Change to **Anyone with the link can view**
4. Copy the shareable URL
5. Paste the URL in this document and in `README.md`

## Validation Queries

Use these queries to verify that dashboard tiles match the raw data:

```sql
-- Tile 1 validation: Transaction count by property type
SELECT
    property_type_label,
    COUNT(*) AS transaction_count
FROM `valeurs-foncieres-analytics.dvf_analytics.fct_transactions`
GROUP BY property_type_label
ORDER BY transaction_count DESC;

-- Tile 2 validation: Transaction volume by year
SELECT
    transaction_year,
    COUNT(*) AS transaction_count,
    AVG(transaction_price_eur) AS avg_price_eur
FROM `valeurs-foncieres-analytics.dvf_analytics.fct_transactions`
GROUP BY transaction_year
ORDER BY transaction_year;

-- Tile 3 validation: Average price per sqm by department
SELECT
    department_code,
    AVG(price_per_sqm) AS avg_price_per_sqm,
    COUNT(*) AS transaction_count
FROM `valeurs-foncieres-analytics.dvf_analytics.fct_transactions`
WHERE price_per_sqm IS NOT NULL
GROUP BY department_code
ORDER BY avg_price_per_sqm DESC;
```

## Screenshot

The live dashboard is accessible at the shareable URL above. A static screenshot has not been added to the repository to keep the repo lightweight. To capture one, open the dashboard URL and use your browser's screenshot feature.

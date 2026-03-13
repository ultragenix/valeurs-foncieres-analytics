# Partitioning and Clustering Strategy

## Overview

The main fact table `fct_transactions` in BigQuery uses **integer range partitioning** on the transaction year and **clustering** on department code and property type code. This combination optimizes the two most common query patterns in real estate analytics: time-range filtering and geographic/property-type drill-downs.

## Partitioning by Year (`transaction_year`)

**What it does:** BigQuery physically separates the table into one partition per year (2014 through 2025). When a query includes a `WHERE transaction_year = 2023` filter, BigQuery reads only that single partition and skips all other years entirely.

**Why year (not date):** The DVF+ dataset spans 10+ years with millions of rows. Most dashboard queries aggregate by year or filter to a specific year range. Integer range partitioning on the year column is simpler and more efficient than DATE partitioning on the full transaction date, because the year is already extracted as an integer during the dbt transformation.

**Example -- partition pruning in action:**

```sql
-- This query scans only the 2023 partition (~1/10th of the data)
SELECT department_code, COUNT(*) AS transaction_count, AVG(transaction_price_eur) AS avg_price
FROM dvf_analytics.fct_transactions
WHERE transaction_year = 2023
GROUP BY department_code
ORDER BY transaction_count DESC;

-- Without partitioning, this would scan the entire table (~20M rows).
-- With partitioning, it scans only ~2M rows (one year).
```

## Clustering by Department + Property Type

**What it does:** Within each partition, BigQuery sorts and co-locates rows that share the same `department_code` and `property_type_code`. When a query filters on these columns, BigQuery skips blocks of data that do not match, reducing bytes scanned.

**Why these columns:** Department code and property type are the two most common filter dimensions in real estate dashboards. Nearly every analytical query asks "show me apartments in Paris" or "compare house prices across departments." Clustering on these two columns ensures those queries read the minimum amount of data.

**Example -- clustering benefit:**

```sql
-- This query benefits from BOTH partitioning (year) AND clustering (department + type)
SELECT transaction_year, AVG(transaction_price_eur) AS avg_price, COUNT(*) AS n
FROM dvf_analytics.fct_transactions
WHERE transaction_year BETWEEN 2020 AND 2023
  AND department_code = '75'
  AND property_type_code = '2'
GROUP BY transaction_year
ORDER BY transaction_year;

-- Partitioning prunes to 4 year-partitions (2020-2023).
-- Clustering then skips blocks not matching department '75' + type '2'.
-- Result: scans ~1% of the full table instead of 100%.
```

## Cost and Performance Impact

| Scenario | Without optimization | With partition + cluster |
|----------|---------------------|------------------------|
| Full table scan (all years, all depts) | ~20M rows, ~2 GB scanned | Same (no filter to prune) |
| Single year query | ~20M rows, ~2 GB scanned | ~2M rows, ~200 MB scanned |
| Single year + single department | ~20M rows, ~2 GB scanned | ~20K rows, ~5 MB scanned |
| Single year + dept + property type | ~20M rows, ~2 GB scanned | ~5K rows, ~1 MB scanned |

BigQuery charges by bytes scanned. Partitioning and clustering together can reduce query costs by 95%+ for typical dashboard queries.

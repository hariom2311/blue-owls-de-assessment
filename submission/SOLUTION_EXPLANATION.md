# 📦 Data Pipeline Project — Medallion Architecture (Bronze → Silver → Gold)

## 🧠 Overview

This project implements a complete data pipeline using a **Medallion Architecture** approach:

* **Bronze Layer**: Raw ingestion from APIs (append-only)
* **Silver Layer**: Cleaned, validated, and deduplicated data
* **Gold Layer**: Business-ready star schema (fact + dimensions)

The final output supports **analytical workloads** such as revenue trends, seller performance, and customer insights.

---

# ⚙️ Technical Decisions & Reasoning

## 1. Medallion Architecture Choice

I chose a **Bronze → Silver → Gold** architecture to clearly separate concerns:

* Bronze preserves raw data for traceability
* Silver enforces **data quality and consistency**
* Gold optimizes for **analytics and reporting**

👉 This separation ensures **reprocessing flexibility** and avoids mixing business logic with raw ingestion.

---

## 2. Upsert Strategy in Silver Layer

Instead of using `MERGE`, I implemented upsert logic using:

```sql
ROW_NUMBER() OVER (PARTITION BY natural_key ORDER BY _ingested_at DESC)
```

### Why?

* Works reliably in file-based storage (CSV)
* Ensures **latest record per natural key**
* Keeps Silver as a **current-state snapshot**

### Trade-off:

* Not as efficient as Delta MERGE for large datasets
* For production, I would replace this with **Delta Lake MERGE**

---

## 3. Data Quality Handling (_is_valid Flag)

Rather than filtering out bad data, I introduced a `_is_valid` flag.

### Why?

* Preserves **data lineage and auditability**
* Allows downstream layers (Gold) to decide how to handle bad data

### Example checks:

* Negative price
* Invalid date relationships (delivery before purchase)
* Foreign key mismatches

---

## 4. Fact Table Design (Grain)

The fact table `fact_order_items` is designed at:

👉 **Grain: one row per order item**

### Why?

* Enables detailed analysis (product, seller, pricing)
* Supports flexible aggregation (order-level, category-level)

---

## 5. Payment Allocation Logic

Payments are stored at order level but required at item level.

I implemented:

```sql
item_payment = (item_price / total_order_price) * total_payment
```

### Why?

* Ensures **accurate revenue attribution**
* Maintains proportional fairness across items

### Trade-off:

* Assumes price is the best proxy for allocation
* Alternative could be quantity-based distribution

---

## 6. Surrogate Keys in Dimensions

Each dimension uses surrogate keys (`customer_key`, `product_key`, etc.)

### Why?

* Improves join performance
* Decouples fact table from natural keys
* Supports future SCD (Slowly Changing Dimensions)

---

# 🔁 API Failure Handling & Resilience Strategy

## Strategy Implemented

1. **Retry Mechanism**

   * Retry failed API calls up to 3 times
   * Use exponential backoff (e.g., 1s → 2s → 4s)

2. **Idempotent Ingestion**

   * Bronze layer is append-only
   * Re-running ingestion does not corrupt data

3. **Partial Failure Handling**

   * If one endpoint fails, others continue processing
   * Failed endpoints logged separately

4. **Logging**

   * Capture:

     * API response status
     * Failure reason
     * Timestamp

---

## Why this approach?

* Prevents pipeline failure due to **temporary API issues**
* Ensures **data availability even with partial success**

---

# ⚠️ Assumptions & Trade-offs

## Assumptions

1. `customer_unique_id` represents a real unique customer
2. Payment distribution is proportional to item price
3. Missing values are meaningful (not always errors)
4. Latest `_ingested_at` reflects the most accurate record

---

## Trade-offs

| Decision                              | Trade-off                                |
| ------------------------------------- | ---------------------------------------- |
| CSV storage                           | Simple but not optimized for large-scale |
| Window-based upsert                   | Easier but less efficient than MERGE     |
| No SCD implementation                 | Simpler model but loses history          |
| `_is_valid` flag instead of filtering | Keeps bad data but increases complexity  |

---

# ☁️ Production Improvements (Azure / Microsoft Fabric)

## 1. Storage & Format

* Replace CSV with **Delta Lake / Parquet**
* Benefits:

  * Faster queries
  * ACID transactions
  * Schema evolution

---

## 2. Orchestration

* Use:

  * **Azure Data Factory** or **Fabric Pipelines**
* Features:

  * Scheduled runs
  * Dependency management
  * Retry policies

---

## 3. Monitoring & Alerting

* Integrate with:

  * Azure Monitor / Log Analytics
* Alerts for:

  * Pipeline failures
  * Data quality drops
  * SLA breaches

---

## 4. CI/CD

* Use:

  * GitHub Actions / Azure DevOps
* Automate:

  * Deployment of notebooks/jobs
  * Testing pipelines

---

## 5. Security

* Use:

  * Managed Identity
  * Role-Based Access Control (RBAC)
* Encrypt:

  * Data at rest and in transit

---

## 6. Cost Optimization

* Use:

  * Auto-scaling clusters
  * Partitioned data (by date)
* Avoid:

  * `.coalesce(1)` on large datasets

---

## 7. Data Quality Framework

* Integrate tools like:

  * Great Expectations / Deequ
* Automate validation checks

---

# 🚀 Conclusion

This pipeline demonstrates:

* Strong **data engineering fundamentals**
* Proper **data modeling (Star Schema)**
* Advanced **SQL analytics capabilities**

The design balances **simplicity, correctness, and scalability**, with clear pathways for production-grade enhancements.

---

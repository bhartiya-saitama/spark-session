# Spark Hands-On Session

Runs on **Databricks Free Edition** (serverless compute + Unity Catalog).
No RDDs, no `sparkContext`, no cluster configuration — serverless blocks those.

```
data_generator/generate_data.py   synthetic dataset generator (stdlib only)
data/                             the five generated files, ~5 MB
notebooks/01_spark_hands_on.py    exercises blank
notebooks/01_spark_hands_on_SOLVED.py   solved copy
```

---

## For attendees — pre-work (10 minutes, do this before the session)

1. Create a free Databricks account at **https://www.databricks.com/learn/free-edition**.
2. In the workspace sidebar: **Workspace → Create → Git folder**, paste this
   repo's HTTPS URL, and clone it.
3. Open `notebooks/01_spark_hands_on`, attach it to **Serverless**, and run
   **Cell 0** only. It creates a volume and copies the five data files into it.
4. You should see five files listed. If you do, you're done — close it.

If Cell 0 fails, let the facilitator know.

You need working Python and basic SQL. Nothing else.

---

## About the data

Synthetic retail e-commerce: customers, products, orders, order items, and a
clickstream event log.

| File | Rows | Format |
|---|---|---|
| `customers.csv.gz` | 5,000 | CSV |
| `products.csv.gz` | 500 | CSV |
| `orders.csv.gz` | 60,500 | CSV |
| `order_items.csv.gz` | 150,000 | CSV |
| `events.jsonl.gz` | 80,000 | JSON Lines |

**The data is deliberately dirty.** Every defect sets up a specific teaching
moment, so please don't "fix" the generator:

| Defect | File | Teaches |
|---|---|---|
| 500 exact duplicate rows | orders | `dropDuplicates` |
| day-first timestamps (`14-06-2024`) | orders | inference can't type it; naive `to_timestamp` returns nulls, not errors |
| `unit_price` sometimes the text `N/A` | order_items | one bad token makes `inferSchema` pick StringType |
| 1% negative quantities | order_items | filter before you aggregate |
| 191 product_ids missing from the master | order_items | inner joins silently drop rows |
| mixed-case country codes | customers | 10 countries, 31 groups |
| 3% null city, 1% null country | customers | nulls are normal; `groupBy` keeps them |
| Electronics ≈ 38% of rows | order_items | a real skew shape to point at |

The dataset is small on purpose. The session teaches the API and the execution
model, not scale.

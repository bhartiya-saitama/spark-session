# Spark Hands-On Session

Runs on **Databricks Free Edition** (serverless compute + Unity Catalog).
No RDDs, no `sparkContext`, no cluster configuration — serverless blocks those.

```
data_generator/generate_data.py   synthetic dataset generator (stdlib only)
data/                             the five generated files, ~5 MB
notebooks/01_spark_hands_on.py    attendee notebook (exercises blank)
notebooks/01_spark_hands_on_SOLVED.py   facilitator copy
facilitator/run_of_show.md        minute-by-minute plan
facilitator/talking_points.md     what to say, per section
```

---

## For attendees — pre-work (10 minutes, do this before the session)

1. Create a free Databricks account at **https://www.databricks.com/learn/free-edition**.
2. In the workspace sidebar: **Workspace → Create → Git folder**, paste this
   repo's HTTPS URL, and clone it.
3. Open `notebooks/01_spark_hands_on`, attach it to **Serverless**, and run
   **Cell 0** only. It creates a volume and copies the five data files into it.
4. You should see five files listed. If you do, you're done — close it.

If Cell 0 fails, message the facilitator before the session, not during it.

You need working Python and basic SQL. Nothing else.

---

## For the facilitator

### One-time setup

```bash
python3 data_generator/generate_data.py    # regenerates data/, ~5 MB, deterministic
```

The generator is seeded (`SEED = 42`), so the row counts quoted throughout
`facilitator/talking_points.md` stay exactly correct as long as you don't change
the constants at the top of the script.

Then, **before sharing the repo**:

1. Push it to a **public** GitHub repo (attendees clone it from Databricks).
2. Set `GITHUB_RAW_BASE` in **both** notebooks to your repo's raw URL. Cell 0
   prefers the local `data/` folder from the Git clone and only falls back to
   this URL — but the fallback is what saves you when someone imports the
   notebook file on its own instead of cloning.
3. Dry-run `01_spark_hands_on_SOLVED.py` end to end on Free Edition with a
   stopwatch. Solo execution should land near 20 minutes; attendees typing take
   roughly double. Over 20 minutes solo means something has to become a demo.

### On the day

Follow `facilitator/run_of_show.md`. Keep the SOLVED notebook open in a second
tab. The hard pace checkpoint is **leaving Section 3 by minute 34**.

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

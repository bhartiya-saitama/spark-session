# Talking Points

Audience: junior data and software engineers, fresh CSE grads. They know Python
and SQL. They have not seen a distributed engine. Breadth over depth — the goal
is that they can open a Spark notebook next week and not be lost.

Format per section: **Hook** (one line you say), **Run**, **Expected output**,
**They will get this wrong** (and your recovery line).

---

## Opening the notebook (min 15)

> "Everything you're about to write is the same code that runs over a hundred
> terabytes at a client. Our dataset is 5 MB, so nothing here is slow — that's
> deliberate. Today is about the API and the mental model, not the scale.
> The scale changes one thing: the cost of being sloppy."

---

## Section 1 — Read & inspect (min 16–23)

**Hook:** "Three ways to read one CSV. They give you three different answers."

**Run 1a** — no options.
Expected: every column `string`, including `order_id` and `currency`.
> "Spark doesn't guess unless you ask. This is the honest default."

**Run 1b** — `inferSchema=True`.
Expected: still all strings for our file, and `order_ts` stays a string.
> "Two things happened. One: Spark read the entire file an extra time just to
> guess. On a terabyte, you paid for a full scan to learn something you already
> knew. Two: it still got `order_ts` wrong — `14-06-2024` is day-first, and
> inference doesn't recognise that. Inference is a convenience for exploring, not
> a production choice."

**Run 1c** — explicit `StructType`.
> "One pass, correct types, and the schema is now documented in code. If the
> upstream team adds a column tomorrow, your job doesn't silently change shape."

**YOU DO 1** (3 min): explicit schema for `order_items`, count null `unit_price`.

Expected output:
```
total rows      : 150000
null unit_price : 2994
```
and `inferSchema` types `unit_price` as **string**.

> "About 2% of the rows have the literal text `N/A` in a price column. One bad
> token, and inference downgrades the whole column to string. With an explicit
> DoubleType, Spark parks those rows as null instead — which is what you want,
> because now you can count them."

**They will get this wrong:** forgetting `header=True`, so the header row becomes
data and every count is off by one. Recovery: "check your first row — is it your
column names sitting in the data?"

---

## Section 2 — Transform (min 23–28)

**Hook:** "This is 80% of every Spark job you will ever write: filter, add a
column, drop the junk."

**Run 2a** — duplicates.
Expected: `60500` → `60000`.
> "Five hundred exact duplicate rows. Nobody told you. If you'd reported revenue
> off this file you'd have been about 0.8% high, which is exactly the kind of
> wrong that survives review."

**Run 2b** — timestamp parsing.
Expected: `naive_parse` is null, `correct_parse` is a real timestamp.
> "This is the single most common silent bug in data engineering. You didn't get
> an error. You got nulls. Then your monthly aggregation quietly drops every row.
> Always pass the format string."

**YOU DO 2** (3 min): clean `items_raw`, add `line_total`.

Expected output:
```
before: 150000
after : 145553
```
> "We dropped 4,447 rows — negative quantities are returns, not sales, and null
> prices can't be multiplied. In real work you don't delete those, you route them
> to a quarantine table. But you never let them into the sum."

---

## Section 3 — Aggregate (min 28–34)

**Hook:** "groupBy is where Spark starts doing something a laptop can't."

**Run 3a** — `groupBy("country")` on raw customers.
Expected: **31 groups** — `IN`, `In`, `in`, `US`, `Us`, `us`, ... plus a null.
> "We sell in ten countries. Spark just gave me thirty-one. It did exactly what I
> asked and nothing warned me. This is the whole job: the engine is correct, the
> data is dirty, and the gap between them is where you live."

**Run 3b** — `upper(trim(...))` and `na.fill`.
Expected: 10 countries + one null group (55 customers with no country).
> "Note the null didn't disappear — `groupBy` keeps nulls as their own group.
> That's usually what you want. Decide explicitly; don't discover it later."

**YOU DO 3** (3 min): top 10 products by revenue.

> "Four aggregations in one pass — `sum`, `countDistinct`, `avg`. Spark reads the
> data once and computes all of them. If you wrote this as four separate queries,
> you'd read four times. This is why you learn `agg()`."

Mention in passing: `countDistinct` is expensive (it needs a shuffle of every
distinct value); `approx_count_distinct` exists for when "close enough" is fine.

**Also point at the skew:** Electronics is ~38% of rows.
> "Remember this shape. When one key holds most of the rows, one worker does most
> of the work and the rest idle. That's called skew, and it's the number one
> reason a Spark job that 'should be fast' takes four hours. It's in the appendix."

---

## Section 4 — Join (min 34–40)

**Hook:** "Joins are where Spark gets expensive and where your numbers go wrong."

**Run 4a** — inner vs left.
Expected:
```
inner: 145368
left : 145553
lost : 185
```
> "An inner join is a filter you didn't know you wrote. 185 line items reference
> a product that isn't in our product master — a deleted SKU, or a bad export.
> Inner join deletes them and says nothing."

**Run 4b** — `left_anti`.
> "`left_anti` gives you exactly the rows that didn't match. This is your
> first-response tool every time a row count looks wrong. Learn it now."

**YOU DO 4** (3 min): build `sales`.

> "Notice I select columns *before* joining. Narrow early. Every extra column
> gets shuffled across the network on a join, and shuffle is the expensive part."

**Run 4c** — `broadcast(products).explain()`.
Look for `BroadcastHashJoin` in the plan.
> "Default join: both sides get shuffled across the network by key. But
> `products` is 500 rows. Instead of moving both tables, ship the small one to
> every worker and join locally. That's a broadcast join, and it's the one
> performance trick worth knowing on day one. Rule of thumb: if one side fits
> comfortably in memory — say under a few hundred MB — broadcast it. Spark often
> does this automatically, but the hint makes it explicit."

---

## Section 5 — Spark SQL + windows (min 40–46)

**Hook:** "Everything you just wrote in Python, you can write in SQL. Same
engine, same plan, same speed. Pick whichever is easier to read."

**Run** the `%sql` aggregation next to the DataFrame version.
> "Identical result. There is no performance argument between DataFrame API and
> SQL — they both compile down to the same Catalyst plan. Use SQL for set logic,
> use Python when you need loops, functions, or tests around it."

**YOU DO 5** (3 min): top 3 per category with `ROW_NUMBER()`.

> "A `GROUP BY` collapses rows. A window function ranks rows *inside* a group and
> keeps them all. 'Top N per something' is the most common analytics question
> there is — customer's last order, product's best month, per-region ranking. If
> you learn one advanced SQL thing today, learn windows."

**Run 5b** — JSON with nested `device`.
> "You wrote no schema. Spark sampled the JSON and built the struct itself, and
> you reach in with dot notation: `device.os`. This is what 'semi-structured'
> means in practice — clickstream and API logs land like this constantly."

---

## Section 6 — Lazy evaluation & plans (min 46–50, DEMO)

**Hook:** "Now the one idea that makes Spark different from pandas. Watch the
clock, not the code."

**Run** the chain builder.
Expected: building the chain ≈ **0.0 seconds**. Then `.count()` takes real time.
> "Five operations on 145,000 rows in a hundredth of a second. It didn't do the
> work. Transformations are lazy — `filter`, `select`, `join`, `groupBy` just
> record your intent. Only an *action* triggers execution: `count`, `collect`,
> `show`, `write`. The rule: if it returns a DataFrame it's lazy, if it returns a
> number, rows, or a file, it ran."

**Run** `.explain()`.
> "Read it bottom-up. Notice the filters ended up down at the file scan, not
> after the join. Spark rewrote your code. It could only do that because it
> waited to see the whole recipe before cooking. That's what laziness buys you."

**Run** the cache comparison.
> "Second run: it did the whole thing again from the files. Spark keeps nothing
> by default. `cache()` says keep the result in memory. Cache when you reuse a
> DataFrame three or more times — not by reflex, because cache costs memory that
> your shuffles want."

**`collect()` warning** — say it out loud:
> "`collect()` pulls every row into the driver, one machine. On a real table
> that's an out-of-memory crash and an angry Slack message. `display()`,
> `show()`, or `.limit(n).collect()`. Never bare `collect()`."

---

## Section 7 — Write it out (min 50–53, DEMO)

**Run** the Delta write, the `SELECT`, and `DESCRIBE HISTORY`.
> "We started with gzipped CSV and we're ending with a Delta table. Why bother?
> CSV is row-oriented, untyped, and unsplittable when compressed — Spark must
> read every byte of every column. Parquet is columnar and typed, so asking for
> two columns reads two columns. Delta is Parquet plus a transaction log, which
> gives you ACID, schema enforcement, and time travel."

Point at `DESCRIBE HISTORY`.
> "Every write is a version. You can query the table as it was before this
> morning's bad job. Do that once in production and you'll never go back."

---

## Where we actually use Spark (min 53–57)

Tie each one back to a cell they just ran.

- **Nightly ingestion into a lakehouse** (Sections 1 + 7). Source systems drop
  extracts; a scheduled job types them, cleans them, and lands Delta tables.
  This is the single most common Spark job in existence.
- **Customer 360 / segmentation** (Section 4). Joining CRM, transactions, and
  web events on keys that don't quite agree. The `left_anti` reconciliation you
  just did is genuinely a billable activity.
- **Pricing and margin analytics** (Sections 3 + 5). Years of line items,
  aggregated by product, region, and month, feeding a pricing model.
- **Feature engineering for ML** (Section 5). Windows over customer history —
  orders in last 30 days, days since last purchase — then handed to the model.
- **Migration off legacy ETL.** Informatica/SAS/stored procedures rewritten as
  Spark. Unglamorous, extremely common, and how most juniors first meet Spark.

**Then be honest — this earns you credibility:**
> "Spark is not always the answer. Under about 10 GB, pandas, Polars or DuckDB
> will beat it, on one machine, with less operational overhead. Spark earns its
> keep when the data doesn't fit on one box, when you're joining many large
> sources, or when you need a scheduled, fault-tolerant, governed pipeline.
> Choosing Spark for a 200 MB CSV is a mistake I've seen more than once."

## Pitfalls to name out loud

1. `collect()` on a big DataFrame — kills the driver.
2. `inferSchema` in production — extra scan, and it will change type on you when
   the data changes.
3. Tiny files — ten thousand 4 KB Parquet files is slower than one 400 MB file.
4. UDFs when a built-in exists — a Python UDF breaks Spark's optimiser and
   serialises row by row. Check `pyspark.sql.functions` first, always.
5. `count()` inside a loop — each one re-runs the whole plan. Cache or restructure.
6. Assuming row order — without `orderBy`, there is no order. Ever.

## Jump-off (min 57–60)

- The appendix in the notebook: partitions, shuffle, skew, Delta MERGE, streaming.
- Spark SQL function reference — https://spark.apache.org/docs/latest/api/sql/
- *Learning Spark, 2nd Edition* — free PDF from Databricks
- Databricks Free Edition — keep this workspace, it doesn't expire
- Delta Lake docs — https://docs.delta.io/

Closing line:
> "You now know how to read, clean, join, aggregate, and write data with Spark,
> and you know why it's lazy. That's genuinely most of the job. The rest —
> partitions, shuffle, tuning — you'll learn the first time something is slow,
> and it will make more sense then than it would today."

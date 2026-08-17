# Run of Show — 60 minutes

Notebook occupies minutes 15–53 = **38 min**, inside the 40 min cap.

| Min | Len | Block | Notes |
|---|---|---|---|
| 0–15 | 15 | Slides (existing) | what/why Spark, driver–executor, DataFrame vs RDD, lakehouse |
| 15–16 | 1 | Everyone confirms Cell 0 is green | pre-work; only stragglers re-run |
| 16–23 | 7 | Section 1 — read & inspect | 3 demos + YOU DO 1 |
| 23–28 | 5 | Section 2 — transform | 2 demos + YOU DO 2 |
| 28–34 | 6 | Section 3 — aggregate | dirty-country demo + YOU DO 3 |
| 34–40 | 6 | Section 4 — join | inner-vs-left demo + YOU DO 4 |
| 40–46 | 6 | Section 5 — SQL + window | YOU DO 5 + JSON demo |
| 46–50 | 4 | Section 6 — lazy eval & plans | demo only, don't let them type |
| 50–53 | 3 | Section 7 — write Delta | demo only |
| 53–57 | 4 | "Where we actually use Spark" + pitfalls | see talking_points.md |
| 57–60 | 3 | Appendix pointer, resources, Q&A | |

## Pace checkpoint

**You must be leaving Section 3 by minute 34.** If you are not, cut in this order:

1. Section 7 becomes a slide, not a run (−3 min)
2. Section 5's window exercise becomes your demo instead of their exercise (−3 min)
3. Section 2's exercise is read aloud from the SOLVED notebook (−3 min)

Never cut Section 4 or Section 6. The join and the transformation-vs-action idea
are the two things they must leave with.

## Exercise protocol

For each `YOU DO`: read the instruction aloud (20s), give them the stated time,
then screen-share the SOLVED cell and run it. **Do not wait for everybody.** Say
up front: "if your cell isn't working when I move on, paste mine and keep up —
you can come back after."

## Facilitator prep checklist

- [ ] `python3 data_generator/generate_data.py` and confirm `data/` is ~5 MB
- [ ] Push repo public; set `GITHUB_RAW_BASE` in **both** notebooks to your repo
- [ ] Dry-run `01_spark_hands_on_SOLVED.py` top to bottom on Free Edition, stopwatch on
- [ ] Send attendees the pre-work note (see README) at least 24h ahead
- [ ] Have the SOLVED notebook open in a second tab, scrolled to Section 1

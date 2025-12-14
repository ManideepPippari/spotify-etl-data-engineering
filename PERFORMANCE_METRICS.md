Spotify ETL Pipeline — Performance & Cost Notes

Purpose of this document

This document summarizes observed performance characteristics, cost considerations, and practical lessons learned while building and testing the Spotify ETL pipeline.

The goal is not to present production SLAs, but to document:
	•	how the pipeline behaved during development and testing
	•	where time and cost were spent
	•	what would need to change at larger scale

All cloud resources were deprovisioned after testing to avoid unnecessary cost.


1. Dataset Scope (During Testing)

During development and testing, the pipeline processed:
	•	~1,400 Spotify tracks
	•	Data from 5 major artists
	•	~280 albums

The dataset size was intentionally kept small to:
	•	avoid Spotify API rate-limit issues
	•	keep cloud costs minimal
	•	focus on pipeline design rather than data volume


2. End-to-End Pipeline Timing (Observed)

The following timings were observed during test runs, not enforced SLAs.

| Stage                      | Approx Time   | Notes                    |
| -------------------------- | ------------- | ------------------------ |
| Spotify API ingestion      | ~1 minute     | Lambda execution         |
| Transformation & S3 write  | < 1 minute    | Small CSV outputs        |
| Glue crawler               | ~2–3 minutes  | Schema inference         |
| Athena validation          | Seconds       | Very small scan size     |
| Databricks processing      | ~8–10 minutes | Includes cluster startup |
| Snowflake load & transform | ~3–4 minutes  | XSMALL warehouse         |

Typical end-to-end runtime:
➡ ~18–25 minutes per full run

The longest delay consistently came from Databricks cluster startup, which is expected for small workloads.


3. Component-Level Observations

AWS Lambda
	•	Lambda was well-suited for API ingestion and lightweight transforms
	•	Cold starts were noticeable but acceptable at this scale
	•	Memory configuration was conservative and could be optimized further

AWS Glue & Athena
	•	Glue crawler worked reliably but is overkill for fixed schemas
	•	Athena queries were fast due to very small data volume
	•	For larger datasets, partitioning would become important

Databricks
	•	Spark provided flexibility for medallion-style processing
	•	Cold start time dominated total processing time
	•	For small datasets, Databricks is functionally correct but cost-inefficient

Snowflake
	•	COPY INTO + stored procedures were fast and predictable
	•	Auto-suspend helped keep compute cost minimal
	•	Snowflake worked well as a final curated analytics layer


4. Cost Considerations (Estimated)

Because all infrastructure was deprovisioned after testing, the numbers below are rough estimates, not billing statements.

At the tested scale:
	•	Storage costs were negligible (few MBs)
	•	Athena query costs were effectively zero
	•	Lambda costs were low due to short execution time

If the pipeline were run hourly with the same dataset, the main cost drivers would be:
	•	Glue crawler frequency
	•	Databricks cluster startup time
	•	Snowflake warehouse uptime

For small or medium datasets, simpler architectures would be cheaper.


5. Data Quality Checks Implemented

The project includes lightweight but meaningful data quality checks, such as:
	•	no null track_id
	•	no duplicate tracks across layers
	•	valid duration values (> 0)
	•	consistent row counts between Athena and Snowflake

These checks were intentionally kept simple to avoid:
	•	expensive full-table scans
	•	complex rule engines

The goal was confidence, not perfection.


6. Scalability Thoughts (What Would Change)

If the dataset or frequency increased significantly:

At ~10× scale
	•	Reduce Glue crawler frequency
	•	Use fixed schemas instead of crawling
	•	Increase Lambda memory slightly
	•	Optimize Databricks cluster configuration

At ~100× scale
	•	Replace Lambda ingestion with containerized services
	•	Introduce incremental ingestion instead of full refresh
	•	Partition data by artist and date
	•	Introduce caching for Spotify API calls

These changes were not implemented, but are natural next steps.


7. Lessons Learned

What worked well
	•	Clear separation of ingestion, transformation, and analytics layers
	•	Using CSV as a common interchange format
	•	Keeping transformations portable across environments

What was less ideal
	•	Databricks cold starts for small data
	•	Glue crawler cost relative to dataset size
	•	Complexity compared to actual data volume

What I would change in a real production system
	•	Fewer tools for small workloads
	•	More incremental processing
	•	Stronger monitoring and alerting


8. Final Notes

This project prioritizes:
	•	design clarity
	•	tool integration
	•	realistic trade-offs

The performance characteristics described here reflect development-time behavior, not production guarantees.
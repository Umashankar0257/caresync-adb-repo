
# CareSync — Azure Data Engineering Project
## Azure Databricks Repository

![Azure Databricks](https://img.shields.io/badge/Azure-Databricks-FF3621?logo=databricks)
![PySpark](https://img.shields.io/badge/Apache-PySpark-E25A1C)
![Architecture](https://img.shields.io/badge/Architecture-Medallion-blue)
![Status](https://img.shields.io/badge/Project-In%20Progress-orange)

## 1. Project Overview

CareSync is an end-to-end Azure Data Engineering portfolio project that demonstrates how data can be processed through a lakehouse-style architecture using Azure Data Factory, Azure Data Lake Storage Gen2, Azure Databricks, PySpark, and Delta Lake.

This repository contains Databricks notebooks for processing data through landing, Bronze, Silver, and Gold layers.

**Databricks Repository:**  
https://github.com/Umashankar0257/caresync-adb-repo

**Azure Data Factory Repository:**  
https://github.com/Umashankar0257/caresync_adf_repo

## 2. Project Objective

The objective is to demonstrate a reusable data processing workflow that:

- Reads files from a configured landing location.
- Organizes ingested data in the Bronze layer.
- Applies data transformation and cleansing in Silver.
- Prepares curated data for analytical use in Gold.
- Uses parameters to make notebooks reusable.
- Adds ingestion metadata for tracking data processing.
- Uses Spark for distributed data processing.
- Demonstrates data quality and lakehouse design concepts.

The project uses a healthcare-inspired CareSync scenario. Sample or synthetic data should be used for demonstrations.

## 3. Technology Stack

| Technology | Purpose |
|---|---|
| Azure Databricks | Data engineering and notebook execution |
| Apache Spark | Distributed processing engine |
| PySpark | Data transformation using Python |
| Azure Data Factory | Pipeline orchestration |
| Azure Data Lake Storage Gen2 | Cloud storage |
| Unity Catalog Volumes | Governed access to files |
| Delta Lake | ACID transactions and table management for Delta tables |
| GitHub | Source control and collaboration |

## 4. End-to-End Architecture

```text
Source Systems / Sample Files
             |
             v
Azure Data Factory
             |
             v
Azure Data Lake Storage Gen2
             |
             v
Landing Files
             |
             v
Databricks: Landing_to_Bronze
             |
             v
         BRONZE
   Raw / minimally processed
             |
             v
Databricks: Bronze_to_silver
             |
             v
          SILVER
 Cleansed / validated / standardized
             |
             v
       Gold Processing
             |
             v
           GOLD
 Curated / business-ready datasets
             |
             v
     Reporting and Analytics
```

The architecture illustrates the intended end-to-end flow. The exact sources, transformations, output tables, and orchestration dependencies depend on the notebook and Azure resource configurations.

## 5. Repository Structure

```text
caresync-adb-repo/
│
├── Landing_to_Bronze.ipynb
│
├── Bronze_to_silver.ipynb
│
├── Gold/
│   └── Gold-layer notebooks or related files
│
├── test.ipynb
│
└── README.md
```

### Notebook Responsibilities

**Landing_to_Bronze.ipynb**

Reads the configured Parquet input from the Databricks Volume path and performs the landing-to-Bronze processing defined in the notebook.

**Bronze_to_silver.ipynb**

Contains the Bronze-to-Silver processing stage, where transformations and data quality checks can be applied.

**Gold/**

Contains the Gold-layer processing assets. Review the individual files to understand the business transformations and final outputs.

**test.ipynb**

Used for experiments and testing. Only validated logic should be promoted into the main processing notebooks.

## 6. Landing-to-Bronze Processing

### Step 1 — Receive Parameters

The landing notebook uses widgets to receive its inputs.

Example:

```python
dbutils.widgets.text("source_system", "")
dbutils.widgets.text("table_name", "")
dbutils.widgets.text("landing_timestamp", "")

source_system = dbutils.widgets.get("source_system")
table_name = dbutils.widgets.get("table_name")
landing_timestamp = dbutils.widgets.get("landing_timestamp")
```

**Parameter definitions**

| Parameter | Meaning |
|---|---|
| `source_system` | Identifies the source system or source category |
| `table_name` | Identifies the dataset being processed |
| `landing_timestamp` | Identifies the landing batch or folder |

These parameters help the notebook process different datasets without hard-coding a different notebook for every table.

### Step 2 — Read Parquet Files

The notebook reads Parquet data from a Unity Catalog Volume.

Example:

```python
landing_path = (
    f"/Volumes/adb_caresync/landing/landing_volume/"
    f"{source_system}/{table_name}/{landing_timestamp}/"
)

landing_df = (
    spark.read
         .format("parquet")
         .load(landing_path)
)
```

The path assumes the catalog, schema, Volume, and folder names are configured exactly as shown.

**Why Parquet?**

- Column-oriented storage format.
- Efficient compression.
- Supports schema information.
- Works well with Spark-based processing.

### Step 3 — Add Ingestion Metadata

Ingestion metadata helps identify when a batch was processed.

Example:

```python
from pyspark.sql.functions import current_timestamp

bronze_df = (
    landing_df
    .withColumn("landing_timestamp", current_timestamp())
    .withColumn("insert_timestamp", current_timestamp())
)
```

In this example:

- `landing_timestamp` records the time assigned during this processing run.
- `insert_timestamp` records the processing or insertion time.

**Important:** If `landing_timestamp` must preserve the original folder or batch timestamp supplied by ADF, parse and use the widget value instead of replacing it with the current time.

For example, a string-based folder timestamp can be retained as a separate column:

```python
from pyspark.sql.functions import lit, current_timestamp

bronze_df = (
    landing_df
    .withColumn("landing_timestamp", lit(landing_timestamp))
    .withColumn("insert_timestamp", current_timestamp())
)
```

Use the version that matches the actual timestamp requirements of the project.

### Step 4 — Write to Bronze

The destination should be configured consistently with the project's Bronze storage design.

For a Delta table or path-based Delta destination, a typical write pattern is:

```python
(
    bronze_df.write
    .format("delta")
    .mode("append")
    .save(bronze_path)
)
```

Here, `bronze_path` must be configured before execution.

This is an illustrative write pattern. Confirm the actual write mode and destination in the notebook before describing them as implemented.

**Append versus overwrite**

- `append` adds rows to existing data.
- `overwrite` replaces the targeted output.
- Reprocessing the same batch with append can create duplicate records unless the ingestion design prevents it.

## 7. Bronze Layer

The Bronze layer stores ingested data in a raw or minimally processed form.

### Purpose

- Preserve the incoming dataset.
- Keep source-oriented data for traceability.
- Support downstream transformations.
- Retain ingestion metadata.
- Help investigate data quality issues.

### Typical metadata

```text
source_system
table_name
landing_timestamp
insert_timestamp
```

The actual columns depend on the implemented notebook and source schema.

### Common Bronze checks

- Confirm that input files exist.
- Validate that the schema is readable.
- Capture the batch or processing timestamp.
- Track row counts.
- Identify empty or invalid inputs.
- Prevent unintended duplicate ingestion.

## 8. Bronze-to-Silver Processing

The Silver layer transforms ingested data into a cleaner, more consistent structure.

A typical workflow is:

1. Read the Bronze dataset.
2. Inspect the schema and data types.
3. Identify null or invalid values.
4. Remove or flag duplicate records according to business rules.
5. Standardize column names and data types.
6. Apply required business rules.
7. Validate the transformed dataset.
8. Write the cleaned output.

Example transformation patterns:

```python
from pyspark.sql.functions import col

silver_df = bronze_df

# Example: remove rows missing a required identifier.
silver_df = silver_df.filter(
    col("record_id").isNotNull()
)

# Example: standardize a column's data type.
silver_df = silver_df.withColumn(
    "record_id",
    col("record_id").cast("string")
)
```

Replace `record_id` with a column that exists in the actual dataset.

### Data Quality Dimensions

| Dimension | Example check |
|---|---|
| Completeness | Required fields are not null |
| Uniqueness | Business keys are not duplicated |
| Validity | Values follow expected formats or rules |
| Consistency | Related fields follow business rules |
| Accuracy | Values match trusted source information |
| Timeliness | Data belongs to the expected processing window |

Invalid records can be rejected, quarantined, or logged according to the design. They should not be silently discarded without an agreed business rule.

## 9. Gold Layer

The Gold layer is intended for business-ready data and analytical consumption.

Typical use cases include:

- Aggregated metrics.
- Summary tables.
- Reporting datasets.
- Business-specific calculations.
- Analytical views across multiple Silver datasets.

Example aggregation pattern:

```python
from pyspark.sql.functions import count

gold_df = (
    silver_df
    .groupBy("status")
    .agg(count("*").alias("record_count"))
)
```

This is a generic example. The actual Gold-layer logic should be documented from the notebooks stored in the `Gold/` directory.

Gold data can be stored as Delta tables or other supported formats, depending on the configured implementation.

## 10. Delta Lake Concepts

Delta Lake can provide a reliable storage layer for lakehouse data when the datasets are written in Delta format.

Important concepts to understand:

- **ACID transactions:** Help maintain transactional consistency for supported Delta operations.
- **Schema enforcement:** Helps prevent incompatible data from being written.
- **Schema evolution:** Allows supported schema changes when explicitly configured.
- **Time travel:** Allows supported historical versions to be queried.
- **MERGE:** Supports insert, update, and delete logic for matching records.
- **UPSERT:** Combines inserting new records and updating existing records.
- **OPTIMIZE:** Improves file layout for supported Delta tables.
- **VACUUM:** Removes eligible old data files according to retention and safety rules.

These are Delta Lake capabilities to explore and use where appropriate. Do not assume every feature is implemented in the current CareSync notebooks.

## 11. Full Load and Incremental Load

**Full load**

Reads and processes the complete dataset for the configured source.

Useful for initial loads or datasets that are small enough to refresh completely.

**Incremental load**

Processes only new or changed records according to a reliable change-detection strategy.

Possible approaches include:

- Source-provided modification timestamps.
- Watermark columns.
- Change Data Capture (CDC).
- Source change logs.
- Comparing incoming records with previously processed data.

A landing timestamp alone does not prove that the source record changed. If source data can be updated without a reliable modification timestamp, another change-detection mechanism is required.

## 12. Idempotency and Rerun Safety

A reliable pipeline should produce the intended result when the same batch is retried.

Possible approaches include:

- Tracking processed batch identifiers.
- Deduplicating using a stable business key.
- Using a deterministic output path.
- Using Delta `MERGE` when matching and update rules are defined.
- Separating ingestion from downstream transformation.
- Recording successful and failed batch states.

An append-only write is not automatically idempotent.

## 13. Integration with Azure Data Factory

ADF and Databricks have different responsibilities.

| Azure Data Factory | Azure Databricks |
|---|---|
| Orchestrates pipeline activities | Executes data processing |
| Manages pipeline parameters | Receives notebook parameters |
| Coordinates data movement | Transforms data using Spark |
| Controls activity dependencies | Implements transformation logic |
| Monitors pipeline execution | Produces processing results and diagnostics |

A typical integration passes values such as `source_system`, `table_name`, and `landing_timestamp` from an orchestration pipeline to a notebook.

The parameter names, notebook path, compute configuration, and storage access must agree between the two services.

## 14. Testing and Validation

Before promoting notebook changes:

- Verify input paths and permissions.
- Confirm expected schemas and data types.
- Check row counts before and after transformations.
- Test null and duplicate handling.
- Validate output paths and formats.
- Confirm timestamp semantics.
- Test reruns and failure scenarios.
- Verify that Gold output matches the intended business logic.

Use non-sensitive test data and avoid committing credentials or real patient information.

## 15. GitHub Development Workflow

The repository can be maintained using feature branches and pull requests.

```text
main
  |
  └── feature/silver-data-quality
             |
             ├── Pull latest main
             ├── Create feature branch
             ├── Modify notebook
             ├── Test changes
             ├── Commit and push
             ├── Open Pull Request
             ├── Manager reviews changes
             └── Merge after approval
```

After the merge, synchronize the local branch before starting the next feature.

GitHub source control does not automatically deploy notebooks to a Databricks workspace unless a deployment workflow has been configured.

## 16. Security Considerations

- Use Unity Catalog permissions where applicable.
- Follow least-privilege access principles.
- Use managed identities or approved service principals for Azure integrations.
- Store secrets in an approved secret-management service.
- Do not commit tokens, passwords, or connection strings.
- Use synthetic data for public demonstrations.
- Avoid exposing sensitive healthcare information in logs or GitHub.

## 17. Current Scope and Future Enhancements

This project is evolving. Possible next steps include:

- Completing and validating Gold-layer transformations.
- Adding reusable data quality functions.
- Implementing reliable incremental processing.
- Adding batch-level audit logging.
- Improving duplicate handling and rerun safety.
- Implementing automated notebook validation.
- Integrating GitHub-based deployment workflows.
- Improving operational monitoring and alerting.

Only list a feature as completed once its implementation has been verified.

## 18. Key Learning Outcomes

- PySpark DataFrame operations.
- Parameterized Databricks notebooks.
- Parquet file ingestion.
- Unity Catalog Volume access.
- Medallion architecture.
- Data quality and cleansing.
- Delta Lake storage concepts.
- ADF-to-Databricks orchestration.
- Incremental processing and rerun safety.
- GitHub version control and collaboration.

## 19. Author

**Umashankar Reddy M**

Azure Data Engineering | Azure Data Factory | Azure Databricks | PySpark | SQL | Delta Lake

---

**Disclaimer:** CareSync is a learning and portfolio project. Validate all transformations, permissions, and deployment configurations before using the design with production or sensitive data.

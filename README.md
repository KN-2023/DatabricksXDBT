## DatabricksXDBT

### Architecture
<img width="450" height="250" alt="Modern ETL Data Pipeline Diagram" src="https://github.com/user-attachments/assets/7a39e3f1-2ecb-4bf6-8cdc-30b7a6a502b0" />

- **Data Ingestion:** Databricks Auto Loader ingests multiple CSV sources into Bronze Delta tables, with schema tracking and checkpoint management.
- **Silver Processing:** Delta Live Tables (DLT) performs streaming transformations, data quality checks, and CDC-based SCD Type 1 processing.
- **Data Integration:** PySpark consolidates booking data with passenger, flight, and airport records to create the Silver layer.
- **Dimension Processing:** Parameterized PySpark workflows automate dimension processing, surrogate key generation, and Delta MERGE operations.
- **Fact Processing:** Metadata-driven SQL dynamically joins dimension tables, resolves surrogate keys, and maintains the `FactBookings` table through MERGE operations.
- **dbt Integration:** dbt is configured to connect to Databricks SQL Warehouse.

### Data Ingestion – Bronze Layer
<img width="550" height="150" alt="notebook_cell_hires" src="https://github.com/user-attachments/assets/ec03ab18-6ca8-4f51-b5b1-00bfa2ab86bf" />
<img width="700" height="300" alt="job_run_ingestion_clear (1)" src="https://github.com/user-attachments/assets/d13ec7b6-025d-4934-8004-0e3ed08d6692" />

Implemented dynamic ingestion of bookings, flights, customers, and airports CSV files using Databricks Auto Loader. Data is stored as Bronze Delta tables with schema tracking and checkpoint management for reliable streaming ingestion.

### Silver Layer – DLT Pipeline
<img width="1000" height="130" alt="pipeline_original_upscaled_4x" src="https://github.com/user-attachments/assets/6daea7e2-419e-4340-a07d-8394759a60c9" />
<img width="700" height="200" alt="flow_graph_upscaled_4x" src="https://github.com/user-attachments/assets/1cf6775c-08a0-42d3-a6ca-b134dfcf6b6f" />

Implemented streaming transformations using Delta Live Tables (DLT) to clean and validate Bronze data. Applied data quality expectations and CDC-based SCD Type 1 processing to maintain updated passenger, flight, and airport records. Integrated the processed data with bookings to create the consolidated `silver_business` table.

### Gold Layer – Dimension Tables
<img width="600" height="200" alt="image" src="https://github.com/user-attachments/assets/5be67c65-ff5e-4e84-bb74-fb5c2f4d8b81" />

Implemented automated dimension table upserts using parameterized PySpark workflows and Delta MERGE. Generated surrogate keys for new records and updated existing records based on CDC timestamps, maintaining dimension data.

### Gold Layer – Fact Table
<img width="500" height="250" alt="image" src="https://github.com/user-attachments/assets/9c232cca-9130-4fd0-bb4c-6de7ab3b3deb" />
<img width="300" height="250" alt="image" src="https://github.com/user-attachments/assets/7e46e5d0-669b-428c-b00f-7365d976c74b" />

Implemented metadata-driven fact table processing using dynamic SQL generation. Integrated booking data with passenger, flight, and airport dimensions to resolve surrogate keys and perform automated UPSERT operations on `FactBookings` using Delta MERGE.

### Project Execution
<img width="600" height="200" alt="image" src="https://github.com/user-attachments/assets/9be6a3de-26d2-4b82-ad3d-73819680d412" />

The pipeline is orchestrated through Databricks Jobs, with separate tasks for source parameterization, Bronze ingestion, and downstream processing across the Silver and Gold layers.

### Business Impact

- **Automated Data Processing:** Streamlined ingestion and transformation across multiple datasets, reducing manual intervention.
- **Improved Data Quality:** Implemented validation checks and CDC-based processing to maintain consistent, up-to-date records.
- **Reduced Repetitive Development:** Used parameterized dimension workflows and metadata-driven fact processing to minimize table-specific logic.
- **Enabled Analytical Reporting:** Built integrated Gold-layer fact and dimension tables with surrogate keys, providing structured data for analytical queries.

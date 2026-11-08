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

Implemented dynamic ingestion of bookings, flights, customers, and airports CSV files using Databricks Auto Loader. Data is stored as Bronze Delta tables with schema tracking and checkpoint management for reliable streaming ingestion.

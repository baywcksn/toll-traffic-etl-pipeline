# Toll Traffic ETL Pipeline

An automated ETL (Extract, Transform, Load) pipeline built with Apache Airflow and BashOperator to process toll traffic data from multiple file formats into a consolidated and transformed dataset.

## 📌 Project Overview

This project demonstrates the implementation of a scheduled ETL pipeline using Apache Airflow.

The pipeline processes toll traffic data provided in different file formats, extracts the required fields, consolidates the datasets, and transforms the data into a standardized format.

The workflow is orchestrated through an Apache Airflow DAG and runs inside a Docker-based environment.

## 🏗️ Architecture

```text
                    ┌─────────────────────┐
                    │   Toll Traffic Data │
                    │                     │
                    │ CSV / TSV / TXT     │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │    Apache Airflow   │
                    │        DAG          │
                    └──────────┬──────────┘
                               │
             ┌─────────────────┼─────────────────┐
             │                 │                 │
             ▼                 ▼                 ▼
      Extract CSV        Extract TSV      Extract Fixed-width
             │                 │                 │
             └─────────────────┼─────────────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │  Consolidate Data   │
                    │   extracted_data    │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │ Transform Data      │
                    │  Standardize Values │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │ transformed_data.csv│
                    └─────────────────────┘

🔄 ETL Workflow

The pipeline consists of six main processing tasks:

unzip_data
     ↓
extract_data_from_csv
     ↓
extract_data_from_tsv
     ↓
extract_data_from_fixed_width
     ↓
consolidate_data
     ↓
transform_data
# BookMyShow Lakehouse Portfolio Project

This project implements a governed BookMyShow-like analytics platform in Databricks using a Lakeflow Spark Declarative Pipeline.

## Architecture

* Landing zone: [workspace.bookmyshow_landing](#schema) with raw JSON source files in [raw_feed](#volume-/Volumes/workspace/bookmyshow_landing/raw_feed)
* Curated zone: [workspace.bookmyshow_curated](#schema) with bronze, silver, and gold managed tables
* Pipeline: [BookMyShow Lakehouse Pipeline](#pipeline-1dd9e402-e950-4d53-a8a0-120881059add)
* Dashboard target: [01f1a0f6b32e1032b1b57fcfe159ff85](#dashboard-01f1a0f6b32e1032b1b57fcfe159ff85)

## Source domains

* Theaters
* Movies
* Customers
* Shows
* Bookings
* Payments

## Curated tables

* Bronze: `bronze_theaters`, `bronze_movies`, `bronze_customers`, `bronze_shows`, `bronze_bookings`, `bronze_payments`
* Silver: `silver_theaters`, `silver_movies`, `silver_customers`, `silver_shows`, `silver_bookings`, `silver_payments`
* Gold: `gold_executive_kpis`, `gold_revenue_daily`, `gold_payment_performance_daily`, `gold_operational_health_daily`, `gold_occupancy_daily`

## Operational pattern

* `tools/generate_bookmyshow_sources.py` creates deterministic synthetic landing data in the governed volume
* The pipeline ingests landing data with Auto Loader into bronze streaming tables
* Silver applies deduplication, validation, and conformed business logic
* Gold publishes KPI-ready tables for executive reporting and interview demos
* The project is ready to be moved into a Declarative Automation Bundle for DEV and PROD deployment targets

## Job design

* Daily job path: source-generation step, then a regular pipeline refresh
* Full rebuild job path: source-generation step, then a pipeline full refresh
* Triggered mode is retained so jobs can control cadence cleanly

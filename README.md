# IS3107 Final Project: Amazon Products ETL & Dashboard

A summarized data engineering project that builds an end-to-end ETL pipeline and visualization tool for analyzing Amazon product listings.

This project demonstrates:
- Use of **Airflow** to automate data ingestion, cleaning, and loading  
- Construction of a **MySQL** data warehouse  
- Visualization through a **Streamlit dashboard**  
- **Exploratory data analysis** with Jupyter  

---

## Project Summary

We created a data pipeline that extracts product data from Kaggle's Amazon dataset, processes and loads it into a MySQL database, then visualizes key insights. The pipeline and dashboard run in a reproducible Docker environment.

The pipeline includes:
- **ETL with Airflow**: A DAG (`another_dataset.py`) downloads the raw dataset, cleans it, and stores it into normalized MySQL tables.
- **Schema Setup**: SQL scripts (`init.sql`) define structured tables for product metadata and category-level aggregations.
- **Dashboard**: A Streamlit app (`app.py`) reads from the database to display trends across categories, prices, ratings, and time.
- **Notebook Analysis**: The `downstream_analysis.ipynb` notebook explores correlations and performs additional statistical analysis on the dataset.

---

## Workflow

```
Amazon-Products.csv (Kaggle)
        |
        v
+---------------------+
|  Airflow DAG (ETL)  |
| download → clean →  |
| load_to_mysql       |
+---------------------+
        |
        v
+---------------------+        +-----------------------+
|     MySQL DB        |<------>| init.sql schema setup |
+---------------------+        +-----------------------+
        |
        v
+---------------------+        +---------------------------+
| Streamlit Dashboard |<------>| app.py: metrics & charts  |
+---------------------+        +---------------------------+

     +-----------------------------------------+
     | downstream_analysis.ipynb: EDA & charts |
     +-----------------------------------------+
```

---

## Key Features

- **Category Insights**: Understand which categories dominate in price and ratings  
- **Price & Rating Trends**: Explore how product prices and ratings are distributed  
- **Time Series Metrics**: Track changes in product count, average price, and rating over time  
- **Modular DAG**: Clear ETL breakdown using Airflow’s TaskFlow API  

---

## Tools Used

`Python 3.8+`, `Apache Airflow`, `MySQL`, `Streamlit`, `Docker`, `pandas`, `SQLAlchemy`, `Jupyter Notebook`

---

## Outcome

This project shows how data engineering concepts like pipelines, warehousing, and analytics come together. It gives end-users a dashboard to interpret large product datasets and offers reusable code for building similar workflows.

---

## Authors & License

Developed by Matthew Ho and team for IS3107.  
Licensed under the MIT License. See `LICENSE` for details.

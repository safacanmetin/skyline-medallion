# 🚀 Skyline Medallion: End-to-End Cloud Data Pipeline

[![Platform: GCP](https://img.shields.io/badge/Platform-GCP-blue?style=flat-square&logo=google-cloud)](https://cloud.google.com/)
[![Engine: Databricks](https://img.shields.io/badge/Engine-Databricks-red?style=flat-square&logo=databricks)](https://www.databricks.com/)
[![Format: Delta Lake](https://img.shields.io/badge/Format-Delta_Lake-00ADFF?style=flat-square&logo=databricks)](https://delta.io/)

> **"Theory is when you know everything but nothing works. Practice is when everything works but no one knows why. In this project, I combined both: I got my hands dirty to understand how modern cloud stacks actually breathe."**

## 🎯 The Philosophy Behind "Skyline Medallion"
Instead of watching 30+ hours of tutorials on GCP and Databricks, I chose to dive straight into the "mud." This project is a focused part of my continuous learning journey in Data Engineering. Technologies and stacks change, but the ability to architect a secure, scalable, and user-centric data product is the core skill I am constantly refining.

---

## 👔 Non-Technical Perspective
This project solves the challenge of transforming raw, messy banking data into actionable business insights.

- **Outcome-Driven:** It calculates real-time customer churn rates across different age demographics.
- **Security-First:** I bypassed the common (but risky) practice of using static JSON keys. Instead, I implemented **Managed Identity Access**, ensuring no "passwords" are hidden in the code.
- **User-Centric:** I developed a dashboard with a **"Contextual Metadata"** approach, ensuring non-technical stakeholders understand the business logic behind every chart.

---

## ⚙️ Data Engineer Perspective
Focusing on **Security, Schema Enforcement, and Minimal Data Movement.**

### 1. Ingestion Layer (Bronze)
- **Source:** Simulated Banking API (GitHub-hosted raw data).
- **Process:** Python-based ingestion script using `google-cloud-storage`.
- **Optimization:** Data is converted from CSV to **Parquet** format immediately to reduce storage costs and improve read performance.

### 2. Transformation Layer (Silver)
- **Engine:** **Databricks Serverless** (PySpark).
- **Logic:** Handled duplicates, null values, and implemented feature engineering (Age Grouping).
- **Storage:** Data is stored in **Delta Lake** format, providing ACID transactions and time-travel capabilities.

### 3. Serving Layer (Gold)
- **Warehouse:** **BigQuery** using **External Tables**. Data remains in GCS but is queryable via SQL, significantly reducing costs by avoiding double storage.
- **Security:** Implemented **Service Account Impersonation** and **Unity Catalog External Locations**—eliminating the need for local JSON credentials.
> **Security Architecture Decision:** > I followed the official [Google Cloud Authentication Decision Tree](https://cloud.google.com/docs/authentication#auth-decision-tree) to implement the most secure authentication method for this environment. By choosing **Service Account Impersonation** over static JSON keys, I ensured that the pipeline adheres to the principle of "Least Privilege" and enterprise-grade security standards.

---

## 📊 Data Product Showcase
### Business Analytics Dashboard
<img width="1920" height="1032" alt="image" src="https://github.com/user-attachments/assets/a9ca7ac5-2988-49d1-96eb-fa62f04ce3e6" />

### Multi-Layer Storage Strategy
<img width="414" height="531" alt="image" src="https://github.com/user-attachments/assets/1abe7718-fdfe-4e86-a12f-2f352d890994" />

### Infrastructure & Governance
<img width="1920" height="911" alt="image" src="https://github.com/user-attachments/assets/0e81299d-ec5b-4a9c-bcb0-7f520aa62629" />

---

## 📈 The Journey Continues
Data Engineering is an evolving field. This project is a "snapshot" of my current mastery of **GCP** and **Databricks**, but more importantly, it is a testament to my commitment to **Learning by Doing.** The goal wasn't just to move data; it was to understand the "why" and "how" of secure cloud integration.

---
**Safa** - Data Engineer | [[LinkedIn Profile Link](https://www.linkedin.com/in/safacanmetin/)]

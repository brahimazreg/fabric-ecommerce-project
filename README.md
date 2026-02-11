<img width="1939" height="1147" alt="image" src="https://github.com/user-attachments/assets/8a6c0a5a-a19b-49ee-b795-fea9b76a16ce" />


# Ecommerce Data Engineer Project

## Use Case
Building a **Unified Customer 360 View** for an e-commerce company.

An e-commerce company operates across multiple channels: website, mobile app, customer service, and order management. Customer data is scattered across different systems. The leadership team wants a **single view of the customer** that brings together:

- Profile information
- Purchase behavior
- Payment history
- Customer support interactions
- Web and app activity

**Objective:**  
Create a **Customer 360° Dashboard** in Microsoft Fabric by integrating and cleaning data from multiple messy sources using **Lakehouse**, **PySpark**, and **Power BI** to provide insights across the entire customer journey.

---

## Data Sources

| File | Source System | Description |
|------|---------------|-------------|
| `Customers.csv` | CRM System | Customer profile: name, gender, date of birth, location |
| `Orders.csv` | Order Management | Product orders, dates, values |
| `Payments.csv` | Payment System | Payment mode, status, amounts |
| `Support_tickets.csv` | Support System | Complaints, resolution, ticket logs |
| `Web_activities.csv` | Website Analytics | Page views, device types, session times |

---

## KPI Description

- **Total Customers:** Active customers with at least one transaction  
- **Average Order Value:** Spend per order  
- **Open Tickets Count:** Number of unresolved customer complaints  
- **Churn Risk:** Customers with past tickets but no recent orders  
- **Payment Mode Split:** % usage of Credit Card, Mobile, UPI, Wallet, etc.  
- **Device Preference:** Web and app device usage trends  

---

## Tools & Technologies

- **Microsoft Fabric** (Lakehouse, Pipelines, Semantic Models)  
- **PySpark** (Data cleaning, transformation, joins)  
- **Power BI** (Customer 360° dashboard and KPIs visualization)  

---

## Outcome

A **fully unified Customer 360 view** providing insights into:

- Customer profiles and demographics  
- Purchase and payment behavior  
- Support interactions and complaint resolution  
- Website and app activity trends  
- KPIs for business decisions and churn prevention

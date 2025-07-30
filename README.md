
---
## 🎯 Project Goals  

This project was developed to:

1. **Analyze traffic and purchase behavior trends**  
   Provide clear visibility into how user sessions, purchases, and revenue evolve over time.  

2. **Identify drivers of performance changes**  
   Break down revenue and purchase shifts by traffic source, device, country, and order value segments.  

3. **Enable actionable insights**  
   Highlight where marketing efforts or operational improvements (e.g., recovering high-value order segments) can have the biggest impact.

---

##  How It Works  
1. **Data Ingestion**  
   - Source data comes from Big Query bigquery-public-data.ga4_obfuscated_sample_ecommerce.  

2. **Data Transformation (dbt)**  
   - dbt models clean and transform raw session and purchase data into analytics-ready tables.  
   - Models calculate key metrics like sessions, revenue.  

3. **Visualization (Power BI)**  
   - Data models are exported to CSV files (due to Big Query trial limitations).  
   - Custom DAX measures power dynamic metrics and period-over-period comparisons.  

---

## 📊 Dashboard Preview  
<img width="1707" height="969" alt="image" src="https://github.com/user-attachments/assets/7d183b6f-4e37-4821-b58f-a12dbf5ad31f" />


🔗 **[View the Live Dashboard](eyJrIjoiMTQxY2U4YTctMmNjZC00MWI4LThkOTEtODA2Y2U5ODE3M2E0IiwidCI6IjY3MDFlY2Y3LTMyZWUtNDZlZS05ZDViLTEzODVlMjc3MmRjZiJ9)**  

---

## 📈 Key Insights from the Dashboard  
- The business is experiencing compounded declines: traffic remains weak and recent weeks show deterioration in both order value and per-item revenue.

- Despite only a minor dip in overall sessions, revenue losses were severe, driven by sharp declines in high-converting traffic sources and order value.

- Conversion rate stability suggests marketing and traffic quality—not site UX—are driving losses.
Immediate focus should be on stabilizing high-value order segments ($50–100) and recovering traffic sources (Direct, Google Organic).

---



# Kibana Dashboard Creation Guide

This guide provides step-by-step instructions for creating a Cryptocurrency Analysis Dashboard in Kibana.

## Prerequisites
- Elasticsearch running with data indexed in the `cryptoprices` index
- Kibana 7.x or higher connected to your Elasticsearch instance

## Step 1: Create Index Pattern

1. Open Kibana in your browser
2. Navigate to Stack Management → Index Patterns
3. Click "Create index pattern"
4. Enter `cryptoprices` as the index pattern name
5. Select `timestamp` as the Time field
6. Click "Create index pattern"

## Step 2: Create Visualizations

### Visualization 1: Cryptocurrency Price Trends Line Chart

1. Go to Visualize → Create visualization
2. Select "Line" as the visualization type
3. Select the "cryptoprices" index pattern
4. Configure the Y-axis:
   - Aggregation: Average
   - Field: current_price
   - Custom label: "Average Price (USD)"
5. Configure the X-axis:
   - Aggregation: Date Histogram 
   - Field: timestamp
   - Interval: Auto
6. Add a Split Series:
   - Sub-aggregation: Terms
   - Field: name.keyword
   - Size: 5 (to show top 5 cryptocurrencies)
7. Click "Update" and save as "Crypto Price Trends"

### Visualization 2: Market Cap Comparison Bar Chart

1. Go to Visualize → Create visualization 
2. Select "Vertical Bar" as the visualization type
3. Select the "cryptoprices" index pattern
4. Configure the Y-axis:
   - Aggregation: Average
   - Field: market_cap
   - Custom label: "Average Market Cap (USD)"
5. Configure the X-axis:
   - Aggregation: Terms
   - Field: name.keyword
   - Size: 10
   - Order By: "Average Market Cap" (metric)
   - Order: Descending
6. Click "Update" and save as "Crypto Market Cap Comparison"

### Visualization 3: 24-Hour Price Change Heatmap

1. Go to Visualize → Create visualization
2. Select "Heat Map" as the visualization type
3. Select the "cryptoprices" index pattern
4. Configure the Y-axis:
   - Aggregation: Terms
   - Field: name.keyword
   - Size: 10
   - Order By: Top
5. Configure the X-axis:
   - Aggregation: Date Histogram
   - Field: timestamp
   - Interval: Hourly
6. Configure the color:
   - Aggregation: Average
   - Field: price_change_percentage_24h
   - Schema: Green to Red
7. Click "Update" and save as "24-Hour Price Change Heatmap"

### Visualization 4: Cryptocurrency Performance Data Table

1. Go to Visualize → Create visualization
2. Select "Data Table" as the visualization type
3. Select the "cryptoprices" index pattern
4. Configure the Table:
   - Split Rows
   - Aggregation: Terms
   - Field: name.keyword
   - Size: 15
5. Add Metrics:
   - Metric: Average, Field: current_price, Custom Label: "Avg Price (USD)"
   - Metric: Average, Field: market_cap, Custom Label: "Avg Market Cap"
   - Metric: Average, Field: price_change_percentage_24h, Custom Label: "Avg 24h Change (%)"
6. Click "Update" and save as "Crypto Performance Table"

## Step 3: Create Dashboard

1. Go to Dashboard → Create dashboard
2. Click "Add" from the top menu
3. Select all four visualizations created earlier
4. Arrange the visualizations on the dashboard:
   - Place "Crypto Price Trends" at the top
   - Place "Crypto Market Cap Comparison" below it
   - Place "24-Hour Price Change Heatmap" on the right
   - Place "Crypto Performance Table" at the bottom
5. Add a filter control:
   - Click "Edit" → "Add filter"
   - Add a filter for specific cryptocurrencies
6. Set the time range selector to "Last 24 hours" initially
7. Save the dashboard as "Cryptocurrency Market Analysis"

## Step 4: Enhance Dashboard (Optional)

1. Add a Markdown panel with dashboard explanation:
   - Click "Add" → "Add new panel" → "Markdown"
   - Enter descriptive text about your dashboard
2. Add a Time Series Visual Builder visualization to show price volatility:
   - Create a new TSVB visualization
   - Plot the standard deviation of prices over time
3. Add a pie chart showing market dominance by percentage

## Final Notes
- The dashboard will automatically refresh based on your Kibana settings
- You can adjust the time range to analyze different time periods
- For deeper analysis, create additional visualizations or modify existing ones
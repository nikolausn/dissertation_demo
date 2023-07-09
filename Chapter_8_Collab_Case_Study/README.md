1. Download the Airbnb Chicago Listings dataset, we used March 19th 2023 version
```
wget http://data.insideairbnb.com/united-states/il/chicago/2023-03-19/data/listings.csv.gz
gzip -d listings.csv.gz
```
2. Copy the data to the respective folder
```
cp listings.csv data_cleaning_workflow_development/chicago_listings.csv
cp listings.csv downstream_task_benchmarking/chicago_listings.csv
```
3. Run the data_cleaning_workflow_development scripts, for testing and developing cleaning workflow
4. Run the downstream_task_benchmarking scripts, for benchmarking different cleaning results on price prediction model.
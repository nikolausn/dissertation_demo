1. Download the Airbnb Chicago Listings dataset, we used March 19th 2023 version
```
wget http://data.insideairbnb.com/united-states/il/chicago/2023-03-19/data/listings.csv.gz
gzip -d listings.csv.gz
```
2. Place and rename the data in this folder as "chicago_listings.csv"
3. Split the dataset into two subset for noise variation by executing code in "Airbnb-Augmentation-DataSplit.ipynb"
4. Generate new dataset with noise variation by executing "AirBnb Augmentation-NoiseType1.ipynb" for noise type 1 on dataset subset 1 and "AirBnb Augmentation-NoiseType2.ipynb" for noise type 2 on dataset subset 2
5. Execute data-cleaning workflows to simulate curator actions by executing "Airbnb-Solution-Template-Workflow-C1.ipynb" "Airbnb-Solution-Template-Workflow-C2.ipynb" and "Airbnb-Solution-Template-Workflow-C3.ipynb" for curator C1,C2,and C3 data-cleaning results respectively.
6. Similarly, reporting and querying using cdcm model and ad-hoc provenance recorder "Airbnb-Solution-Template-DCMHarvest-C1.ipynb", "Airbnb-Solution-Template-DCMHarvest-C2.ipynb", and "Airbnb-Solution-Template-DCMHarvest-C3.ipynb", need to be executed sequentially.
7. Analyze and compare different results on downstream task by executing: "CollaborativeAsessment.ipynb"

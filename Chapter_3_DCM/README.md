# ORPE Data Cleaning (Provenance) Model - DCM

Execute provenance harvester:
```
python provenance_harvester.py <openrefine_projectfile>
```
This will produce an sqlite db file, for example
```
python provenance_harvester.py ipaw_2021_demo.tar.gz
```
will produce a db file: ipaw_2021_demo.db


After we harvest the openreifne project artifacts, execute sqlite query report using
```
./report_query.sh  <project dbfile>
```
for example
```
./report_query.sh  ipaw_2021_demo.db
```
This will produce queries result from the DCM


More data cleaning use case for practice on NYPL Menu dataset is available in the ***examples*** folder.


Data Cleaning Provenance Model Explorer Notebook (JCDL Poster / Demo) submission can be found on the JCDL branch:
https://github.com/idaks/IPAW2021-ORPE/tree/dcmx-jcdl


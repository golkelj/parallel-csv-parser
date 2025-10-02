# parallel-csv-stats

Created CLI c++ optimized csv parser and reader that is designed to be distributed across nodes -thread in my case-- while calculating the statistic summary, in aims to help data scientist and data analyst understand dataset more quickly and aid in exploritory data analysis. 

## Build
```g++ -O2 -std=c++17 -pthread distributed_csv.cpp -o distributed_csv```

## Run 
```./distributed_csv your_file.csv number_of_nodes```
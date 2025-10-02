# parallel-csv-stats

Created CLI c++ optimized csv parser and reader that is designed run chucks in parallel while calculating the statistic summary, in aims to help data scientist and data analyst understand dataset more quickly and aid in exploritory data analysis. 

## Build
```g++ -O2 -std=c++17 -pthread parallel_csv.cpp -o parallel_csv```

## Run 
```./parallel_csv your_file.csv number_of_nodes```
# parallel-csv-stats

Created CLI C++ csv parser and reader that is designed run chucks in parallel while calculating the statistic summary, in aims to help data scientist and data analyst understand dataset more quickly in the CLI and aid in exploritory data analysis. 

## Build
```g++ -O2 -std=c++17 -pthread parallel_csv.cpp -o parallel_csv```

## Run 
```./parallel_csv your_file.csv number_of_nodes```

## Testing

The goal of this tool was for it to work in the CLI as pandas does not natively support this. 
I compared the run time to pandas (written in highly optimized C) and got results that were 3x slower. However, I was able to 
-Gain flexibility → CLI interface, configurable concurrency, works in shell scripts/pipelines. 
-Gain control → explicit tuning of worker processes, more transparent design.
-Gain portability → no Python required, unlike pandas.

While this would not serve as a effective tool purely for speed this can be used effectively in an EDA workflow.

As I develop my systems understanding I hope to reach the performance (if not better) of these highly optimized libraires that use C and C++ under the hood. 
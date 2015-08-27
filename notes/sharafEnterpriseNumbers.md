

5 udfs             5 udfs x 100 days = 500 times the number of things
90 days (100 )
10-100 blinks (updates)

     4,000,000 things

    20,000,000 udf-things

 2,000,000,000 blinks

how to store them

     4,000,000 records one doc for the complete-history all udfs and all changes

 2,000,000,000 record by udf-things by day ( in 90 days)

24,000,000,000 record by udf-things by hour ( in 90 days )


execute m/r  timeseries by day,  in 98342 msec = 1:38
    1 million of blinks in 2 mins
    2 billions of blinks in 55 hours


about timeseries, I have statistics and some estimated number about how big the DB will be and how much time we'll need
to execute a Map/Reduce process,

and these are the numbers:


I think, I have the numbers and the tests about the first use case: get the last value for udf in all things

but the use case is still wide-open,

about the feature to sort Udfs, we need to narrow it, because I don't like the idea to go over a 4M
collection and sort in the use case,
maybe


5 udfs             5 udfs x 100 days = 500 times the number of things
90 days (100 )
10-100 blinks (updates)

     4,000,000 things

    20,000,000 udf-things

 2,000,000,000 blinks

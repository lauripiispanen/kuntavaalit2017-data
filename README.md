Kuntavaalidata 2017
===================

Apache Spark analysis of the 2017 Finnish municipal elections.

Download the latest response data from YLE: http://yle.fi/uutiset/3-9526290 and place it in `src/main/resources`.

*NOTE:* Computing principal components takes a fair bit of memory. Consider allocating more memory to the process by running e.g. `sbt -mem 8192 run`.
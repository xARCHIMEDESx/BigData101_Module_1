## HOMEWORK 1

**==Finding all the longest words in a text==**

**--Driver settings--**
DecreasingComparator is used to sort the keys in decreasing order.

**--Map stage--**
To avoid the processing of unnecessary data preliminary filtering consisted from the next steps:
   1)Finding the length of the longest word in line.
   2)Checking whether the longest word in the current line is longer than the longest word in previous lines.
   3)If positive - getting all words with than length from the line and writing to the context. If negative - there is no need to push these words to the map phase, because there are already longer ones.
	
**--Combine/reduce stage--**
As the data, that coming to the combiner/reducer, is sorted in decreasing key order with the longest words at the top, there is no reason to process records except the first one. So, combiner produces to the next stage only the longest words from each mapper. Reducer takes the sorted output from combiners and, in the same way, processes only the first record, which contains the longest word(s) in all the text. 
 
***
 
## HOMEWORK 2

**==Calculating total and average bytes in all the requests per IP address + calculating browsers from which request were made==**

**--Driver settings--**
Sequence file output with Snappy compression is used according to the homework requrements.

**--Map stage--**
The first step is passing every input line to the method, which analyzes user-agent data in the line and retrieving browser name, incrementing corresponding counter. Next step - the actual mapping - retrieving IP address and bytes from the requests and writing to the context.

**--Combine stage--**
On this stage aggregation of mapped data is done - calculating total bytest per IP address, produced by every concrete mapper. Since it is not final data, we must keep the number of aggregations made per every IP. So, it was decided to use custom tuple-like data structure which implements WritableComparable interface. Combiner produces that custom tuple, containing number of aggregations made and sum of the bytes.

**--Reduce stage--**
Reducer takes data from all the combiners, makes final aggregation and writes to the hdfs key-value pairs:
-key - IP address
-value - custom tuple again, which now consists of average num of bytes in all requests per IP (total bytes devided by number of aggregations) and the sum of bytes in all requests per IP.
 
***

## HOMEWORK 3

**==Bidding dataset analyzer with custom partitioner==**

**--Driver settings--**
User custom partitioner with two reducers set. Partitioner selects a reducer, depending on the OS name, which is passed as a part of a key. It was noticed that near 60% of all bids were made from systems under Windows XP, so it was decided to use two reducers: one for WinXP requests, another - for all others OS. In this way we balance the load on the system.

**--Map stage--**
The map stage consist from the next steps:
	1)Filtering input data according to the requirement given - processing only bids with price > 250.
	2)Passing the input line to OS detection method.
	3)Retrieving cityID from the input data.
	4)Using dictionaries, stored in distributed cache, comparing ID with actual name. First looking into file with the list of cities. If the ID value is not presented there - looking into file with the list of regions, assuming that the city has the same name as the region it belongs. If the ID isn't presented there - marking that record missing by writing ("MISSING CITY ID " + cityID). 
	5)Writing down the key value pairs: key is a custom tuple-like data structure with a pair of actual city name and OS_id, got from the user-input, as described earlier. Value is a counter `IntWritable(1)`.
	
**--Combine stage--**
Combiner just aggregate corresponding data from mapper.

**--Reduce stage--**
Each of two reducers gets corresponding key-value pairs and aggregate the result. It writes to hdfs data, that consists only from part of a key (city name, we don't need OS name, it was provided for partitioning) and a count how many bids were made from this city.




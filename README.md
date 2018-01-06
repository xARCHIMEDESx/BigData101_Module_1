HOMEWORK 1
==Finding all the longest words in a text==

--Driver settings--
DecreasingComparator is used to sort the keys in decreasing order.

--Map stage--
To avoid the processing of unnecessary data preliminary filtering consisted from the next steps:
	1)Finding the length of the longest word in line.
	2)Checking whether the longest word in the current line is longer than the longest word in previous lines.
	3)If positive - getting all words with than length from the line and writing to the context. If negative - there is no need to push these words to the map phase, because there are already longer ones.
	
--Combine/reduce stage--
 As the data, that coming to the combiner/reducer, is sorted in decreasing key order with the longest words at the top, there is no reason to process records except the first one. So, combiner produces to the next stage only the longest words from each mapper. Reducer takes the sorted output from combiners and, in the same way, processes only the first record, which contains the longest word(s) in all the text. 
 
 
HOMEWORK 2
==Calculating total and average bytes in all the requests per IP address + calculating browsers from which request were made==

--Driver settings--
Sequence file output with Snappy compression is used according to the homework requrements.

--Map stage---
The first step is passing every input line to the method, which analyzes user-agent data in the line and retrieving browser name, incrementing corresponding counter. Next step - the actual mapping - retrieving IP address and bytes from the requests and writing to the context.

--Combine stage--
On this stage aggregation of mapped data is done - calculating total bytest per IP address, produced by every concrete mapper. Since it is not final data, we must keep the number of aggregations made per every IP. So, it was decided to use custom tuple-like data structure which implements WritableComparable interface. Combiner produces that custom tuple, containing number of aggregations made and sum of the bytes.

--Reduce stage--
Reducer takes data from all the combiners, makes final aggregation and writes to the hdfs key-value pairs:
-key - IP address
-value - custom tuple again, which now consists of average num of bytes in all requests per IP (total bytes devided by number of aggregations) and the sum of bytes in all requests per IP.

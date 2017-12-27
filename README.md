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

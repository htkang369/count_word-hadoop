The three java files are written in mapreduce.

Function:

# wordcount.java

This file can be found in any hadoop source, just the "hello word" for hadoop.
The function of this is to count one word frequency in a given file.

# wcdouble.java

This java file aims at counting double-word in a given file.

# ucache.java

This java file can be used to count certain parrent words in given files. There fore, I used distributed cache to store the pattern file and then to count the word exists in the file.

use statement below to utilize ucache.jar

# bin/hadoop jar ucache.jar ucache ¨Cslist ~/patterns.txt ~/input Output

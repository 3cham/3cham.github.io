# Writing single CSV file inside pyspark program

One of the common things that we do while implementing data science 
usecases is writing the prediction or recommendation result as csv file
and send it to another stakeholder. It could be a team that serves
an API, or customer relation team that sends the recommendation emails on 
top of those result. It may sound simple but there are several interesting
problems. In this article I will tell you how to do this in a convenient
way with pyspark.

In our tech stack, pyspark is heavily used by teams for implementing their 
data science pipelines. We prefer spark not only because of its friendly 
python API but also its great performance while processing large amount of
data. For example: we have an [Alternating Least Square model](http://stanford.edu/~rezab/classes/cme323/S15/notes/lec14.pdf) that recommends
products for more than 40 million users. With a fine-tuned spark application,
we achieved to train the model within 1 hour. 

The current infrastructure requires us to produce the recommendation weekly
and the result should be single uncompressed csv file containing the top 100 
products and its normalized scores for each user. If we have `8 bytes` storing 
user ids, `8 bytes` storing products id and `4 bytes` for the scores, each row
should have `8 + 1 + 8 + 1 + 4 = 22` bytes of data. This leads to a single 
file with size of: `22 x 40 x 10^6 x 100 = 88 x 10^9 bytes` or approximately
`82GiB`. (Yes I know it is not great to send this single file but that is another
story)

To produce this file, we started with saving the prediction in a dataframe. After
that the csv files could be written as simply as:

```python
df.write.csv("/path/to/csv/file")
```

Then the csv file will be written into the HDFS filesystem (We use Spark on yarn). However, 
our dataframe may have multiple partitions and we ended up having multiple 
csv files inside our target path. One trivial way to have single csv file is to 
repartition the dataframe before writing sothat it has only 1 partition, thus, 1 
csv file is produced. 
```python
df.repartition(1).write.csv("/path/to/csv/file")
```
> **_NOTE:_** If you plan to have other than 1 partition, you may consider using `coalesce()` 
> instead of `repartition()` for better performance.

Unfortunately, it doesn't work this way because spark has to move all the data into 
1 executor and writes our csv file from there. It will cause `OutOfMemoryException`
since the executor does not have 82GiB of memory for having this big partition in memory.
> **_NOTE:_** Actually, to have 82GiB capacity for single partition, the executors
> must be configured with ~150GiB each, due to [Spark's memory management](https://spark.apache.org/docs/latest/configuration.html#memory-management).

So how could we solve this problem? Going back one step, we found out that it is ok to 
have multiple csv files at once because:
 - Besides `OOM`, it takes a lot of time to repartition our dataframe
 - Without repartition, our files are written in parallel, and it is really fast!
 - **It is possible to merge those files into single file**

I found that the most typical way when it comes to file manipulating along with 
pyspark is to use subprocess and calling unix command inside our driver program. It works
most of the cases. However, it becomes pretty complicated in our case: you need to copy 
those files into your spark driver node. Working with HDFS on yarn, your spark driver 
program is started as a yarn container with a temporary filesystem. After copying & merging
files, you'll need to put our final csv file back to HDFS since our spark driver filesystem 
will be deleted afterwards. 


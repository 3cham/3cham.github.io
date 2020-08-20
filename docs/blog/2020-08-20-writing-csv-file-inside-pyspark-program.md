# Writing CSV file inside pyspark program

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
data. For example: we have an Alternating Least Square model that recommends
products for more than 40 million users. With fine-tuned spark application,
we achieved to train the model in under 2 hours. 

The current infrastructure requires us to produce the recommendation weekly
and the result should be single uncompressed csv file containing the top 100 
products and its normalized scores for each user. If we have `8 bytes` storing 
user ids, `8 bytes` storing products id and `4 bytes` for the scores, each row
should have `8 + 1 + 8 + 1 + 4 = 22` bytes of data. This leads to a single 
file with size of: `22 x 40 x 10^6 x 100 = 88 x 10^9 bytes` or approximately
`82GiB`. (Yes I know it is not great to send this single file but that is another
story)

To produce this file, we started with saving the prediction in one dataframe. After
that the csv files could be written as simply as:

```python
df.save
```

 

### MapReduce

#### Without Hadoop Installation

java -cp bigdata-1.0-SNAPSHOT-shaded.jar <class_name> <required_arguments>

#### With Hadoop

hadoop jar bigdata-1.0-SNAPSHOT.jar <class_name> <required_arguments>

q1 -> hadoop jar bigdata-1.0-SNAPSHOT.jar mr.q1.CommonFriends <soc-LiveJournal1Adj.txt path> output1     
q2 -> hadoop jar bigdata-1.0-SNAPSHOT.jar mr.q2.Top10MutualFriends <soc-LiveJournal1Adj.txt path> output2    
q3 -> hadoop jar bigdata-1.0-SNAPSHOT.jar mr.q3.JoinInMemory 422 415 <userdata.txt path> <soc-LiveJournal1Adj.txt path> output3    
q4 -> hadoop jar bigdata-1.0-SNAPSHOT.jar mr.q4.JobChaining <userdata.txt path> <soc-LiveJournal1Adj.txt path> output-temp output4

### Spark

q1 -> java -cp bigdata-1.0-SNAPSHOT-shaded.jar spark.q1.Common <soc-LiveJournal1Adj.txt path> output1
q2 -> java -cp bigdata-1.0-SNAPSHOT-shaded.jar spark.q2.Top10 <soc-LiveJournal1Adj.txt path> <userdata.txt> output2
q3 -> java -cp bigdata-1.0-SNAPSHOT-shaded.jar spark.q3.Filter <business.csv path> <review.csv path> output3
q4 -> java -cp bigdata-1.0-SNAPSHOT-shaded.jar spark.q4.Average <business.csv path> <review.csv path> output4
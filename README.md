# MtsStats

hadooper works here

To try it:
1. Bind 100-500 dependencies to eclipse.
2. Start your hadoop. I use pseudo-distributed mode.
3. Build jar

Input files should be in HDFS.
Too start task 
hadoop jar MtsStats.jar hdfs://localhost:54310/input/*.xml hdfs://localhost:54310/bomba

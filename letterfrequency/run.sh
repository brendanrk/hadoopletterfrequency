set -x
set -e

hadoop fs -rm -r /user/hduser/myOutput  || true
hadoop fs -rm -r /user/hduser/myOutput-LetterFrequency-tmp || true
hadoop fs -rm -r /user/hduser/myOutput-LetterFrequency-tmp1 || true
javac -encoding UTF8 *java 
jar cvf wc.jar  *class
rm *class
# hadoop jar wc.jar LetterFrequency gutenberg/de/57909.txt myOutput
hadoop jar wc.jar LetterFrequency gutenberg/* myOutput

hdfs dfs -cat myOutput/part-r-00000 | head | grep fr || true
hdfs dfs -cat myOutput/part-r-00001 | head | grep de || true
hdfs dfs -cat myOutput/part-r-00002 | head | grep es || true
hdfs dfs -cat myOutput/part-r-00003 | head | grep it || true

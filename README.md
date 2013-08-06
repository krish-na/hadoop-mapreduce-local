<h1>Introduction</h1>
Compilation of MapReduce (v1/v2) job. I will continue to use this repo to further my understanding of MapReduce, along with other Hadoop ecyosystem of techonologies. I've implemented Maven for compiling, running local tests, and packaging a jar file for execution.

<h2>Outline</h2>

1. manOfSteelReview - classic word count example, but using Man Of Steel review from various websites. Would like to eventually use Flume to ingest from Twitter streams hash tags
2. averageWordLenght - another classic mapreduce sample, compute average word length 
3. invertedIndex - create an inverted index of every word from list of files
4. logAnalysisCounter - example project to demonstrate map reduce counters
5. logAnalysis - example project deonstrating multiple partitioners, where individual logs files are generated per month
6. more to come!

<h2>Setup</h2>
1. Clone this repository and download Hadoop Distribution from here, you will need the libraries from ${hadoop_home}/lib dir
http://hadoop.apache.org/releases.html

2. Add the Hadoop /lib libraries to your classpath

3. For example, if you would like to run logAnalysis as a MapReduce job. 
You can run mvn clean package from the project directory (ex. ${user_workspace}/hadoop-mapreduce-local/code/logAnalysis/target

You should be able to run the mapreduce job using appropriate input and output directories

4. For detail instructions on setting up Hadoop in your local environment: 



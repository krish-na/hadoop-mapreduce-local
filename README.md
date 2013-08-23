<h1>Introduction</h1>
Bootstrap Maven project for running local (or cluster if you have one) MapReduce (v1/v2) jobs. I use Maven for partial dependency management, compilation, running MRUnit tests, and packaging a jar file.

<h2>Outline</h2>

1. manOfSteelReview - classic word count example, but using Man Of Steel review from various websites. Would like to eventually use Flume to ingest from Twitter streams hash tags
2. averageWordLength - another classic mapreduce sample, compute average word length 
3. invertedIndex - create an inverted index of every word from list of files
4. logAnalysisCounter - example project to demonstrate MapReduce Counters functionality
5. logAnalysis - example project deonstrating MapReduce multiple partitioners, where individual logs files are generated on a month basis
6. tidem - text analyzer that processes text and provides information about its word contents. Generate key value pairs that shows a count of how many times each word occurs in the text. Result is Primary sort by word length, and a Secondary sort based on ASCII.

<h2>Setup</h2>

Note: For detailed instructions on setting up Hadoop in your local environment: 

http://hadoop.apache.org/docs/stable/single_node_setup.html

1. Clone this repository and download Hadoop Distribution from here, you will need the libraries from ${hadoop_home}/lib dir
http://hadoop.apache.org/releases.html

2. Add the Hadoop /lib libraries to your classpath in your IDE of choice

3. For example, if you would like to run "logAnalysis" MapReduce job: 

run mvn clean package from the project directory (ex. ${user_workspace}/hadoop-mapreduce-local/code/logAnalysis/target

You should be able to run the mapreduce job using appropriate input and output directories

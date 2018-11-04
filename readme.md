## Big-Data-Fall-2018

## Suggestions/assumptions:
* instead of word_id project uses String.hashCode()

## File SearchEngine
Introduce search part of the project

## Hadoop-based Search Engine

## File IndexEngine
Duty for indexing input files 

Task description:
https://hackmd.io/s/H1LM2fR5m#


Project initialized with respect to this web-guide:
http://www.soulmachine.me/blog/2015/01/30/debug-hadoop-applications-with-intellij/


### Assignment: 
Create Search and Index engines using map-reduce tasks on hadoop cluster. Cluster itself with dataself to learn from were provided by university staff. Project itself uses several mapReduce jobs to analyze provided data and allows you to learn about the relevance of the entered word in the database.

### Our team was motivated by several reasons:
* learn how to write and adapt mapReduce (paradigm) jobs 
* touch hadoop cluster in practice
* check how big data environment is organized 
* learn about effectiveness of clean mapReduce job without any frameworks
* develop practical skills of using Java language for Hadoop aplications
* evolve team working skills


### Team workflow and Tasks distribution 
   We decided to use pair programming paradigm, and created two sub-teams
   1 - Boris and Danila who were intended to create indexer engine
   2 - Vlad and Timur who based on ready indexer prototype written by  sub-team 1 created searcher engine
   3 - Report made by all teammates 

### How to run:
Build all packages into single ```.jar``` file. Run this inside Hadoop environment.

#### Indexing job
Runs once, preliminary before Query job. Also generates ```output_idf/``` folder in place of execution. Due to output_idf needed for both tasks.
```bash
$ [JAR_NAME] Indexer [INPUT_PATH] [OUTPUT_PATH]
```
#### Query job
Runs to find indices of documents that are the most relevant for given query. As searching metric, [TF/IDF](https://www.codeproject.com/Articles/33952/Anatomy-of-a-relevant-search-engine-part-1) is used. 
```bash
$ [JAR_NAME] Query [INDEXER_OUTPUT] [SEARCH_OUT] [NUM_OF_DOCKS] [QUERY]
```
## Workflow

![](https://i.imgur.com/AmessQ6.png)

#### 1) TF job
Works like wordcount from example, but produces MapWritable for each document. Due to works with hugest amount of data in project, this job executes on several Mappers and Reducers. As words representation, standart [Java hash](https://en.wikipedia.org/wiki/Java_hashCode()) function used. Collisions not messured, but it assumed that they are not sufficient for project aims.

#### 2) IDF job
Uses result of TF job, due to it already contains unique elements in map. It reduces work done, even for big corpus.
Note: for now, this job works dummy - it uses only one Mapper/Reducer. It needs improvement to works in parallel. But still, it does it's work correct.

#### 3) TF/IDF job
MapReduce paradigm uses as input only one type of input files, TF/IDF job needs two outputs. To overcome this limitations, it was decided to send IDF job result as JSON parameter (in Configuration object). This decision force to read output file of IDF job directly from Disk. Hence, to not to do this on each node, this job done once in job preparation block. Each node just restores map from JSON representation in setup() method.

#### 4) Query preprocessing
Query represents in Map object. This is implementation of Sparce Vector form, as was described in task document. Also IDF approach applied for query vector.

#### 5) Search job
Query arrives (as in p.3) in JSON format. This job calculates production of each document sparse vector and query sparse vector. Due to all sparse vectores implemented as Map, key set of Query is used (this is more optimal). 

#### 6) Sorting task 
Implemented by linux [command](https://stackoverflow.com/questions/20583211/top-n-values-by-hadoop-map-reduce-code) 
```bash
$ cat %s/part* | sort -n -k2 -r| head -n[TOP_NUM]
```


### Appendix
#### How to run directly on cluster
```bash
$ hadoop jar /home/team10/keck.jar Indexer /EnWiki /home/team10/output_indexer
```
```bash
$ hadoop jar /home/team10/keck.jar Query /home/team10/output_indexer /home/team10/search_res 10 "cat"
```

There is issue in Query part: MapWritable objects are stored by default method in memory. But when it again uses as input for other Mapper, it raises ```NullPointerException```. This is internal problem of framework and code works. To prove this, you can execute Indexing and Query such as:
```bash
$ hadoop jar /home/team10/keck.jar Indexer /EnWiki/AA_wiki_00 /home/team10/output_indexer

$ hadoop jar /home/team10/keck.jar Query /home/team10/output_indexer /home/team10/search_res 10 "and"
```
Expected file (/home/team10/search_res) will appear with results. 

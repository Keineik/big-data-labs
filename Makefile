container := myhadoop
HADOOP_HOME := /opt/hadoop
hdfs := $(HADOOP_HOME)/bin/hdfs
hadoop := $(HADOOP_HOME)/bin/hadoop 

src := $(src)
dest := $(dest)
input := $(input)
output := $(output)
base_name := $(shell basename $(src))
base_name_strip := $(shell basename $(src) .java)

hadoop-build:
	docker build -t $(container) ./hadoop;

hadoop-run:
	docker run -p 9870:9870 --name $(container) --rm -it $(container);

# Params: src, dest
hdfs-dfs-put:
	docker exec $(container) mkdir -p /tmp/hdfs_tmp;
	docker cp $(src) $(container):/tmp/hdfs_tmp;
	docker exec $(container) $(hdfs) dfs -mkdir -p $(dest);
	docker exec $(container) $(hdfs) dfs -put -f /tmp/hdfs_tmp/$(base_name) $(dest);

# Params: src, input, output
mapred-compile-run:
	docker exec $(container) rm -rf /tmp/mapred;
	docker exec $(container) mkdir -p /tmp/mapred;
	docker cp $(src) $(container):/tmp/mapred;
	docker exec --workdir /tmp/mapred $(container) $(hadoop) com.sun.tools.javac.Main $(base_name);
	docker exec --workdir /tmp/mapred $(container) sh -c "jar cf $(base_name_strip).jar $(base_name_strip)*.class";
	- docker exec $(container) $(hdfs) dfs -rm -R $(output);
	docker exec --workdir /tmp/mapred $(container) $(hadoop) jar $(base_name_strip).jar $(base_name_strip) $(input) $(output);

spark-up:
	docker compose -f ./spark/docker-compose.yaml up --scale spark-worker=2 -d

spark-down:
	docker compose -f ./spark/docker-compose.yaml down --volumes

# Params: src, input
spark-submit:
	docker exec spark mkdir -p /tmp/jobs;
	docker cp $(src) spark:/tmp/jobs/$(shell basename $(src));
	docker cp $(input) spark:/tmp/jobs/$(shell basename $(input));
	docker exec --workdir /tmp/jobs spark spark-submit --master spark://spark:7077 --files $(shell basename $(input)) $(shell basename $(src)) $(shell basename $(input))

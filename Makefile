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


hdfs-dfs-put:
	docker exec $(container) mkdir -p /tmp/hdfs_tmp;
	docker cp $(src) $(container):/tmp/hdfs_tmp;
	docker exec $(container) $(hdfs) dfs -mkdir -p $(dest);
	docker exec $(container) $(hdfs) dfs -put -f /tmp/hdfs_tmp/$(base_name) $(dest);

mapred-compile-run:
	docker exec $(container) mkdir -p /tmp/mapred;
	docker cp $(src) $(container):/tmp/mapred;
	docker exec --workdir /tmp/mapred $(container) $(hadoop) com.sun.tools.javac.Main $(base_name);
	docker exec --workdir /tmp/mapred $(container) sh -c "jar cf $(base_name_strip).jar $(base_name_strip)*.class";
	docker exec --workdir /tmp/mapred $(container) $(hadoop) jar $(base_name_strip).jar $(base_name_strip) $(input) $(output);
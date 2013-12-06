JFLAGS = -d ../bin/
JEXFLAGS = -d ../bin2/
JC = javac
.SUFFIXES: .java .class
.java.class:
	$(JC) $(JFLAGS) $*.java

CLASSES = \
		mapreduce/ClientJobSubmitter.java\
		mapreduce/ReducerWorker.java\
		mapreduce/TaskTrackerServices.java\
		mapreduce/Utility.java\
		mapreduce/ClientServices.java\
		mapreduce/DefaultTaskScheduler.java\
		mapreduce/InputFormat.java\
		mapreduce/Job.java\
		mapreduce/JobClient.java\
		mapreduce/JobConf.java\
		mapreduce/JobMeta.java\
		mapreduce/JobTracker.java\
		mapreduce/JobTrackerJobSubmitter.java\
		mapreduce/JobTrackerServices.java\
		mapreduce/MapStatusChecker.java\
		mapreduce/Mapper.java\
		mapreduce/MapperOutputer.java\
		mapreduce/MapperTaskInfo.java\
		mapreduce/MapperWorker.java\
		mapreduce/OutputFormat.java\
		mapreduce/Outputer.java\
		mapreduce/Partitioner.java\
		mapreduce/Record.java\
		mapreduce/Reducer.java\
		mapreduce/ReducerOutputer.java\
		mapreduce/ReducerTaskInfo.java\
		mapreduce/StatusUpdater.java\
		mapreduce/TaskInfo.java\
		mapreduce/TaskLauncher.java\
		mapreduce/TaskMeta.java\
		mapreduce/TaskOutput.java\
		mapreduce/TaskProgress.java\
		mapreduce/TaskScheduler.java\
		mapreduce/TaskStatusChecker.java\
		mapreduce/TaskTracker.java\
		mapreduce/TaskTrackerAliveChecker.java\
		mapreduce/TaskTrackerMeta.java\
		mapreduce/TaskTrackerUpdatePkg.java\
		mapreduce/Worker.java

default: classes

classes: $(CLASSES:.java=.class)

clean:
	rm -rf ../bin/*



WORDCOUNT_FILES = ./example/wordcount/*.java
wordcount:
	$(JC) $(JEXFLAGS) $(WORDCOUNT_FILES)

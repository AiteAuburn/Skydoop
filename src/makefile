JFLAGS = -d ../bin/
JEXFLAGS = -d ../skyBin/
FinalBin = -d ../FinalskyBin/
MaxMin = -d ../MaxMin/
ProbSkyline = -d ../ProbSkyline/
JC = javac
.SUFFIXES: .java .class
.java.class:
	$(JC) $(JFLAGS) $*.java


CLASSES = \
		mapreduce/ClientJobSubmitter.java\
		mapreduce/ReducerWorker.java\
		mapreduce/JobTracker.java\
		mapreduce/TaskTrackerServices.java\
		mapreduce/JobMeta.java\
		mapreduce/ClusterConfig.java\
		mapreduce/Utility.java\
		mapreduce/ClientServices.java\
		mapreduce/DefaultTaskScheduler.java\
		mapreduce/InputFormat.java\
		mapreduce/Job.java\
		mapreduce/JobClient.java\
		mapreduce/JobConf.java\
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



SKYLINE_FILES = ./example/skyline/*.java
skyline:
	$(JC) $(JEXFLAGS) -cp log4j-1.2.17.jar:. $(SKYLINE_FILES)
	$(JC) $(JEXFLAGS) -cp log4j-1.2.17.jar:. ./ProbSkyline/DataStructures/PartitionInfo.java
	$(JC) $(JEXFLAGS) -cp log4j-1.2.17.jar:. ./mapreduce/ClusterConfig.java
	$(JC) $(JEXFLAGS) -cp log4j-1.2.17.jar:. ./ProbSkyline/WrapRect/*.java
	$(JC) $(JEXFLAGS) -cp log4j-1.2.17.jar:. ./ProbSkyline/ProbSkyQuery/Prune3.java
	$(JC) $(JEXFLAGS) -cp log4j-1.2.17.jar:. ./ProbSkyline/ProbSkyQuery/Prune1And2.java

Finalsky_FILES = ./example/FinalSky/*.java
Finalsky:
	$(JC) $(FinalBin) -cp log4j-1.2.17.jar:. $(Finalsky_FILES)
	$(JC) $(FinalBin) -cp log4j-1.2.17.jar:. ./mapreduce/ClusterConfig.java
	$(JC) $(FinalBin) -cp log4j-1.2.17.jar:. ./ProbSkyline/ProbSkyQuery/InstNaive.java
	$(JC) $(FinalBin) -cp log4j-1.2.17.jar:. ./ProbSkyline/InterClient.java

MaxMinMain = ./ProbSkyline/GenerateData/SplitData.java

maxmin:
	$(JC) $(MaxMin) -cp log4j-1.2.17.jar:. $(MaxMinMain)

prune:
	$(JC)  -cp log4j-1.2.17.jar:. ./ProbSkyline/SkyMain.java 

skyMain:
	java -cp log4j-1.2.17.jar:. ProbSkyline.SkyMain

inter:
	java -cp log4j-1.2.17.jar:. ProbSkyline.Intermediate

compileInter:
	javac -cp log4j-1.2.17.jar:. ./ProbSkyline/Intermediate.java

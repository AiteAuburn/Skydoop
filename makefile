MAPREDUCE_HOME=.
RMI_CODE_BASE=-Djava.rmi.server.codebase=file:$(MAPREDUCE_HOME)/bin/
CLASSPATH=$(MAPREDUCE_HOME)/bin/
PORT = 7890
SEQ = 1
OUTPUT = ./output/
INPUT = ./input/liang.txt
compile:
	cd src; make
	
jar:
	jar cf MapReduce.jar -C bin/ .

WCJar:
	jar cf wordcount.jar -C bin2/ .

rmi:
	cd $(CLASSPATH); rmiregistry $(PORT) &
	
jobtracker:
	java -cp $(CLASSPATH) $(RMI_CODE_BASE) -Djava.security.policy=java.security.AllPermission mapreduce.JobTracker &

tasktracker:
	java -cp $(CLASSPATH) $(RMI_CODE_BASE) mapreduce.TaskTracker $(SEQ)
	
client:
	java -cp $(CLASSPATH) $(RMI_CODE_BASE) mapreduce.JobClient &
	 
degreecount:
	java -cp degreecount.jar:$(CLASSPATH) $(RMI_CODE_BASE) example.degreecount.Main $(INPUT) $(OUTPUT) degreecount.jar

wordcount:
	java -cp wordcount.jar:$(CLASSPATH) $(RMI_CODE_BASE) example.wordcount.Main $(INPUT) $(OUTPUT) wordcount.jar

clean:
	#rm -rf bin/* MapReduce.jar
	pkill -9 java
	pkill -9 rmiregistry

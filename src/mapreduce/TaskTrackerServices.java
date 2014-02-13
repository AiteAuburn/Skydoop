package mapreduce;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.io.*;

import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.Enumeration;
import java.util.Map;
import java.util.Map.Entry;

import java.net.Socket;
import java.net.InetAddress;


/**
 * This class is the rmi services provided by task tracker
 */
public class TaskTrackerServices extends UnicastRemoteObject implements TaskLauncher, StatusUpdater {

  /* reference to the task tracker */
  private TaskTracker taskTracker;
	public boolean isJarTransfered = false;


  /**
   * constructor method
   * 
   * @param taskTracker
   * @throws RemoteException
   */
  public TaskTrackerServices(TaskTracker taskTracker) throws RemoteException {
    super();
    this.taskTracker = taskTracker;
  }


  /**
   * This method is called by job tracker to assign task to task tracker
   * 
   * @param taskInfo
   *          : the information about the task
   * @return the task is submitted successfully or not
   */
  public boolean runTask(TaskInfo taskInfo) throws RemoteException {

		while(isJarTransfered == false){
			isJarTransfered = taskTracker.downloadJar();
			try{
				Thread.sleep(1000);
				System.out.println("it is transferring jar File from jobtracker to tasktracker.");
			}catch(InterruptedException e){
				e.printStackTrace();	
			}
		}
		String [] div= Utility.getParam("Jar_Path").split("/");
		String newFilePath = getSystemTempDir()+File.separator+div[div.length-1];
		int jobID = taskInfo.getJobID();
		if( extractJobClassJar(jobID, newFilePath) == false){
			System.out.println("ExtractJobClassJar doesn't work in this Machine.");	
			return false;
		}

		/* if this is a mapper task */
		if (taskInfo.getType() == TaskMeta.TaskType.MAPPER) {
			MapperTaskInfo mapperTaskInfo = (MapperTaskInfo) taskInfo;
			/* lock the mapper counter */
			synchronized (taskTracker.mapperCounter) {

				/* if there is free mapper slots */
				if (taskTracker.mapperCounter < taskTracker.NUM_OF_MAPPER_SLOTS) {

					/* do some logging */
					System.out.println("task tracker " + this.taskTracker.getTaskTrackerName()
							+ " received runTask request taskid:" + taskInfo.getTaskID() + " "
							+ taskInfo.getType());

					/* increase number of mapper running on this task tracker */
					taskTracker.mapperCounter++;

					/* start new process */
					String[] args = new String[] { MapperWorker.class.getName(),
						String.valueOf(mapperTaskInfo.getTaskID()), mapperTaskInfo.getInputPath(),
						String.valueOf(mapperTaskInfo.getOffset()),
						String.valueOf(mapperTaskInfo.getBlockSize()), mapperTaskInfo.getOutputPath(),
						mapperTaskInfo.getMapper(), mapperTaskInfo.getPartitioner(),
						mapperTaskInfo.getInputFormat(), String.valueOf(mapperTaskInfo.getReducerNum()),
						taskTracker.getTaskTrackerName(), String.valueOf(taskTracker.getRPort()) };
					try {
						Utility.startJavaProcess(args, taskInfo.getJobID());
					} catch (Exception e) {
						e.printStackTrace();
					}
					return true;
				} else {
					return false;
				}
			}
		} else {
			/* this is a reducer task */
			ReducerTaskInfo reducerTaskInfo = (ReducerTaskInfo) taskInfo;

			/* lock the reducer counter */
			synchronized (taskTracker.reducerCounter) {
				/* if there is free reducer slots */
				if (taskTracker.reducerCounter < taskTracker.NUM_OF_REDUCER_SLOTS) {

					/* do some logging */
					System.out.println("task tracker " + this.taskTracker.getTaskTrackerName()
							+ " received runTask request taskid:" + taskInfo.getTaskID() + " "
							+ taskInfo.getType());

					/* increase the number of reducer running on this task tracker */
					taskTracker.reducerCounter++;

					/* start new process */
					String[] args = new String[] { ReducerWorker.class.getName(),
						String.valueOf(reducerTaskInfo.getTaskID()),
						String.valueOf(reducerTaskInfo.getOrderId()), reducerTaskInfo.getReducer(),
						reducerTaskInfo.getOutputFormat(), reducerTaskInfo.getInputPath(),
						reducerTaskInfo.getOutputPath(), taskTracker.getTaskTrackerName(),
						String.valueOf(taskTracker.getRPort()) };
					try {
						Utility.startJavaProcess(args, taskInfo.getJobID());
					} catch (Exception e) {
						e.printStackTrace();
					}
					return true;
				} else {
					return false;
				}
			}
		}
	}

	public boolean transfer(String path, String ttName) throws RemoteException{
		//Empty method, as no one will call this.	
		return false;
	}

  /**
   * This method is called by job tracker to assign task to task tracker
   * 
   * @param taskInfo
   *          : the information about the task
   * @return the task is submitted successfully or not
   */
	public boolean transferFolder(int jid, Map<Integer, TaskMeta> mapTasks, Map<String, TaskTrackerMeta> tasktrackers) throws RemoteException {

		/*
		 * since tasktracker doesn't distinguish the diff between maptask and reducetasks
		 * we have to refer to jobtrackers' info to differentiate them.
		 * if mapTasks has the taskID, then, we know this task is a maptask and also in this taskTracker.
		 */
		Map<Integer, TaskProgress> taskStatus = this.taskTracker.getTaskStatus();
		synchronized (taskStatus) {
			for(Entry<Integer, TaskProgress> entry: taskStatus.entrySet()){
				int key = entry.getKey();
				if(mapTasks.containsKey(key) == true){
					String taskMapperOutputPath = getSystemTempDir() + File.separator + "/mapper_output_task_" + jid
							 + "/mapper_output_task_" + key;

					if(sendFiles(taskMapperOutputPath, tasktrackers, key) == false)
						return false;
				}
			}
		}
		return true;
	}

	public boolean sendFiles(String folderPath, Map<String, TaskTrackerMeta> tasktrackers, int taskId){
		
		try{
		File file = new File(folderPath);
		if(!file.exists()) return true;

		/*
		 * if the file (part-X) exist, we send it to all tasktrackers.
		 */
		for(Entry<String, TaskTrackerMeta> entry: tasktrackers.entrySet()){
			TaskTrackerMeta ttm = entry.getValue();
			Runnable aInst = new FolderClient(folderPath, ttm.tthost, ttm.dataPort, ttm.msgPort);
			Thread t = new Thread(aInst);
			t.start();
			t.join();
		}
		}catch(Exception e){
			e.printStackTrace();
		}
		return true;
	}


	/**
	 * this method is called by task worker to update task status to task tracker
	 */
	public void update(Object statuspck) throws RemoteException {
		if (statuspck.getClass().getName() != TaskProgress.class.getName())
			return;

		TaskProgress taskProgress = (TaskProgress) statuspck;
		/* do some logging */
		System.out.println(System.currentTimeMillis() + " Receive update from worker "
				+ taskProgress.getTaskID() + " : " + taskProgress.getStatus());

		/* update the status to task tracker */
		Map<Integer, TaskProgress> taskStatus = this.taskTracker.getTaskStatus();
		synchronized (taskStatus) {
			taskStatus.put(taskProgress.getTaskID(), taskProgress);
		}
	}


	/**
	 * extract the jar file into the system's ClassPath folder
	 * 
	 * @param jobid
	 * @param jarpath
	 * @return
	 */
	public boolean extractJobClassJar(int jobid, String jarpath) {
		try {
			JarFile jar = new JarFile(jarpath);
			Enumeration enums = jar.entries();

			// find the path to which the jar file should be extracted
			String destDirPath =Utility.getParam("USER_CLASS_PATH") + File.separator + "job" + jobid + File.separator;
			File destDir = new File(destDirPath);
			if (!destDir.exists()) {
				destDir.mkdirs();

				// copy each file in jar archive one by one
				while (enums.hasMoreElements()) {
					JarEntry file = (JarEntry) enums.nextElement();

					File outputfile = new File(destDirPath + file.getName());
					if (file.isDirectory()) {
						outputfile.mkdirs();
						continue;
					}
					InputStream is = jar.getInputStream(file);
					FileOutputStream fos = null;
					try {
						fos = new FileOutputStream(outputfile);
					} catch (FileNotFoundException e) {
						outputfile.getParentFile().mkdirs();
						fos = new FileOutputStream(outputfile);
					}
					while (is.available() > 0) {
						fos.write(is.read());
					}
					fos.close();
					is.close();
				}
			}
		} catch (IOException e) {
			// TODO : handle this exception if the jar file cannot be found
			return false;
		}
		return true;
	}

	/**
	 * get the system's temporary dir which holds mapper's output
	 * 
	 * @return
	 */
	public static String getSystemTempDir() {
		String res = Utility.getParam("SYSTEM_TEMP_DIR");
		if (res.compareTo("") == 0)
			res = System.getProperty("java.io.tmpdir");

		File tmpdir = new File(res);
		if (!tmpdir.exists()) {
			tmpdir.mkdirs();
		}

    return res;
  }
}

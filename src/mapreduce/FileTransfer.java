package mapreduce;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Map;

/**
 * This interface is used for clients to send HeartBeat to the server, in a 
 * Server-Client model. Instead of sending message through socket, in this
 * project, we choose RMI to do communication in a way that the client call
 * the update() function of a remote StatusUpdater by RMI.
 */
public interface FileTransfer extends Remote {
	
	public boolean transfer(String path, String ttName) throws RemoteException;

	/**
	 * transferFolder is a method which transfers temp files outputed by mapper
	 * phase to reducerworkers.
	 */
	public boolean transferFolder(int orderID, Map<Integer, TaskMeta> mapTasks, Map<String, TaskMeta> tasktrackers) throws RemoteException;
}

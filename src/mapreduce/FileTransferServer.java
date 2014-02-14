package mapreduce;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Network File Transfer Application
 * Server.java
 * 
 * @author Liang  Tang
 * @date Feb 2014
 */
public class FileTransferServer implements Runnable{
	ServerSocket server = null;
	ServerSocket msgServer = null;
	int dataPort;
	int msgPort;

	public FileTransferServer(int dataPort, int msgPort) {
		this.dataPort = dataPort;
		this.msgPort = msgPort;
	}

	public void run() {
		try {
			server = new ServerSocket(dataPort);
			msgServer = new ServerSocket(msgPort);
			System.out.println("FileTransferServer listening...");
		} catch (IOException e) {
			System.out.println("Could not listen " + e);
			System.exit(-1);
		}
		while(true){

			ClientWorker w;
			try {
				w = new ClientWorker(server.accept(), msgServer.accept());
				//System.out.println("Client accepted.");
				Thread t = new Thread(w);
				t.start();
			} catch (IOException e) {
				System.out.println("Accept failed " + e);
			}
		}
	}
	protected void finalize() {
		// Objects created in run method are finalised when
		// program terminates and thread exits
		try {
			server.close();
			msgServer.close();
		} catch (IOException e) {
			System.out.println("Could not close socket");
			System.exit(-1);
		}
	}

	public static void main(String[] args) {
		//FileTransferServer prog = new FileTransferServer(4444, 4445);
		//prog.listenSocket();
	}
}

class ClientWorker implements Runnable {
	private Socket client;
	private Socket msg;

	BufferedReader msgIn = null;
	PrintWriter msgOut = null;

	BufferedInputStream in = null;
	BufferedOutputStream out = null;

	File file;

	ClientWorker(Socket client, Socket msg) {
		this.client = client;
		this.msg = msg;
	}


	public void receiveFile(File filename) {

		try {
			// If the file parent folder does not exist, create it.
			if (filename.getParentFile() != null) {
				if (!filename.getParentFile().exists()) {
					filename.getParentFile().mkdirs();
				}
			}

			in = new BufferedInputStream(client.getInputStream());
			out = new BufferedOutputStream(new FileOutputStream(filename));

			int c;
			while ((c = in.read()) != -1) {
				out.write(c);
			}
			out.flush();
			in.close();
			out.close();

		} catch (IOException e) {
			System.out.println(e + "......");
			e.printStackTrace();
		}
	}

	/**
	 * This can delete folders which have files within them.
	 * @param path Root File Path
	 * @return true if the file and all sub files/directories have been removed
	 * @throws FileNotFoundException
	 */
	public static boolean deleteRecursive(File path) throws FileNotFoundException{
		if (!path.exists()) throw new FileNotFoundException(path.getAbsolutePath());
		boolean ret = true;
		if (path.isDirectory()){
			for (File f : path.listFiles()){
				ret = ret && deleteRecursive(f);
			}
		}
		return ret && path.delete();
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

	public void cleanUp() {
		try {
			msgIn.close();
			msgOut.close();
		} catch (IOException e) {
			System.out.println(e + "......");
			e.printStackTrace();
		}
	}

	public void run() {
		String line;

		try {
			msgIn = new BufferedReader(new InputStreamReader(msg.getInputStream()));
			msgOut = new PrintWriter(msg.getOutputStream(), true);

			line = msgIn.readLine();
			String[] cmd = line.toString().split(":");

			if (line != null && cmd != null) {
				System.out.println(line);

				if (cmd[0].equals("x")) {
					System.out.println("Client exit");
				} else {
					if (cmd[0].equals("u") && cmd[2] != null) {

						//System.out.println("Receiving file from client");
						file = new File(cmd[2]);
						receiveFile(file);
						//System.out.println("after Receiving File");

						// Let client know it has been received
						msgOut.println("200");

					} else if (cmd[0].equals("l") && cmd[1] != null) {

						System.out.println("Listing directory");
						ObjectOutputStream oOut = new ObjectOutputStream(msg.getOutputStream());
						file = new File(cmd[1]);
						File[] list = file.listFiles();
						oOut.writeObject(list);

					} else if (cmd[0].equals("e") && cmd[1] != null) {

						System.out.println("Deleting");
						file = new File(cmd[1]);

						if (deleteRecursive(file) == true) {
							msgOut.println("200");
						} else {
							msgOut.println("550");
						}

					} else {
						// Let client know of syntax error
						msgOut.println("500");
					}
				}
			}
			client.close();
			msg.close();

		} catch (IOException e) {
			System.out.println(e + "......");
			e.printStackTrace();
		} finally {
			cleanUp();
		}
	}
}

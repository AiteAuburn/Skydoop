
import java.io.File;
import java.io.IOException;

public class Main implements Runnable{
	
	private FileWriter fw;

	public Main(FileWriter fw){
		
		this.fw = fw;	
	}
	
	public void run() {
		fw.append("111111\n");	
   /*     try{*/
		//Thread.sleep(1000);
		//}catch(Exception e){
			//e.printStackTrace();	
		/*}*/
	}

	public static void main(String[] args) throws IOException{
		
		File aFile = new File("a");
		FileWriter aFW = new AsyncFileWriter(aFile);
		((AsyncFileWriter)aFW).open();

		Thread one = new Thread(new Main(aFW) );
		Thread two = new Thread(new Main(aFW) );

		one.start();
		two.start();
		
		try{
			Thread.sleep(1000);
		}catch(Exception e){
			e.printStackTrace();	
		}
		aFW.close();
	}
}

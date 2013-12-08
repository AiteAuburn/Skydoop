package dist;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Arrays;
import java.util.Properties;
import java.util.Iterator;
import java.util.Map;
import java.util.HashSet;
import java.util.Set;

import java.awt.*;
import java.awt.geom.Rectangle2D;
import java.awt.geom.Point2D;
import java.awt.geom.Ellipse2D;
import javax.swing.JComponent;
import javax.swing.JFrame;
import javax.swing.JPanel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.*;
import com.romix.quickser.*;

import dist.HyperList;
import dist.HyperPlane;

public class Prune{
	
	public static final Logger log = LoggerFactory.getLogger(Prune.class);
	public static final boolean logEnable = true;
	public static final int testArea = 2;
	public static ArrayList< HashMap<Integer, HashMap<Integer, instance.point>> > outputLists;
	public static boolean[] objectPrune;
	public static ArrayList<Integer> objectAfterPrune1;
	public static HashMap<Integer, item> hashItem;

	public static HashMap<Integer,instance> instanceSet ;
	public static HashMap<Integer,instance> notSkySet ;

	public List<item> listItem;
	public List<item> afterPrune1List;
	public static int dim;
	public static int MaxObjNum;
	public static HashMap<Integer, ArrayList<instance.point> > PruneObject;
	public static double[] splitValue = {0.111, 0.222, 0.333, 0.555, 0.777, 1.0};
	public static Boolean [] freq;

	Prune(){
		preprocess();	
		readFile();
	}

	public void readFile(){

		try{
		MyInputStream MIPS = new MyInputStream();
		MIPS.setName(Integer.toString(testArea) +  ".txt");
		MIPS.setDim(dim);
		MIPS.read(listItem /* , hashItem */ );
		MIPS.close();

		FileInputStream input = new FileInputStream(new File("MAX_MIN"));
		ObjectInputStream in = new ObjectInputStream(input);

		outputLists = ( ArrayList< HashMap<Integer, HashMap<Integer, instance.point>> >)in.readObject();
		in.close();
		input.close(); 
		}catch(ClassNotFoundException cnfe){ cnfe.printStackTrace();  }
		 catch(IOException e){ e.printStackTrace(); }
		
		if(logEnable){
			System.out.println("the outputlists size = "+ outputLists.size() );
			for(int i=0; i<outputLists.size() ;i++){
				HashMap<Integer, HashMap<Integer, instance.point >> HM = outputLists.get(i);				
				Iterator iter = HM.entrySet().iterator();
				while(iter.hasNext()){
					Map.Entry obj = (Map.Entry) iter.next();
					//System.out.println("The map key = "+obj.getKey() );
					HashMap<Integer, instance.point> mapPoint =(HashMap<Integer, instance.point>) obj.getValue();
					
					Iterator iter_2 = mapPoint.entrySet().iterator();
					while(iter_2.hasNext()){
						Map.Entry obj_2 = (Map.Entry) iter_2.next();
						//if( (int) obj.getKey()==0)
						System.out.println("The object id = "+obj_2.getKey()+"   "+ ((instance.point)obj_2.getValue()).toString()  );
							
					}

				}
			}
		}
	}

	public void preprocess(){

		objectAfterPrune1 = new ArrayList<Integer> ();
		listItem = new ArrayList<item>();
		afterPrune1List = new ArrayList<item>();

		hashItem = new HashMap<Integer, item>();
		PruneObject = new HashMap<Integer, ArrayList<instance.point>> ();
		Properties prop = new Properties();
		
		try {
			prop.load(new FileInputStream("config.properties"));;

		}catch (IOException ex) {
			ex.printStackTrace();
		}
		MaxObjNum = Integer.parseInt(prop.getProperty("objectNum"));
		dim = Integer.parseInt(prop.getProperty("dim"));
		objectPrune = new boolean[MaxObjNum];
	}

	public  void rule1( ){

		int statisTrue = 0;
		freq = new Boolean[MaxObjNum];
		Arrays.fill(freq,false);

		for(int i=0; i<outputLists.size(); i++){

			HashMap<Integer, HashMap<Integer, instance.point >> oneArea = outputLists.get(i);				
			Iterator iter = oneArea.entrySet().iterator();
			while(iter.hasNext()){
				Map.Entry obj = (Map.Entry) iter.next();	
				if( (int)obj.getKey() == 0 )continue;

				HashMap<Integer, instance.point> mapPoint = ( HashMap <Integer, instance.point>)obj.getValue();
				
				Iterator iter_max = mapPoint.entrySet().iterator();
				while(iter_max.hasNext()){
					Map.Entry obj_max = (Map.Entry) iter_max.next();	

					int max_id = (int) obj_max.getKey();
					if(freq[max_id] == true ) continue;

					instance.point max_value = (instance.point)obj_max.getValue();	
					//System.out.println(" MAX = " + max_id+ "     "+max_value.toString()  );	

					HashMap<Integer, instance.point> minPoint = oneArea.get(0);
					Iterator iter_min = minPoint.entrySet().iterator();
					while(iter_min.hasNext()){
						Map.Entry obj_min = (Map.Entry) iter_min.next();	
						int min_id = (int)obj_min.getKey();
						if( min_id==max_id || freq[min_id] == true ) continue;

						instance.point min_value = (instance.point)obj_min.getValue();	
						//System.out.println("MIN = "+ min_id+ "     "+min_value.toString()  );	

						if(max_value.DominateAnother(min_value)) {
						
							if(logEnable){
								//System.out.println(min_id+" "+min_value.toString()+ "  dominate "+max_id+ "  "+max_value.toString()  );	
							}
							freq[min_id] =true;	
						}
					}
				}
			
			}
		}
		int i = 0;
		if(logEnable == true){	
			for(i=0; i<MaxObjNum; i++){
				if(freq[i] == true)	statisTrue++;
				if(freq[i] == false){

				int ret_index = listItem.indexOf(new item(i));
				if(ret_index == -1){
					//System.out.println("Something wrong happened in finding unpruned object in listItem.");
					continue;
				}
				//else
					//System.out.println("the index return = " + ret_index);

				afterPrune1List.add(listItem.get(ret_index));

				objectAfterPrune1.add(i);
				PruneObject.put(i,new ArrayList<instance.point>() );
				for(int j=0; j<outputLists.size(); j++){

					HashMap<Integer, HashMap<Integer, instance.point >> oneArea = outputLists.get(j);
					HashMap<Integer, instance.point> min_area = (HashMap<Integer, instance.point>) oneArea.get(0);				
					HashMap<Integer, instance.point> max_area = (HashMap<Integer, instance.point>) oneArea.get(1);				
					ArrayList<instance.point> adding = (ArrayList<instance.point>) PruneObject.get(i);
					if(min_area.containsKey(i)){ adding.add(min_area.get(i) ); }
					if(max_area.containsKey(i)){ adding.add(max_area.get(i) ); }

				}
			}
			}
			System.out.println("True rule 1 pruning = "+ statisTrue + " The original No. = "+MaxObjNum);
			System.out.println("After Rule 1, unpruned object num  = "+ afterPrune1List.size() );
			//new GameFrame();
		}
	}

	public void rule2(){

		instanceSet = new HashMap<Integer, instance>() ;
		notSkySet = new HashMap<Integer, instance>() ;

		for(int ii =0; ii<listItem.size(); ii++){
			item iterItem = listItem.get(ii);

			//if(logEnable){
			
				//for(int k=0; k<iterItem.instances.size(); k++)
				//{
					//instance inst = iterItem.instances.get(k);
					//System.out.println( inst.toString() );	
				//}
			//}

			if(freq[iterItem.objectID] ) {continue;}
			
			HashMap<Integer, instance.point > oneArea_max = outputLists.get(testArea).get(1);				
			
			for(int i=0; i<objectAfterPrune1.size(); i++){
				
				if( oneArea_max.containsKey(objectAfterPrune1.get(i) ) ){
					
					for( int j=0; j<iterItem.instances.size(); j++ ){

						instanceSet.put( iterItem.instances.get(j).instanceID, iterItem.instances.get(j) );
						if( oneArea_max.get( objectAfterPrune1.get(i) ).DominateAnother( iterItem.instances.get(j).a_point   )    )
						{
							iterItem.instances.get(j).IfSkyline = false;
							notSkySet.put( iterItem.instances.get(j).instanceID, iterItem.instances.get(j) );
						}
					}
				}
			}
		}
		System.out.println("total instances size = "+ instanceSet.size()  + " notSkySet size = "+ notSkySet.size()  );
		//new GameFrame(true);
	}

	public void rule3(){
		
	}

	public static class GameFrame extends JFrame {

		public static void AddingPoints(Squares squares){

			//Iterator iter = notSkySet.entrySet().iterator();
			Iterator iter = instanceSet.entrySet().iterator();
			while(iter.hasNext()){
				Map.Entry obj = (Map.Entry) iter.next();	
			
				if(  notSkySet.containsKey(obj.getKey())  ) continue;	
				int instanceID = (int) obj.getKey();
				instance a_instance = (instance) obj.getValue();

				assert(a_instance.dimension == 2):"The size of Arraylist doesn't match";
				
				double a_x = a_instance.a_point.__coordinates[0];
				double a_y = a_instance.a_point.__coordinates[1];
					

				System.out.println(" a_x = "+a_x + "  a_y ="+a_y  );

				squares.addPoints(a_x*600, Squares.PREF_W- a_y*600 );
				}
				
		}				

		public static void AddingRectangle(Squares squares){
			Iterator iter = PruneObject.entrySet().iterator();

			while(iter.hasNext()){
				Map.Entry obj = (Map.Entry) iter.next();	
			
				ArrayList<instance.point> adding = (ArrayList<instance.point>) obj.getValue();
				assert(adding.size() == 2):"The dimension of data is not suitable for java Swing. ";
				
				double a_x = adding.get(0).__coordinates[0]*600;
				double a_y = adding.get(0).__coordinates[1]*600;
				double b_x = adding.get(1).__coordinates[0]*600;
				double b_y = adding.get(1).__coordinates[1]*600;
					
				System.out.println("a_x = " +a_x+" a_y = "+a_y+ " b_x = "+b_x +" b_y = "+b_y    );

				if(adding.get(0).DominateAnother(adding.get(1) )  )	
					squares.addRectangle(a_x, Squares.PREF_W-a_y-(b_y-a_y), b_x-a_x, (b_y-a_y) );
				else
					squares.addRectangle(b_x, Squares.PREF_W-b_y-(a_y-b_y), a_x-b_x, (a_y-b_y) );


				}
				
		}				

		public GameFrame() {
			super("Game Frame");
			setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
			Squares squares = new Squares();
			getContentPane().add(squares);
			//for (int i = 0; i < 15; i++) {
				//squares.addRectangle( i * 10.0, i * 10.0, 100.0, 100.0);
			//}

			AddingRectangle(squares);

			pack();
			setLocationRelativeTo(null);
			setVisible(true);

		}
		
		public GameFrame(boolean prune_2) {
			super("Game Frame");
			if(prune_2 == true){
				setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
				Squares squares = new Squares();
				getContentPane().add(squares);
				
				AddingRectangle(squares);
				AddingPoints(squares);

				pack();
				setLocationRelativeTo(null);
				setVisible(true);
			}
		    else {
				setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
				Squares squares = new Squares();
				getContentPane().add(squares);

				AddingRectangle(squares);

				pack();
				setLocationRelativeTo(null);
				setVisible(true);
			}
		}
	}

	public static class Squares extends JPanel {
		private static final int PREF_W = 800;
		private static final int PREF_H = PREF_W;
		private List<Rectangle> squares = new ArrayList<Rectangle>();
		private List<Rectangle2D.Double> Rectangles = new ArrayList<Rectangle2D.Double>();
		private List<Ellipse2D.Double> ellipses = new ArrayList<Ellipse2D.Double>();
		
		public void addSquare(int x, int y, int width, int height) {
			Rectangle rect = new Rectangle(x, y, width, height);
			squares.add(rect);
		}

		public void addRectangle(double x, double y, double w, double h) {
			Rectangle2D.Double rect = new Rectangle2D.Double(x, y, w, h);
			Rectangles.add(rect);
		}

		public void addPoints(double x, double y ) {
			Ellipse2D.Double el = new Ellipse2D.Double(x,y,10.0,6.0  );
			ellipses.add(el);
		}

		@Override
			public Dimension getPreferredSize() {
				return new Dimension(PREF_W, PREF_H);
			}

		@Override
			protected void paintComponent(Graphics g) {
				super.paintComponent(g);
				Graphics2D g2 = (Graphics2D) g;
				for (Rectangle2D rect : Rectangles) {
					g2.draw(rect);
				}
				for (Ellipse2D el : ellipses) {
					g2.draw(el);
				}
			}
	}

	public static void main(String[] args){
		
		Prune pr = new Prune();
		pr.rule1();
		pr.rule2();	

		HyperList HL = new HyperList(5,dim); // first parameter: split num, second: dim
		
		for(int i=0; i<HL.splitNum; i++){
			HyperPlane HP =  new HyperPlane(dim);
			HP.setXCoef(splitValue[i]);
			HL.hyperList.add(HP);
		}

		divide dv = new divide(pr.afterPrune1List, HL.hyperList.get(0), HL.hyperList.get(1));
		dv.compProb(5);
		//dv.computeNaiveProb();

	}
}

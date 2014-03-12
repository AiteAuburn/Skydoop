package ProbSkyline.IO;

import ProbSkyline.DataStructures.instance;
import ProbSkyline.DataStructures.item;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.FileNotFoundException;

import java.util.List;

public class TextInstanceReader{

	private String inputFile;
	private BufferedReader br;
	private int dim;

	public TextInstanceReader(String inputFile) throws IOException {
		this.inputFile = inputFile;
		initialize();
	}

	public void setDim(int a_dim){
		dim = a_dim;	
	}

	public String readLine()throws FileNotFoundException, IOException {
		
		return br.readLine();	
	}

    public void readListItem(List<item> listItem) throws FileNotFoundException, IOException{
	
		int currObject = 0;
		listItem.add(0, new item(currObject));
		//hashItem.put(0, new item(currObject));
		String line = this.readLine();
		while(line !=null){
			String [] parts = line.split(" ");
			if(Integer.parseInt(parts[0]) == currObject  ){
				
			}
			else {
				currObject = Integer.parseInt(parts[0]);
				assert(currObject == Integer.parseInt(parts[0])):"the object ID doesn't match";
				listItem.add( new item(currObject));	
				//hashItem.put(Integer.parseInt(parts[0]), new item(currObject)  );
			}	

			listItem.get( listItem.size()-1 ).addInstance(parts,dim);	
			//hashItem.get(currObject).addInstance(parts,dim);
			line = this.readLine();
		}	
	}

	private void initialize() throws IOException {
		br = new BufferedReader(new FileReader(inputFile));
	}

	public void close() throws IOException {
		br.close();
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
}

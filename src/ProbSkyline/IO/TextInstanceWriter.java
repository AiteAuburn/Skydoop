package org.liang.IO;

import org.liang.DataStructures.instance;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;


public class TextInstanceWriter{

	private String outputFile;
	private BufferedWriter bw;
	private String tmpSuffix;
	private ObjectOutputStream outStream;

	public TextInstanceWriter(String outputFile) throws IOException {
		this.outputFile = outputFile;
		this.tmpSuffix = ".txt";
		initialize();
	}

	public TextInstanceWriter(String outputFile, boolean writeObject) throws IOException {
		this.outputFile = outputFile;
		this.tmpSuffix = ".txt";
		FileOutputStream fileOut = new FileOutputStream("Max_MIN");
		outStream = new ObjectOutputStream(fileOut);

	}

    public void write(String a_string ) throws IOException{
        bw.write(a_string);    
    }    
    
    public void write(instance a_inst ) throws IOException
    {
        String temp = Integer.toString(a_inst.objectID) + " "+ Integer.toString(a_inst.instanceID)+" ";
        temp += a_inst.a_point.toString();
        temp += Double.toString(a_inst.prob)+"\n";

        this.write(temp);
    }    
	

	public void write(Object _object) throws IOException{
		
		outStream.writeObject(_object);	
	}

	private void initialize() throws IOException {
		bw = new BufferedWriter(new FileWriter(outputFile + tmpSuffix));
	}

	public void close() throws IOException {
		bw.flush();
		bw.close();
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
}

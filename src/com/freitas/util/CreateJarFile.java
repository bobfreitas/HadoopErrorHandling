package com.freitas.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

/*
 * This class is used to create a JAR file from the current project to be used when 
 * submitting the job.  The classes used in the mapper and reducer need to be in a
 * JAR.  If changes are made to those classes then this will need to be invoked to 
 * recreate the JAR that gets submitted at runtime.  It will create the jobsubmit.jar
 * and place it in the lib directory where it is already on the classpath.  
 */
public class CreateJarFile {

	public static int BUFFER_SIZE = 10240;
	
	public static void readDirectory(String path, List<String> saveFiles) {
		
		if (path.compareTo(".") == 0 || path.compareTo("..") == 0) {
			return;
		}
		
		File f = new File(path);
		if (f.isFile()) {
			saveFiles.add(f.getPath());
			return;
		}
		// must be a directory, need to traverse
		else {
			String[] ff = f.list();
			for (int i = 0; i < ff.length; i++) {
				readDirectory(path + File.separator + ff[i], saveFiles);
			}
		}
	}
	

	public static void createJarArchive(File archiveFile, List<String> tobeJared) {
		try {
			byte buffer[] = new byte[BUFFER_SIZE];
			// Open archive file
			FileOutputStream stream = new FileOutputStream(archiveFile);
			JarOutputStream out = new JarOutputStream(stream, new Manifest());
			
			for(String fqFileStr : tobeJared){
				File file = new File(fqFileStr);
				if (file == null || !file.exists() || file.isDirectory()){
					continue;
				}
				// adjust the path to remove the bin
				String nameInJar = fqFileStr.substring(4, fqFileStr.length());
				System.out.println("Adding " + nameInJar);
				// create an entry in the jar
				JarEntry jarAdd = new JarEntry(nameInJar);
				jarAdd.setTime(file.lastModified());
				out.putNextEntry(jarAdd);
				// write the class file to the jar
				FileInputStream in = new FileInputStream(file);
				while (true) {
					int nRead = in.read(buffer, 0, buffer.length);
					if (nRead <= 0)
						break;
					out.write(buffer, 0, nRead);
				}
				in.close();
			}
			out.close();
			stream.close();
			System.out.println("Adding completed OK");
		} catch (Exception ex) {
			ex.printStackTrace();
			System.out.println("Error: " + ex.getMessage());
		}
	}
	
	
	public static void main(String[] args) throws Exception {
		
		String archiveFilePath = "lib" + File.separator + "jobsubmit.jar";
		File archiveFile = new File(archiveFilePath);
		List<String> tobeJared = new ArrayList<String>();
		readDirectory("bin", tobeJared);
		createJarArchive(archiveFile, tobeJared) ;
		
		System.out.println("");
	}
	

}

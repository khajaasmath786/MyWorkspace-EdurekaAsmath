package com.mcd.gdw.daas.util;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

public class HDFSUtil {

	public static final String DAAS_MAPRED_FILE_SEPARATOR_CHARACTER = "daas.mapred.fileSeparatorCharacter";
	public static final String DAAS_MAPRED_TERR_FIELD_POSITION = "daas.mapred.terrFieldPosition";
	public static final String DAAS_MAPRED_LCAT_FIELD_POSITION = "daas.mapred.lcatFieldPosition";
	public static final String DAAS_MAPRED_BUSINESSDATE_FIELD_POSITION = "daas.mapred.busnDtFieldPosition";
	
	public static final String CORE_SITE_XML_FILE = "core-site.xml";
	public static final String HDFS_SITE_XML_FILE = "hdfs-site.xml";
	public static final String MAPRED_SITE_XML_FILE = "mapred-site.xml";
	
	public static final String FILE_PART_SEPARATOR = "~";
	
	private static final String SPECIAL_CHARACTER_PREFIX = "RxD";

	private static String hdfsSetUpDir = null;

	public static String getHdfsSetUpDir() {
		return hdfsSetUpDir;
	}
	
	public static void setHdfsSetUpDir(String hdfsSetUpDir) {
		HDFSUtil.hdfsSetUpDir = hdfsSetUpDir;
	}
	
	public static boolean updHdfsConfig(String hdfsSetupDir, Configuration config) {

		boolean retValue = true; 
		
		if ( ! new File(hdfsSetupDir + File.separator + CORE_SITE_XML_FILE).exists() ) {
        	System.err.println("Missing Hadoop Configuration file " + CORE_SITE_XML_FILE +  " from " + hdfsSetupDir );
        	retValue = false;
        }

        if ( ! new File(hdfsSetupDir + File.separator + HDFS_SITE_XML_FILE).exists() ) {
        	System.err.println("Missing Hadoop Configuration file " + HDFS_SITE_XML_FILE +  " from " + hdfsSetupDir );
        	retValue = false;
        }

        if ( ! new File(hdfsSetupDir + File.separator + MAPRED_SITE_XML_FILE).exists() ) {
        	System.err.println("Missing Hadoop Configuration file " + MAPRED_SITE_XML_FILE +  " from " + hdfsSetupDir );
        	retValue = false;
        }
        
        if ( retValue ) {
        	config.addResource(new Path(hdfsSetupDir + File.separator + CORE_SITE_XML_FILE));  
        	config.addResource(new Path(hdfsSetupDir + File.separator + HDFS_SITE_XML_FILE));  
        	config.addResource(new Path(hdfsSetupDir + File.separator + MAPRED_SITE_XML_FILE));
        }

	    //***************************************************
	    //**
	    //* Only needed for multiple treaded applications
	    //*
	    //hdfsConfig.setBoolean("fs.hdfs.impl.disable.cache", true);
	    //**
	    //***************************************************
		
		return(retValue);
	}
	
	public static Configuration getHdfsConfiguration () {
		
        if ( ! new File(hdfsSetUpDir + File.separator + CORE_SITE_XML_FILE).exists() ) {
        	System.err.println("Missing Hadoop Configuration file " + CORE_SITE_XML_FILE +  " from " + hdfsSetUpDir );
        }

        if ( ! new File(hdfsSetUpDir + File.separator + HDFS_SITE_XML_FILE).exists() ) {
        	System.err.println("Missing Hadoop Configuration file " + HDFS_SITE_XML_FILE +  " from " + hdfsSetUpDir );
        }

        if ( ! new File(hdfsSetUpDir + File.separator + MAPRED_SITE_XML_FILE).exists() ) {
        	System.err.println("Missing Hadoop Configuration file " + MAPRED_SITE_XML_FILE +  " from " + hdfsSetUpDir );
        }

		Configuration hdfsConfig = new Configuration();
		
	    hdfsConfig.addResource(new Path(hdfsSetUpDir + File.separator + CORE_SITE_XML_FILE));  
	    hdfsConfig.addResource(new Path(hdfsSetUpDir + File.separator + HDFS_SITE_XML_FILE));  
	    hdfsConfig.addResource(new Path(hdfsSetUpDir + File.separator + MAPRED_SITE_XML_FILE));  

	    //***************************************************
	    //**
	    //* Only needed for multiple treaded applications
	    //*
	    //hdfsConfig.setBoolean("fs.hdfs.impl.disable.cache", true);
	    //**
	    //***************************************************
	    
	    return hdfsConfig;
	}
	
	public static FileSystem getHdfsFileSystem () {
		FileSystem hdfsFileSystem = null;
		try{
		
		
		try {
			URI uri = new URI("hdfs://192.65.208.60:8020");
			hdfsFileSystem = FileSystem.get(uri,getHdfsConfiguration());
			
		} catch (IOException ex) {
			ex.printStackTrace();
			return null;
		}
		}catch(Exception ex){
			ex.printStackTrace();
		}
		return hdfsFileSystem;
	 }

	public static void createHdfsSubDirIfNecessary(FileSystem fs
                                                  ,Path hdfsPath
                                                  ,Boolean echoMsg) {

		try {
			if ( ! fs.exists(hdfsPath) ) {
				if ( FileSystem.mkdirs(fs, hdfsPath,new FsPermission(org.apache.hadoop.fs.permission.FsAction.ALL,org.apache.hadoop.fs.permission.FsAction.ALL,org.apache.hadoop.fs.permission.FsAction.READ_EXECUTE)) ) {
					if ( echoMsg ) { 
						System.out.println("Created HDFS Path: " + hdfsPath.toString());
					}
				} else {
					System.err.println("Create HDFS Path: " + hdfsPath.toString() + " failed.");
					System.exit(8);
				}
			}
		} catch (Exception ex) {
			System.err.println("Create HDFS Path: " + hdfsPath.toString() + " failed.");
			System.err.println(ex.toString());
			System.exit(8);
		}
	}

	public static void removeHdfsSubDirIfExists(FileSystem fileSystem
                                                ,Path hdfsPath
                                                ,Boolean echoMsg) {

		try {
			if ( fileSystem.exists(hdfsPath) ) {
				fileSystem.delete(hdfsPath,true);
				
				if ( echoMsg ) {
					System.out.println("Removed HDFS Path: " + hdfsPath.toString());
				}
			}
		} catch (Exception ex) {
			System.err.println("Remove HDFS Path: " + hdfsPath.toString() + " failed.");
			System.err.println(ex.toString());
			System.exit(8);
		}
	}

	public static String replaceMultiOutSpecialChars(String fromValue) {

		String retValue = "";

		for ( int idx=0; idx < fromValue.length(); idx ++ ) {
			if ( (fromValue.toUpperCase().charAt(idx) >= 'A' && fromValue.toUpperCase().charAt(idx) <= 'Z') || 
			     (fromValue.charAt(idx) >= '0' && fromValue.charAt(idx) <= '9') ) {
				retValue += fromValue.substring(idx, idx+1);
			} else {
				retValue += SPECIAL_CHARACTER_PREFIX + String.format("%03d",(int)fromValue.charAt(idx));
			}
		}
		
		return(retValue);
	}

	public static String restoreMultiOutSpecialChars(String fromValue) {

		String retValue = "";
		
		int pos = 0;
		int foundPos = -1;
		
		foundPos = fromValue.indexOf(SPECIAL_CHARACTER_PREFIX, pos);
		
		while ( foundPos != -1 ) {
			if ( foundPos > 0 ) {
				retValue += fromValue.substring(pos, foundPos);
			}
			
			int charValue = Integer.parseInt(fromValue.substring(foundPos+3, foundPos+6));
			
			retValue += String.valueOf((char)charValue);
			
			pos = foundPos+SPECIAL_CHARACTER_PREFIX.length()+3;
			
			foundPos = fromValue.indexOf(SPECIAL_CHARACTER_PREFIX, pos);
		}
		
		if ( pos < fromValue.length() ) {
			retValue += fromValue.substring(pos, fromValue.length());
		}

		return(retValue);
	}
	
	
	public static Calendar validDate(String inDt) {
		
	    String ckDt = "";
	    Calendar calDt;
	    Calendar retDt = new GregorianCalendar(0001, 01, 01); 
	    String[] dtParts;
	    int dtYear;
	    int dtMonth;
	    int dtDay;
	    String tmpDt;
	    SimpleDateFormat date_format = new SimpleDateFormat("yyyy-MM-dd");

	    if ( inDt.toUpperCase().startsWith("C") ) {
	    	inDt = offsetDt(inDt);
	    }
	    
	    if ( inDt.length() == 8 ) {
	      ckDt = inDt.substring(0,4) + '-' + inDt.substring(4,6) + '-' + inDt.substring(6,8);
	    } else {
	      ckDt = inDt;
	    }

	    if ( ckDt.length() == 10 ) {
	      dtParts = (ckDt + "--").split("-");

	      try {
	        dtYear = Integer.parseInt(dtParts[0]);
	        dtMonth = -1;

	        switch ( Integer.parseInt(dtParts[1]) ) {
	          case  1: dtMonth = Calendar.JANUARY;
	                   break;
	          case  2: dtMonth = Calendar.FEBRUARY;
	                   break;
	          case  3: dtMonth = Calendar.MARCH;
	                   break;
	          case  4: dtMonth = Calendar.APRIL;
	                   break;
	          case  5: dtMonth = Calendar.MAY;
	                   break;
	          case  6: dtMonth = Calendar.JUNE;
	                   break;
	          case  7: dtMonth = Calendar.JULY;
	                   break;
	          case  8: dtMonth = Calendar.AUGUST;
	                   break;
	          case  9: dtMonth = Calendar.SEPTEMBER;
	                   break;
	          case 10: dtMonth = Calendar.OCTOBER;
	                   break;
	          case 11: dtMonth = Calendar.NOVEMBER;
	                   break;
	          case 12: dtMonth = Calendar.DECEMBER;
	                   break;
	        }
	      
	        dtDay = Integer.parseInt(dtParts[2]);
	        calDt = new GregorianCalendar(dtYear, dtMonth, dtDay);
	        tmpDt = date_format.format(calDt.getTime());

	        if ( ckDt.equals(tmpDt) ) {
	          retDt = calDt;
	        } 
	      } catch (Exception ex) {
	      }
	    }

	    return(retDt);
	}

	private static String offsetDt(String dt) {
		
		Calendar cal = Calendar.getInstance();
		SimpleDateFormat fmt = new SimpleDateFormat("yyyyMMdd");
		
		int offset=0;
		
		try {
			offset = Integer.parseInt(dt.substring(1)) * -1;
		} catch (Exception ex) {
			offset=0;
		}
		
		cal.add(Calendar.DATE, offset);
		
		return(fmt.format(cal.getTime()).toString());
		
	}

}
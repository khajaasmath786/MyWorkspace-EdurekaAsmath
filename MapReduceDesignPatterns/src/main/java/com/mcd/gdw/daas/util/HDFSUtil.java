/*
##########################################################################
# File Name:      		HDFS Util   
# Project Name:    		AWS Migration    
# Written By:   		KhajaAsmath Mohammed        
# Date Created:         2016/04/11
#
# Description:    		This file contains utility functions that are used within Daas Project
#               
#
# Assumptions:   		None     
#
#
# Parameters:     		None       

#Change Log:
#                    Date Modified:         2016/04/11
#                    Developer Name:    	KhajaAsmath		    
#                    Description: 			AWS Migration. Added new property which will be used inside the mapper and reducer to read the S3Bucket instead of 
											Hadoop file system. This property is backward compatible to reach from hadoop fs too.
#
##########################################################################
*/

package com.mcd.gdw.daas.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.*;
//AWS START
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
//AWS END
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
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
    //AWS START
	public static boolean renameWithRetry(FileSystem fs, Path fromPath, Path toPath, boolean removeSource, int retries) {
		boolean moveSuccessful = false;

		// retry N number of if move fails
		for (int i=0; i<=retries; i++) {
			try {
				if (!fs.exists(toPath)) {
					if (fs.rename(fromPath, toPath)) {
//						System.out.println(" moved " + fromPath.toString() + " to " + toPath.toString());
                        moveSuccessful = true;
						break;//mc41946
					} else {
						System.out.println("could not move " + fromPath.toString() + " to " + toPath.toString());
                        moveSuccessful = false;
					}
				} else {
					System.out.println(" target file " + toPath.toString() + " already exists");
					if (removeSource && fs.exists(fromPath)) {
                        System.out.println(" removing source file " + fromPath.toString() + " since target already exists");
						fs.delete(fromPath, true);
					}
                    moveSuccessful = true;
					break;//mc41946
				}
			} catch (Exception ex1) {
				System.err.println("renameWithRetry failed on " + i + " retry attempt for " + fromPath.toString() + " to " + toPath.toString());
				ex1.printStackTrace(System.err);
                moveSuccessful=false;
			}

			Random ran = new Random();
			int x = ran.nextInt(10) + 5;
			System.err.println("renameWithRetry retry (wait " + x + " sec) " + fromPath.toString() + " to " + toPath.toString());
			try {
				Thread.sleep(x*1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		} // end for

		return moveSuccessful;
	}
    //AWS END
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
	
	public static Path[] requestedArgsPaths(FileSystem fileSystem
			                               ,DaaSConfig daasConfig
                                           ,String requestArgs
                                           ,String... subFileTypes) throws Exception {
		
		Path[] retPath = null;
		String addPath = "";

		if (subFileTypes.length == 0 ) {
			return(null);
		}

		ArrayList<String> allTerrCodes = new ArrayList<String>();
		
		Path listPath = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsFinalSubDir() + Path.SEPARATOR + daasConfig.fileSubDir() + Path.SEPARATOR + subFileTypes[0]);
		
		FileStatus[] fstus = fileSystem.listStatus(listPath);
		
		for (int idx=0; idx < fstus.length; idx++ ) {
			allTerrCodes.add(fstus[idx].getPath().getName());
		}
		
		ArrayList<String> terrCodes; 
		
	    String[] listParts;
	    String[] argParts;
	    
	    Calendar calFromDt = Calendar.getInstance();
	    Calendar calToDt = Calendar.getInstance();
	    Calendar calInvalidDt = new GregorianCalendar(0001, 01, 01);
	    Calendar calIdxDt;
	    SimpleDateFormat date_format2 = new SimpleDateFormat("yyyyMMdd");

	    HashMap<String,Integer> pathMap = new HashMap<String,Integer>();
	    
	    listParts = requestArgs.split(",");

	    for ( int idxList=0; idxList < listParts.length; idxList++ ) {

	    	argParts = (listParts[idxList]).split(":");
	      
	    	if ( argParts[0].equals("*") ) {
	    		terrCodes = allTerrCodes;
	    	} else {
	    		terrCodes = new ArrayList<String>();
	    		terrCodes.add(argParts[0]);
	    	}
	      
	    	for (String terrCd : terrCodes ) {

	    		if ( argParts.length >= 2 ) {
	    			calFromDt = validDate(argParts[1]);
	    		} else {
	    			calFromDt = calInvalidDt;
	    		}
			      
	    		if ( argParts.length == 2 ) {
	    		  calToDt = validDate(argParts[1]);
	    		} else {
	    			if ( argParts.length == 3 ) {
	    				calToDt = validDate(argParts[2]);
	    			} else {
	    				calToDt = calInvalidDt;
	    			}
	    		}

	    		if ( calFromDt.compareTo(calInvalidDt) == 0 || calToDt.compareTo(calInvalidDt) == 0 ) {
	    			System.err.println("Ignoring: Invalid from/to date for subdirectory parameter = " + listParts[idxList] );
	    		} else {
	    			if ( calFromDt.compareTo(calToDt) > 0 ) {
	    				System.out.println("Ignoring: Invalid from date after to date for subdirectory parameter = " + listParts[idxList] );
	    			} else {
	    				calIdxDt = calFromDt;
	    				
	    				while ( calIdxDt.compareTo(calToDt) <= 0 ) {

	    					for (int idx1=0; idx1 < subFileTypes.length; idx1++ ) {
	    						addPath = daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsFinalSubDir() + Path.SEPARATOR + daasConfig.fileSubDir() + Path.SEPARATOR + subFileTypes[idx1] + Path.SEPARATOR + terrCd + Path.SEPARATOR + date_format2.format(calIdxDt.getTime());	
			    			
	    						if  ( pathMap.containsKey(addPath) ) {
	    							pathMap.put(addPath, (int)pathMap.get(addPath)+1);
	    						} else {
	    							pathMap.put(addPath, 1);
	    						}
	    					}
					
	    					calIdxDt.add(Calendar.DATE, 1);
	    				}
	    			}
	    		}
	    	}
	    }
	    
	    if ( pathMap.size() > 0 ) {

	    	int idx = 0;
	    	retPath = new Path[pathMap.size()];
	    	
	    	for ( Map.Entry<String, Integer> entry : pathMap.entrySet()) { 
	    		retPath[idx] = new Path(entry.getKey());
	    		idx++;
	    	}
	    }
	    
	    return(retPath);
	}
	
	public static Path[] requestedArgsPaths(FileSystem fileSystem
			                               ,DaaSConfig daasConfig
                                           ,String requestArgs
                                           ,ArrayList<String> subFileTypes) throws Exception {
		
		Path[] retPath = null;
		String addPath = "";

		if (subFileTypes.size() == 0 ) {
			return(null);
		}

		ArrayList<String> allTerrCodes = new ArrayList<String>();
		
		Path listPath = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsFinalSubDir() + Path.SEPARATOR + daasConfig.fileSubDir() + Path.SEPARATOR + subFileTypes.get(0));
		
		FileStatus[] fstus = fileSystem.listStatus(listPath);
		
		for (int idx=0; idx < fstus.length; idx++ ) {
			allTerrCodes.add(fstus[idx].getPath().getName());
		}
		
		ArrayList<String> terrCodes; 
		
	    String[] listParts;
	    String[] argParts;
	    
	    Calendar calFromDt = Calendar.getInstance();
	    Calendar calToDt = Calendar.getInstance();
	    Calendar calInvalidDt = new GregorianCalendar(0001, 01, 01);
	    Calendar calIdxDt;
	    SimpleDateFormat date_format2 = new SimpleDateFormat("yyyyMMdd");

	    HashMap<String,Integer> pathMap = new HashMap<String,Integer>();
	    
	    listParts = requestArgs.split(",");

	    for ( int idxList=0; idxList < listParts.length; idxList++ ) {

	    	argParts = (listParts[idxList]).split(":");
	      
	    	if ( argParts[0].equals("*") ) {
	    		terrCodes = allTerrCodes;
	    	} else {
	    		terrCodes = new ArrayList<String>();
	    		terrCodes.add(argParts[0]);
	    	}
	      
	    	for (String terrCd : terrCodes ) {

	    		if ( argParts.length >= 2 ) {
	    			//calFromDt = validDate(argParts[1]);
	    			String fromDate=argParts[1];
	    			if(!(fromDate.length()==10))
	    			{
	    				fromDate=fromDate+"-01";
	    				
	    			}
	    			calFromDt = validDate(fromDate);
	    		} else {
	    			calFromDt = calInvalidDt;
	    		}
			      
	    		if ( argParts.length == 2 ) {
	    		  //calToDt = validDate(argParts[1]);
	    			String toDate=argParts[1];
	    			if(!(toDate.length()==10))
	    			{
	    				toDate=toDate+"-"+getLastDayInMonth(toDate);
	    			}
	    			calToDt = validDate(toDate);
	    		} else {
	    			if ( argParts.length == 3 ) {
	    				//calToDt = validDate(argParts[2]);
	    				String toDate=argParts[2];
		    			if(!(toDate.length()==10))
		    			{
		    				toDate=toDate+"-"+getLastDayInMonth(toDate);
		    			}
	    				calToDt = validDate(toDate);
	    			} else {
	    				calToDt = calInvalidDt;
	    			}
	    		}

	    		if ( calFromDt.compareTo(calInvalidDt) == 0 || calToDt.compareTo(calInvalidDt) == 0 ) {
	    			System.err.println("Ignoring: Invalid from/to date for subdirectory parameter = " + listParts[idxList] );
	    		} else {
	    			if ( calFromDt.compareTo(calToDt) > 0 ) {
	    				System.out.println("Ignoring: Invalid from date after to date for subdirectory parameter = " + listParts[idxList] );
	    			} else {
	    				calIdxDt = calFromDt;
	    				
	    				while ( calIdxDt.compareTo(calToDt) <= 0 ) {

	    					for (int idx1=0; idx1 < subFileTypes.size(); idx1++ ) {
	    						addPath = daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsFinalSubDir() + Path.SEPARATOR + daasConfig.fileSubDir() + Path.SEPARATOR + subFileTypes.get(idx1) + Path.SEPARATOR + terrCd + Path.SEPARATOR + date_format2.format(calIdxDt.getTime());	
			    			
	    						if  ( pathMap.containsKey(addPath) ) {
	    							pathMap.put(addPath, (int)pathMap.get(addPath)+1);
	    						} else {
	    							pathMap.put(addPath, 1);
	    						}
	    					}
					
	    					calIdxDt.add(Calendar.DATE, 1);
	    				}
	    			}
	    		}
	    	}
	    }
	    
	    if ( pathMap.size() > 0 ) {

	    	int idx = 0;
	    	retPath = new Path[pathMap.size()];
	    	
	    	for ( Map.Entry<String, Integer> entry : pathMap.entrySet()) { 
	    		retPath[idx] = new Path(entry.getKey());
	    		idx++;
	    	}
	    }
	    
	    return(retPath);
	}
	
	public static String getLastDayInMonth(String date)
	{
		String year=date.split("-")[0];
		String month=date.split("-")[1];
		GregorianCalendar gc = new GregorianCalendar(Integer.parseInt(year), (Integer.parseInt(month))-1, 1);
		java.util.Date monthStartDate = new java.util.Date(gc.getTime().getTime());
	    Calendar calendar = Calendar.getInstance();
	    calendar.setTime(monthStartDate);
	    calendar.add(calendar.MONTH, 1);
	    calendar.add(calendar.DAY_OF_MONTH, -1);
	    String lastDay=String.format("%02d", calendar.getTime().getDate());
	    return lastDay;
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

	//AWS START
	public static FileSystem getFileSystem(DaaSConfig daasConfig
			                              ,Configuration hdfsConfig) throws Exception {
		
		FileSystem hdfsFileSystem = null;
		String prefix;
		
		prefix = daasConfig.hdfsRoot();
		
		if ( prefix.toUpperCase().startsWith("S3://") ) {
			int pos = (prefix + "/").indexOf("/", 5);
			hdfsFileSystem = FileSystem.get(new URI(prefix.substring(0,pos)), hdfsConfig);
		} else {
			hdfsFileSystem = FileSystem.get(hdfsConfig);
		}
		
		return(hdfsFileSystem);
	}
	

	public static FileSystem getFileSystem(String hdfsRoot
			                              ,Configuration hdfsConfig) throws Exception {
		
		FileSystem hdfsFileSystem = null;
		String prefix;
		
		prefix = hdfsRoot;
		
		if ( prefix.toUpperCase().startsWith("S3://") ) {
			int pos = (prefix + "/").indexOf("/", 5);
			hdfsFileSystem = FileSystem.get(new URI(prefix.substring(0,pos)), hdfsConfig);
		} else {
			hdfsFileSystem = FileSystem.get(hdfsConfig);
		}
		
		return(hdfsFileSystem);
	}
	
	public static void removeExistingFilesinS3(FileSystem fileSystem, String outPath, final String filename)
	{
		try {
			Path outputPath=new Path(outPath); 
			FileStatus[] fstatustmp = fileSystem.listStatus(outputPath,new PathFilter() {
				
				@Override
				public boolean accept(Path pathname) {
					if(pathname.getName().startsWith(filename))
						return true;
					return false;
				}
			});
			
			for(FileStatus fstat:fstatustmp){
//				fileName  = fstat.getPath().getName().replace("SALES", "SALES-");
				
				String fileName  = fstat.getPath().getName();
				if(fileSystem.exists(new Path(fileName)))
				{
					fileSystem.delete(new Path(fileName),true);
				}			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	//AWS END
	
	
}
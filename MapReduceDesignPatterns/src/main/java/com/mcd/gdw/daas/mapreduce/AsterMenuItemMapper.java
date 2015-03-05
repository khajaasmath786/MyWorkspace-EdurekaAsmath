package com.mcd.gdw.daas.mapreduce;
import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
//import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
//import org.apache.hadoop.mapreduce.Mapper.Context;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
public class AsterMenuItemMapper extends Mapper<LongWritable, Text, Text, Text> {

	String storeId = "";
	String productId = "";
	String takePrice = "";
	String eatInPrice = "";
	String longName = "";
	
	DocumentBuilder builder;
	DocumentBuilderFactory factory;
	Document doc;
	
	NodeList MenuNodeList;
	Element MenuNodeElm;
	Element ProductNodeElm;
	NodeList ProductNodeList;
	
	private String pipeDelimiter="|";
	private String commaDelimiter=",";
	
	//private NullWritable mapKeyPmenu = new NullWritable();
	private Text mapValuePmenu = new Text();
	private Text mapKeyPmenu = new Text();

	public void setup(Context context) throws IOException,
			InterruptedException {
		
		try {
			factory = DocumentBuilderFactory
					.newInstance();
			builder = factory.newDocumentBuilder();
			
		} catch (Exception ex) {
			System.err.println("Error in initializing AsterMenuItemMapper:");
			System.err.println(ex.toString());
			System.exit(8);
		}
	}

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String menuXML = value.toString().substring(
				value.toString().indexOf("<"));

		factory = DocumentBuilderFactory
				.newInstance();
		
		try {
			builder = factory.newDocumentBuilder();

			

			doc = builder.parse(new InputSource(new StringReader(menuXML)));

			doc.getDocumentElement().normalize();

			MenuNodeList = doc.getElementsByTagName("MenuItem");
			for (int i = 0; i < MenuNodeList.getLength(); i++) {
				MenuNodeElm = (Element) MenuNodeList.item(i);

				storeId = MenuNodeElm.getAttribute("storeId");
				System.out.println(storeId);
				ProductNodeList = MenuNodeElm ==null?null: MenuNodeElm.getElementsByTagName("ProductInfo");
				
				
				for (int product = 0; product < ProductNodeList.getLength(); product++) {
					ProductNodeElm = (Element) ProductNodeList.item(product);
					
				
					productId = ProductNodeElm.getAttribute("id");
					takePrice = ProductNodeElm.getAttribute("takeoutPrice");
					eatInPrice = ProductNodeElm.getAttribute("eatinPrice");
					longName = ProductNodeElm.getAttribute("longName");
					/*System.out.println(productId);
					System.out.println(takePrice);
					System.out.println(eatInPrice);
					System.out.println(longName);
					*/

					mapKeyPmenu.clear();
					mapKeyPmenu.set((new StringBuffer(storeId)
					.append(pipeDelimiter).append(productId).toString()));
					mapValuePmenu.clear();
					mapValuePmenu.set((new StringBuffer(storeId)
							.append(commaDelimiter).append(productId).append(commaDelimiter)
							.append(takePrice).append(commaDelimiter)
							.append(eatInPrice).append(commaDelimiter)
							.append(longName)).toString());
					context.write(mapKeyPmenu,mapValuePmenu);

				}

			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.err.println("Error in AsterMenuItemMapper:");
			e.printStackTrace(System.err);
			System.exit(8);
		}
	}
	
	public void cleanup(Context context) throws IOException,
	InterruptedException {

}



}

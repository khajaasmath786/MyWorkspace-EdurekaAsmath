package com.mcd.gdw.daas.mapreduce;

import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
//import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
//import org.apache.hadoop.mapreduce.Mapper.Context;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

public class AsterExtractSTLDMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private static final BigDecimal DECIMAL_ZERO = new BigDecimal("0.00");
	
	private static String[] parts = null;
	private Text mapKey = new Text();
	private Text mapValue = new Text();
	

	private DocumentBuilderFactory docFactory = null;
	private DocumentBuilder docBuilder = null;
	private InputSource xmlSource = null;
	private Document doc = null;

	private Element eleRoot;
	private NodeList nlNode;
	private Element eleNode;
	private NodeList nlEvent;
	private Element eleEvent;
	private String eventType;
	private Element eleTrx;
	private Element eleOrder;
	private NodeList nlTRX;
	private NodeList nlOrder;

	private String terrCd = "";
	private String lgcyLclRfrDefCd = "";
	private String storeID="";
	private String tldBusinessDate = "";
	private String businessDate = "";
	private String status = "";
	private String orderLocation = "";
	private String orderKind = "";
	private String orderSaleType="";
	private String orderTimestamp="";
	private String orderKey="";
	private String orderTotalAmount="";
	

	private SimpleDateFormat timeformat = new SimpleDateFormat(
			"yyyyMMddHHmmss00");
	//2015022609025110
	SimpleDateFormat format = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss.00");
	private BigDecimal tranTotalAmount = DECIMAL_ZERO;
	private BigDecimal totalAmount = DECIMAL_ZERO;
	private int tranCount = 0;

	private static FileSplit fileSplit = null;
	private static String fileName = "";

	private String owshFltr = "*";
	private boolean keepValue = false;
	
	private int itemQty = 0;
	private int tenderKind = 0;
	private int couponQuantity = 0;
	private BigDecimal couponAmount = DECIMAL_ZERO;

	private String itemQtyPromo = "";
	private String itemLevel = "";
	private String itemTotalPrice = "";
	private String unitPrice = "";
	private String menuItem = "";
	private String itemCode = "";
	private String itemType="";
	private String commaDelimiter=",";
	private String pipeDelimiter="|";

	@Override
	public void setup(Context context) {

        fileSplit = (FileSplit) context.getInputSplit();
        fileName = fileSplit.getPath().getName();

			try {
			docFactory = DocumentBuilderFactory.newInstance();
			docBuilder = docFactory.newDocumentBuilder();
			
		} catch (Exception ex) {
			System.err.println("Error in initializing AsterExtractMapper:");
			System.err.println(ex.toString());
			System.exit(8);
		}
		
	}

	@Override
	public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {

		try {
			
			getSalesSummary(value.toString(),context);
			
	    } catch (Exception ex) {
	    	System.err.println("Error in map NpSalesSummaryMapper:");
	    	ex.printStackTrace(System.err);
	    }
	}

	public String getFormatedBusinessDate(String date) throws ParseException
	{
		 SimpleDateFormat inputDateFormat = 
                 new SimpleDateFormat("yyyyMMdd");
         
         SimpleDateFormat expectedDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                     
         Date parsed = inputDateFormat.parse(date);
         date= expectedDateFormat.format(parsed);
		
		return date;
	}
	
	public String getFormatedOrderDate(String date) throws ParseException
	{
		SimpleDateFormat inputDateFormat = 
                new SimpleDateFormat("yyyyMMddHHmmssSSS");
        
        SimpleDateFormat expectedDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                    
        Date parsed = inputDateFormat.parse(date);
        date= expectedDateFormat.format(parsed);
		
		return date;
	}
	
	private void getSalesSummary(String xmlText
			                    ,Context context) {

		tranTotalAmount = DECIMAL_ZERO;
		tranCount = 0;
		StringReader strReader = null;
		try {
			strReader  = new StringReader(xmlText);
			xmlSource = new InputSource(strReader);
			doc = docBuilder.parse(xmlSource);

			eleRoot = (Element) doc.getFirstChild();

			if ( eleRoot.getNodeName().equals("TLD") ) {
				//lgcyLclRfrDefCd = eleRoot.getAttribute("gdwLgcyLclRfrDefCd");
				storeID = eleRoot.getAttribute("storeId");
				tldBusinessDate = eleRoot.getAttribute("businessDate");
				businessDate=getFormatedBusinessDate(tldBusinessDate);
			

				nlNode = eleRoot.getChildNodes();
				if (nlNode != null && nlNode.getLength() > 0) {
					for (int idxNode = 0; idxNode < nlNode.getLength(); idxNode++) {
						if (nlNode.item(idxNode).getNodeType() == Node.ELEMENT_NODE) {
							eleNode = (Element) nlNode.item(idxNode);
							if (eleNode.getNodeName().equals("Node")) {

								nlEvent = eleNode.getChildNodes();
								if (nlEvent != null && nlEvent.getLength() > 0) {
									for (int idxEvent = 0; idxEvent < nlEvent
											.getLength(); idxEvent++) {
										if (nlEvent.item(idxEvent)
												.getNodeType() == Node.ELEMENT_NODE) {
											eleEvent = (Element) nlEvent
													.item(idxEvent);

											if (eleEvent.getNodeName().equals(
													"Event")) {
												eventType = eleEvent
														.getAttribute("Type");

												if (eventType
														.equals("TRX_Sale")) {
													eleTrx = null;
													nlTRX = eleEvent
															.getChildNodes();
													int idxTRX = 0;

													while (eleTrx == null
															&& idxTRX < nlTRX
																	.getLength()) {
														if (nlEvent
																.item(idxTRX)
																.getNodeType() == Node.ELEMENT_NODE) {
															eleTrx = (Element) nlTRX
																	.item(idxTRX);
															status = eleTrx
																	.getAttribute("status")
																	+ "";
															orderLocation = eleTrx
																	.getAttribute("POD");
														}
														idxTRX++;
													}

													if ((eventType
															.equals("TRX_Sale") && status
															.equals("Paid"))) {
														eleOrder = null;
														nlOrder = eleTrx
																.getChildNodes();
														int idxOrder = 0;

														while (eleOrder == null
																&& idxOrder < nlOrder
																		.getLength()) {
															if (nlOrder
																	.item(idxOrder)
																	.getNodeType() == Node.ELEMENT_NODE) {
																eleOrder = (Element) nlOrder
																		.item(idxOrder);
															}
															idxOrder++;
														}

														totalAmount = new BigDecimal(
																eleOrder.getAttribute("totalAmount"));
														totalAmount = totalAmount
																.subtract(new BigDecimal(
																		eleOrder.getAttribute("nonProductAmount")));
														
														orderKey = eleOrder
																.getAttribute("key");
													
														orderSaleType = eleOrder
																.getAttribute("saleType");
														
														//orderLocation
														orderKind = eleOrder
																.getAttribute("kind");
														
														System.out
																.println(eleOrder
																.getAttribute("Timestamp")+"");
														/*orderTimestamp = timeformat.parse(eleOrder
																.getAttribute("Timestamp"))+"";*/
														orderTimestamp = eleOrder
																.getAttribute("Timestamp")+"";
														orderTimestamp=getFormatedOrderDate(orderTimestamp);
														orderTotalAmount=eleOrder
														.getAttribute("totalAmount");
														
														
														//OrderKey, OderSaleType, OrderLocation, OrderTimeStamp, OrderTotalAmount

														/*if (eventType
																.equals("TRX_Sale")) {

															if (orderKind.contains("Manager")
																	|| orderKind.contains("Crew")) {
																if (totalAmount
																		.compareTo(DECIMAL_ZERO) != 0) {
																	tranCount++;
																}
															} else {
																tranCount++;
															}

															tranTotalAmount = tranTotalAmount
																	.add(totalAmount);
														}*/
														if (eventType
																.equals("TRX_Sale") && orderKind.contains("Sale")) {

														
														// Logic for Item Code.
														NodeList ItemList = (eleOrder == null ? null
																: eleOrder
																		.getElementsByTagName("Item"));

														if (ItemList != null) {
															for (int s3 = 0; s3 < ItemList
																	.getLength(); s3++) {

																Element ItemElm = (Element) ItemList
																		.item(s3);
																
																if (ItemList
																		.getLength() > 1) {
																	Element subItemElm = (Element) ItemList
																			.item(1);

																	if (ItemElm
																			.hasAttributes()) {

																		NamedNodeMap nodeMapItem = ItemElm
																				.getAttributes();

																		for (int k = 0; k < nodeMapItem
																				.getLength(); k++) {

																			Node nodeItem = nodeMapItem
																					.item(k);
																			String NodeNameItem = nodeItem
																					.getNodeName()
																					.toString();

																			if (eventType
																					.equals("TRX_Sale")) {

																				if (NodeNameItem
																						.equals("code")) {
																					
																					itemCode = nodeItem
																							.getNodeValue();
																					System.out
																							.println("Itemcode"+itemCode);

																				}
																				if (NodeNameItem
																						.equals("qty")) {
																					itemQty = Integer
																							.parseInt(nodeItem
																									.getNodeValue());
																				}
																				if (NodeNameItem
																						.equals("unitPrice")) {
																					unitPrice = nodeItem
																							.getNodeValue();
																				}
																				if (NodeNameItem
																						.equals("totalPrice")) {
																					itemTotalPrice = nodeItem
																							.getNodeValue();
																				}
																				if (NodeNameItem
																						.equals("level")) {
																					itemLevel = nodeItem
																							.getNodeValue();
																				}
																				if (NodeNameItem
																						.equals("qtyPromo")) {
																					itemQtyPromo = nodeItem
																							.getNodeValue();
																				}
																				if (NodeNameItem
																						.equals("type")) {
																					itemType = nodeItem
																							.getNodeValue();
																					// Added for Vicki Requirement
																					/*if(!itemType.equalsIgnoreCase("NON_FOOD_PRODUCT") && itemQty!=0)
																					{*/

																					mapKey.clear();
																					mapKey.set((new StringBuffer(storeID)
																					.append(pipeDelimiter).append(itemCode).toString()));
																					mapValue.clear();
																					mapValue.set(tldBusinessDate + commaDelimiter + businessDate+commaDelimiter +storeID + commaDelimiter+
																							orderKey + commaDelimiter + orderSaleType + commaDelimiter + orderLocation+ commaDelimiter +  orderKind + commaDelimiter + orderTimestamp + commaDelimiter + orderTotalAmount+commaDelimiter+
																							itemCode+ commaDelimiter+itemType+ commaDelimiter+itemQty+ commaDelimiter+itemLevel+ commaDelimiter+itemTotalPrice);
																					//Output Fields in excel:  Business Date, StoreId, OrderKey, OderSaleType, OrderLocation, OrderTimeStamp, OrderTotalAmount, ItemCode, ItemType, ItemQuantity, ItemLevel, ItemTotalPrice.
																					context.write(mapKey, mapValue);
																						
																					//}
																				}

																			}
																		
																		}

																	}

																}

															}
														}
														
														
													}//
													}
												}
											}
										}
									}
								}
							}
						}

						
					}

				} 
			}
		}catch (Exception ex) {
			System.err.println("Error in AsterExtractMapper:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}finally{
			
				doc = null;
				xmlSource = null;
				
				if(strReader != null){
					strReader.close();
					strReader = null;
					
				}
		}
	}
}

	

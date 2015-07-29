/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.stsci.registry.solr;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.solr.handler.dataimport.Context;
import org.apache.solr.handler.dataimport.DataSource;
import org.apache.solr.handler.dataimport.EntityProcessorBase;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 *
 * @author sweissman
 */
public class OAIPMHEntityProcessor extends EntityProcessorBase{

    private DataSource<HttpClient> dataSource;
    private NodeList nodes;
    private int currentNode;
    private static final Logger logger = LogManager.getLogger(OAIPMHEntityProcessor.class.getName());
    private String resumptionToken = null;
    private CloseableHttpClient httpClient;
    
    @Override
    public void init(Context context) {
        super.init(context);
        dataSource = context.getDataSource();
        rowIterator = null;
        httpClient = HttpClients.createDefault();
        initNodes();

    }

    private void initNodes(){
        InputStream is = null;

        CloseableHttpResponse response = null;
        try {
            currentNode = 0;
            nodes = null;
            String url = context.getEntityAttribute(URL);
            
            logger.info("Resumption token " + resumptionToken);
            URI uri = null;
            // If the resumption token 
            if(resumptionToken != null){
                uri = new URIBuilder(url)
                        .setParameter("resumptionToken", resumptionToken)
                        .setParameter("verb", "ListRecords")
                        .build();
            }else{
                URIBuilder uriBuilder = new URIBuilder(url).setParameter("verb", "ListRecords");
                String metadataPrefix = context.getEntityAttribute(PREFIX);
                if(metadataPrefix == null){
                    logger.error("Metadata prefix must be set for OAI-PMH data import.");
                    return;
                }
                uriBuilder.setParameter("metadataPrefix", metadataPrefix);
                String from = context.getEntityAttribute(FROM);
                if(from != null){
                    try {
                        //Calendar fromCalendar = Calendar.getInstance();
                        //fromCalendar.set(2014, Calendar.JANUARY, 1);
                        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                        df.parse(from);
                        uriBuilder.setParameter("from", from);
                    } catch (ParseException ex) {
                        logger.error("Error parsing from date " + from, ex);
                    }
                }
                uri = uriBuilder.build();
            }
            logger.info("Request url: " + uri.toString());
            //is = client.execute(parameters);
            HttpGet httpget = new HttpGet(uri);
            response = httpClient.execute(httpget);
            if(HttpStatus.SC_OK != response.getStatusLine().getStatusCode()){
                return;
            }

            HttpEntity entity = response.getEntity();
            is = entity.getContent();
            
            XPath xpath = XPathFactory.newInstance().newXPath();
            InputSource inputSource = new InputSource(is);
            
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();
            Document document = db.parse(inputSource);
            
            //String expression = "/OAI-PMH/ListRecords/record";
            String expression = context.getEntityAttribute(FOR_EACH);
            logger.info("For each expression " + expression);
            nodes = (NodeList) xpath.evaluate(expression, document, XPathConstants.NODESET);
            logger.info("Len = " + nodes.getLength());


            NodeList rtNodes = (NodeList) xpath.evaluate(RT_EXPRESSION, document, XPathConstants.NODESET);
            if(rtNodes.getLength() > 0){
                Node node = rtNodes.item(0);
                resumptionToken = node.getTextContent();
            }else{
                resumptionToken = null;
            }
        } catch (ParserConfigurationException ex) {
            logger.error("",ex);
        } catch (SAXException ex) {
            logger.error("",ex);
        } catch (IOException ex) {
            logger.error("",ex);
        } catch (XPathExpressionException ex) {
            logger.error("",ex);
        } catch (URISyntaxException ex) {
            logger.error("",ex);
        }finally{
            if(response != null){
                try {
                    response.close();
                } catch (IOException ex) {
                }
            }
        }
    }
    
    @Override
    public Map<String, Object> nextRow() {
        XPath xpath = XPathFactory.newInstance().newXPath();
        Map<String,Object> result = new HashMap<>();


        if(nodes == null || currentNode >= nodes.getLength()){
            if(resumptionToken != null){
                initNodes();
            }else{
                return null;
            }
        }

        Node node = nodes.item(currentNode);
        for (Map<String, String> field : context.getAllEntityFields()) {
            try {
                List<String> valueList = new ArrayList<>();
                if (field.get(XPATH) == null)
                    continue;
                String expression = field.get(XPATH);
                    
                //String value = xpath.evaluate(expression,node);
                NodeList nList = (NodeList) xpath.evaluate(expression, node, XPathConstants.NODESET);
                for(int i=0;i<nList.getLength();i++){
                    Node n = nList.item(i);
                    valueList.add(n.getTextContent());
                    //logger.info("Found value for column " + field.get("column") + n.getTextContent());
                    
                }
                //valueList.add(value);
                result.put(field.get("column"), valueList);
//                logger.info("Extracting field with expression " + expression + " field " + field.get("column") + " " + valueList);
            } catch (XPathExpressionException ex) {
            }
        }
        currentNode++;
        return result;
    }
  
  //Document fields

  public static final String FOR_EACH = "forEach";
  
  public static final String FROM = "from";

  public static final String URL = "url";
  
  public static final String PREFIX = "prefix";
  
  public static final String XPATH = "xpath";

  public static final String RT_EXPRESSION = "/OAI-PMH/ListRecords/resumptionToken";

  
}

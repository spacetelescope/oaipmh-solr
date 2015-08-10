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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.solr.handler.dataimport.Context;
import org.apache.solr.handler.dataimport.EntityProcessorBase;
import org.apache.solr.handler.dataimport.SolrWriter;
import org.apache.solr.handler.dataimport.config.ConfigNameConstants;
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

    // Variables used for tracking nodes retrieved from OAIPMH
    private NodeList nodes;
    private List<Node> deletedNodes;
    private int currentNode;
    private List<Map<String,Object>> deltaNodes = null;
    private int currentDeltaNode;
    private int waitSeconds = -1;

    private static final Logger logger = LogManager.getLogger(OAIPMHEntityProcessor.class.getName());
    private String resumptionToken = null;
    private CloseableHttpClient httpClient;

    // The current process of Solr (DELTA_QUERY, etc.)
    private String process;
    private Date lastImport;
    
    @Override
    public void init(Context context) {
        super.init(context);
        rowIterator = null;
        httpClient = HttpClients.createDefault();
        process = context.currentProcess(); //DELTA_DUMP or ...
        lastImport = null;
        
        String waitStr = context.getEntityAttribute(WAIT_SECS);
        if(waitStr != null){
            waitSeconds = Integer.valueOf(waitStr);
        }
        
        if(process.equals(Context.FIND_DELTA)){
            deletedNodes = new ArrayList<>();
            Map<String,Object> stats = context.getStats();
            long docCount = (long) stats.get("docCount");
            // If we have no documents in the index, don't do a delta import
            if(docCount > 0){
                Map<String,Object> importerMap = (Map<String,Object>) context.resolve(ConfigNameConstants.IMPORTER_NS_SHORT);
                String lastIndexStr = (String) importerMap.get(SolrWriter.LAST_INDEX_KEY);
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                try {
                    lastImport = dateFormat.parse(lastIndexStr);
                } catch (ParseException ex) {
                    logger.error(ex);
                }
            }

        }
        
        if(process.equals(Context.DELTA_DUMP)){
            deltaNodes = new ArrayList<>();
            // This is where the modified node gets placed by DocBuilder. We just pull it back out.
            Map<String,Object> dtest = (Map<String,Object>) context.resolve(ConfigNameConstants.IMPORTER_NS_SHORT + ".delta");
            if(dtest != null && !dtest.isEmpty()){
                deltaNodes.add(dtest);
                currentDeltaNode = 0;
            }
            // Don't init nodes on a delta dump
        }else{
            initNodes();
        }
    }

    private void initNodes(){
        InputStream is = null;

        CloseableHttpResponse response = null;
        try {
            currentNode = 0;
            nodes = null;
            String url = context.getEntityAttribute(URL);
                        
            URI uri = null;
            // If the resumption token exists, use it to retrieve rest of data set
            if(resumptionToken != null && !resumptionToken.equals("")){
                logger.info("Resumption token >>>" + resumptionToken + "<<<");
                uri = new URIBuilder(url)
                        .setParameter("resumptionToken", resumptionToken)
                        .setParameter("verb", "ListRecords")
                        .build();
            }else{
                String from = context.getEntityAttribute(FROM);                
                if(process.equals(Context.FIND_DELTA)){
                    // Check that we have a last Import date
                    if(lastImport == null)
                        return;
                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
                    from = dateFormat.format(lastImport);
                }
                URIBuilder uriBuilder = new URIBuilder(url).setParameter("verb", "ListRecords");
                String metadataPrefix = context.getEntityAttribute(PREFIX);
                if(metadataPrefix == null){
                    logger.error("Metadata prefix must be set for OAI-PMH data import.");
                    return;
                }
                uriBuilder.setParameter("metadataPrefix", metadataPrefix);

                if(from != null){
                    //DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                    uriBuilder.setParameter("from", from);
                }

                uri = uriBuilder.build();
            }


            HttpGet httpget = new HttpGet(uri);
            while(true){
                if(waitSeconds > 0){
                    Thread.sleep(waitSeconds*1000);
                }
                logger.info("Request url: " + uri.toString() + " with wait time " + waitSeconds + " seconds.");
                response = httpClient.execute(httpget);
                int status = response.getStatusLine().getStatusCode();
                if(status == HttpStatus.SC_OK){
                    break;
                }else if(status == HttpStatus.SC_SERVICE_UNAVAILABLE && response.containsHeader("Retry-After")){
                    // Check to see if we need to wait to repeat the request
                    waitSeconds = Integer.getInteger(response.getFirstHeader("Retry-After").getValue());
                    logger.info("Server requested wait time of " + waitSeconds + " seconds.");
                    // Add one just in case
                    waitSeconds++;
                }else{
                    return;
                }
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
            logger.error(ex);
        } catch (SAXException ex) {
            logger.error(ex);
        } catch (IOException ex) {
            logger.error(ex);
        } catch (XPathExpressionException ex) {
            logger.error(ex);
        } catch (URISyntaxException ex) {
            logger.error(ex);
        } catch (InterruptedException ex) {
            java.util.logging.Logger.getLogger(OAIPMHEntityProcessor.class.getName()).log(Level.SEVERE, null, ex);
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
        //logger.info("In nextRow");

        if(process.equals(Context.DELTA_DUMP)){
            if(deltaNodes != null && currentDeltaNode < deltaNodes.size()){
                Map<String,Object> returnNode = deltaNodes.get(currentDeltaNode);
                currentDeltaNode++;
                return returnNode;
            }else{
                return null;
            }
        }
        
        XPath xpath = XPathFactory.newInstance().newXPath();
        Map<String,Object> result = new HashMap<>();

        
        if(nodes == null || currentNode >= nodes.getLength()){
            if(resumptionToken != null && !resumptionToken.equals("")){
                logger.info("Calling initNodes from nextRow");
                initNodes();
            }else{
                return null;
            }
        }
        
        // Add catalog name to record
        String catalog = context.getEntityAttribute(CATALOG);
        String catalogField = context.getEntityAttribute(CATALOG_FIELD);
        result.put(catalogField, catalog);
        
        Node node = nodes.item(currentNode);
        for (Map<String, String> field : context.getAllEntityFields()) {
            try {
                List<String> valueList = new ArrayList<>();
                if (field.get(XPATH) == null)
                    continue;
                String expression = field.get(XPATH);
                String dateTimeFormat = field.get("dateTimeFormat");
                    
                //String value = xpath.evaluate(expression,node);
                NodeList nList = (NodeList) xpath.evaluate(expression, node, XPathConstants.NODESET);
                for(int i=0;i<nList.getLength();i++){
                    Node n = nList.item(i);
                    String nTxt = n.getTextContent();
                    if(dateTimeFormat != null){
                        try {
                            SimpleDateFormat recordDateFormat = new SimpleDateFormat(dateTimeFormat);
                            Date recordDate = recordDateFormat.parse(nTxt);
                            SimpleDateFormat solrDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
                            String solrDateTxt = solrDateFormat.format(recordDate);
                            valueList.add(solrDateTxt);
                        } catch (ParseException ex) {
                            logger.error(ex);
                        }
                    }else{
                        valueList.add(nTxt);
                    }
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
  
    @Override
    public Map<String, Object> nextModifiedRowKey() {
        logger.info("In nextModifiedRow currentNode = " + currentNode); 
        XPath xpath = XPathFactory.newInstance().newXPath();
        Map<String,Object> result = new HashMap<>();

        if(nodes == null || currentNode >= nodes.getLength()){
            if(resumptionToken != null && !resumptionToken.equals("")){
                logger.info("Calling initNodes from nextModifiedRowKey");
                initNodes();
            }else{
                currentNode = 0;
                return null;
            }
        }
        Node node = nodes.item(currentNode);
        try {            
            Node nodeStat;

            nodeStat = (Node) xpath.evaluate(STATUS_EXPRESSION, node, XPathConstants.NODE);
            if(nodeStat != null){
                String status = nodeStat.getTextContent();
                if(status.equals("deleted")){
                    deletedNodes.add(node);
                    currentNode++;
                    return null;
                }
            }
        } catch (XPathExpressionException ex) {
            logger.error(ex);
        }


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
                logger.error(ex);
            }
        }
        currentNode++;
        return result;
    }

    @Override
    public Map<String, Object> nextDeletedRowKey() {
        Map<String,Object> result = new HashMap<>();

        logger.info("In nextDeletedRow, currentNode = " + currentNode);
        XPath xpath = XPathFactory.newInstance().newXPath();

            
        if(deletedNodes == null || deletedNodes.isEmpty() || currentNode > deletedNodes.size()){
            return null;
        }
        Node node = deletedNodes.get(currentNode);
            
        try {            
            Node nodeStat;

            nodeStat = (Node) xpath.evaluate(STATUS_EXPRESSION, node, XPathConstants.NODE);
            if(nodeStat == null){
                return null;
            }
            String status = nodeStat.getTextContent();
            if(!status.equals("deleted")){
                return null;
            }
        } catch (XPathExpressionException ex) {
            java.util.logging.Logger.getLogger(OAIPMHEntityProcessor.class.getName()).log(Level.SEVERE, null, ex);
        }

        String idCol = context.getEntityAttribute(IDCOL);
        
        for (Map<String, String> field : context.getAllEntityFields()) {
            try {
                List<String> valueList = new ArrayList<>();
                if (field.get(XPATH) == null)
                    continue;
                String column = field.get("column");
                if(!column.equals(idCol))
                    continue;
                String expression = field.get(XPATH);
                Node nodeId = (Node) xpath.evaluate(expression, node, XPathConstants.NODE);
                String id = nodeId.getTextContent();
                logger.info("Deleted node: " + id);
                valueList.add(id);
                result.put(column, valueList);
                    
            } catch (XPathExpressionException ex) {
                logger.error(ex);
            }
        }
        currentNode++;
        if(result.isEmpty()){
            return null;
        }else{
            return result;
        }
    }

    
  //Document fields

  public static final String CATALOG = "catalog";
  public static final String CATALOG_FIELD = "catalogField";
    
  public static final String FOR_EACH = "forEach";
  
  public static final String FROM = "from";

  public static final String URL = "url";
  
  public static final String PREFIX = "prefix";
  
  public static final String XPATH = "xpath";
  
  public static final String WAIT_SECS = "wait";
  
  public static final String IDCOL = "idcol";

  public static final String RT_EXPRESSION = "/OAI-PMH/ListRecords/resumptionToken";

  public static final String STATUS_EXPRESSION = "header[@status]";
  
}

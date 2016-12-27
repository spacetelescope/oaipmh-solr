/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.stsci.registry.solr;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
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
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
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
import org.apache.solr.handler.dataimport.DataImportHandlerException;
import org.apache.solr.handler.dataimport.EntityProcessorBase;
import org.apache.solr.handler.dataimport.SolrWriter;
import org.apache.solr.handler.dataimport.config.ConfigNameConstants;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
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
    private List<Node> modifiedNodes;
    private int currentNode;
    private int currentDeletedNode;
    private int currentModifiedNode;

    private int waitSeconds = -1;

    private static final Logger logger = LogManager.getLogger(OAIPMHEntityProcessor.class.getName());
    private String resumptionToken = null;
    private CloseableHttpClient httpClient;

    // The current process of Solr (DELTA_QUERY, etc.)
    private String process;
    private Date lastImport;
    private boolean returnedRow = false;
    private XPath xpath = null;
    
    @Override
    public void init(Context context) {
        super.init(context);
        logger.debug("In init");
        rowIterator = null;
        httpClient = HttpClients.createDefault();
        process = context.currentProcess(); //DELTA_DUMP or ...

        if(xpath == null){
            xpath = XPathFactory.newInstance().newXPath();
        }
        
        String waitStr = context.getEntityAttribute(WAIT_SECS);
        if(waitStr != null){
            waitSeconds = Integer.valueOf(waitStr);
        }
        
        
        if(process.equals(Context.FIND_DELTA)){
            logger.info("Find delta");
            deletedNodes = new ArrayList<>();
            modifiedNodes = new ArrayList<>();
            Map<String,Object> stats = context.getStats();
            //long docCount = (long) stats.get("docCount");
            //for(String key : stats.keySet()){
            //    logger.info(key + " " + stats.get(key).toString());
            //}
            // If we have no documents in the index, don't do a delta import
            //logger.info("docCount = " + docCount);
            // These stats aren't working (always 0) so don't rely on them
            //if(docCount > 0){        
            
                Map<String,Object> importerMap = (Map<String,Object>) context.resolve(ConfigNameConstants.IMPORTER_NS_SHORT);
                String lastIndexStr = (String) importerMap.get(SolrWriter.LAST_INDEX_KEY);
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                try {
                    lastImport = dateFormat.parse(lastIndexStr);
                    logger.info("lastImport: " + lastIndexStr);
                } catch (ParseException ex) {
                    logger.error(ex);
                }
            //}
                while(true){
                    boolean hasResumption = getNextNodes();
                    logger.debug("Delta found " + nodes.getLength() + " new nodes");                    

                    while(currentNode < nodes.getLength()){
                        logger.debug("Current node: " + currentNode);
                        Node node = nodes.item(currentNode);
                        try {            
                            Node nodeStat;
                            
                            nodeStat = (Node) xpath.evaluate(STATUS_EXPRESSION, node, XPathConstants.NODE);
                            Node nodeId = (Node) xpath.evaluate(ID_EXPRESSION, node, XPathConstants.NODE);
                            if(nodeStat != null){
                                String status = nodeStat.getTextContent();
                                if(status.equals("deleted")){
                                    deletedNodes.add(node);
                                    logger.info("Found deleted node: " + nodeId.getTextContent());
                                }else{
                                    modifiedNodes.add(node);
                                }
                                
                            }else{
                                modifiedNodes.add(node);
                            }
                        } catch (XPathExpressionException ex) {
                            logger.error("Error getting node status",ex);
                        }
                        currentNode++;
                    }
                    if(!hasResumption) break;
                }
                logger.info("Found " + deletedNodes.size() + " deleted nodes.");
                logger.info("Found " + modifiedNodes.size() + " new or modified nodes.");
                currentDeletedNode = 0;
                currentModifiedNode = 0;


        }

        if(process.equals(Context.FULL_DUMP)){
            logger.debug("Full import");
            getNextNodes();
        }
        
        if(process.equals(Context.DELTA_DUMP)){
            logger.debug("Delta dump");
            returnedRow = false;
        }

    }

    // Gets the next batch of nodes from the OAI-PMH service
    private boolean getNextNodes(){
        InputStream is = null;

        CloseableHttpResponse response = null;
        try {
            currentNode = 0;
            nodes = null;
            String url = context.getEntityAttribute(URL);
              
            
            // Build URL for making the OAI-PMH request
            URI uri = null;
            // If the resumption token exists, use it to retrieve rest of data set
            // A resumption request only needs the OAI PMH verb and the resumption token
            if(resumptionToken != null && !resumptionToken.equals("")){
                logger.info("Resumption token >>>" + resumptionToken + "<<<");
                uri = new URIBuilder(url)
                        .setParameter("resumptionToken", resumptionToken)
                        .setParameter("verb", "ListRecords")
                        .build();
            }else{
                // If we are not resuming a previous request build a full request
                String from = context.getEntityAttribute(FROM);                

                // For a delta request, only ask for nodes from the last import date
                if(process.equals(Context.FIND_DELTA)){
                    // Check that we have a last Import date. What should the default be.
                    if(lastImport == null){
                        throw new DataImportHandlerException(500,"No last import date found. Does conf/dataimport.properties exist?");
                    }
                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
                    from = dateFormat.format(lastImport);
                }
                URIBuilder uriBuilder = new URIBuilder(url).setParameter("verb", "ListRecords");
                String metadataPrefix = context.getEntityAttribute(PREFIX);
                if(metadataPrefix == null){
                    logger.error("Metadata prefix must be set for OAI-PMH data import.");
                    return false;
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
                // The OAI-PMH protocol specifies that providers may enforce a wait period between resumption
                // requests. If one is specified, we wait here.
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
                    return false;
                }
            }

            // Parse the OAI PMH response
            HttpEntity entity = response.getEntity();
            is = entity.getContent();            
            InputSource inputSource = new InputSource(is);            
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();
            Document document = db.parse(inputSource);
            
            String expression = context.getEntityAttribute(FOR_EACH);
            nodes = (NodeList) xpath.evaluate(expression, document, XPathConstants.NODESET);
            logger.info("Len = " + nodes.getLength());

            // Get resumption token if there is one
            NodeList rtNodes = (NodeList) xpath.evaluate(RT_EXPRESSION, document, XPathConstants.NODESET);
            if(rtNodes.getLength() > 0){
                Node node = rtNodes.item(0);
                resumptionToken = node.getTextContent();
                logger.info("Resumption token: " + resumptionToken);
                if(resumptionToken == null || resumptionToken.equals("")){
                    logger.debug("No resumption token");
                    return false;
                }
                return true;
            }else{
                resumptionToken = null;
                logger.debug("No resumption token");
                return false;
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
        return false;
    }

    // Get the fields out of each node for ingest into Solr
    private void processRecord(Map<String,Object> result, Node node){
        
        // Add catalog name to record. If indexing from multiple OAI-PMH catalogs, the catalog field lets you
        // distinguish between sources for a record. This is configured in the entity attributes.
        String catalog = context.getEntityAttribute(CATALOG);
        String catalogField = context.getEntityAttribute(CATALOG_FIELD);
        if(catalogField != null && catalog != null){
            result.put(catalogField, catalog);
        }
        
        // The fields are defined in data-config.sql. Each field has a name and an xpath
        // expression that gives the location of that field in the OAI-PMH XML
        // The exact fields that are present will depend on your metadata schema
        IndexSchema schema = context.getSolrCore().getLatestSchema();
        for (Map<String, String> field : context.getAllEntityFields()) {
            try {
                SchemaField sf = schema.getField(field.get(NAME_FIELD));

                List<String> valueList = new ArrayList<>();
                if (field.get(XPATH_FIELD) == null)
                    continue;
                String expression = field.get(XPATH_FIELD);
                String dateTimeFormat = field.get(DATETIMEFORMAT_FIELD);
                    
                NodeList nList = (NodeList) xpath.evaluate(expression, node, XPathConstants.NODESET);
                for(int i=0;i<nList.getLength();i++){
                    Node n = nList.item(i);
                    String nTxt = n.getTextContent();
                    if(dateTimeFormat != null){
                        try {
                            // Convert input date into a date that solr understands
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
                    logger.debug("Found value for field " + field.get(NAME_FIELD) + n.getTextContent());
                    
                }
                // If the field is not multivalued put the first item from the valueList as the value of the field
                if(sf != null && !sf.multiValued() && valueList.size() > 0){
                    result.put(field.get(NAME_FIELD), valueList.get(0));
                }else{
                    result.put(field.get(NAME_FIELD), valueList);
                }

            } catch (XPathExpressionException ex) {
            }
        }
        
    }
    
    @Override
    public Map<String, Object> nextRow() {
        
        // If we are in delta dump, get the row information from the context. 
        if(process.equals(Context.DELTA_DUMP)){
            if(!returnedRow){
                Map<String,Object> deltaMap = (Map<String,Object>) context.resolve("dih.delta");
                returnedRow = true;
                return deltaMap;
            }else{
                return null;
            }

        }

        if(process.equals(Context.FULL_DUMP)){
            Map<String,Object> result = new HashMap<>();

            // If we have no more nodes to process, check to see if there are more from the OAI-PMH service
            if(nodes == null || currentNode >= nodes.getLength()){
                if(resumptionToken != null && !resumptionToken.equals("")){
                    getNextNodes();
                }else{
                    return null;
                }
            }
            Node node = nodes.item(currentNode);
            String status = null;
            String nid = null;
            try {            
                Node nodeStat;
                            
                nodeStat = (Node) xpath.evaluate(STATUS_EXPRESSION, node, XPathConstants.NODE);
                if(nodeStat != null){
                    status = nodeStat.getTextContent();
                }
                try{
                Node nodeId = (Node) xpath.evaluate(ID_EXPRESSION, node, XPathConstants.NODE);
                nid = nodeId.getTextContent();
                }catch(Exception ex){
                    logger.error("Exception getting nodeId for node " + nodeToString(node), ex);
                }

            } catch (XPathExpressionException ex) {
                logger.error("Error getting node status",ex);
            }
            // Don't process any nodes that are marked deleted during a full import
            if(status != null && status.equals("deleted")){
                logger.info("Found deleted node: " + nid);
            }else{
                logger.info("Processing record for node: " + nid);
                processRecord(result, node);
            }

            
            currentNode++;
            return result;
        }
        
        return null;
    }
  
    @Override
    public Map<String, Object> nextModifiedRowKey() {
        Map<String,Object> result = new HashMap<>();

        if(modifiedNodes == null || modifiedNodes.isEmpty() || currentModifiedNode >= modifiedNodes.size()){
            currentModifiedNode = 0;
            return null;            
        }
        Node node = modifiedNodes.get(currentModifiedNode);
        processRecord(result,node);

        currentModifiedNode++;
        return result;
    }

    @Override
    public Map<String, Object> nextDeletedRowKey() {
        Map<String,Object> result = new HashMap<>();

        if(deletedNodes == null || deletedNodes.isEmpty() || currentDeletedNode >= deletedNodes.size()){
            return null;
        }
        Node node = deletedNodes.get(currentDeletedNode);
        Node nodeId;
        try {
            nodeId = (Node) xpath.evaluate(ID_EXPRESSION, node, XPathConstants.NODE);
            logger.info("Deleting node: " + nodeId.getTextContent());
        } catch (XPathExpressionException ex) {
            logger.error(ex);
        }
        processRecord(result,node);
        
        currentDeletedNode++;
        if(result.isEmpty()){
            return null;
        }else{
            return result;
        }
    }
    
    private String nodeToString(Node node) {
        try {
            StringWriter writer = new StringWriter();
            Transformer transformer = TransformerFactory.newInstance().newTransformer();
            transformer.transform(new DOMSource(node), new StreamResult(writer));
            String xml = writer.toString();
            return xml;
        } catch (TransformerException ex) {
            return "Problem converting to string";
        }

    }

    
    //Document fields

    public static final String NAME_FIELD = "column";
    public static final String XPATH_FIELD = "xpath";

    public static final String DATETIMEFORMAT_FIELD = "dateTimeFormat";
    public static final String CATALOG = "catalog";
    public static final String CATALOG_FIELD = "catalogField";
    
    public static final String FOR_EACH = "forEach";
    
    public static final String FROM = "from";
    
    public static final String URL = "url";
  
    public static final String PREFIX = "prefix";
    
    
    public static final String WAIT_SECS = "wait";
  
    public static final String IDCOL = "idcol";

    public static final String RT_EXPRESSION = "/OAI-PMH/ListRecords/resumptionToken";

    public static final String STATUS_EXPRESSION = "header/@status";
    
    public static final String ID_EXPRESSION = "header/identifier";
  
}

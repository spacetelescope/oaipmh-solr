/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.stsci.registry.solr;

import com.lyncode.xoai.model.oaipmh.Verb;
import com.lyncode.xoai.serviceprovider.client.HttpOAIClient;
import com.lyncode.xoai.serviceprovider.exceptions.HttpException;
import com.lyncode.xoai.serviceprovider.parameters.Parameters;
import java.io.IOException;
import java.io.InputStream;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
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

    private DataSource<HttpOAIClient> dataSource;
    private NodeList nodes;
    private int currentNode;
    private static final Logger logger = LogManager.getLogger(OAIPMHEntityProcessor.class.getName());

    @Override
    public void init(Context context) {
        super.init(context);
        dataSource = context.getDataSource();
        rowIterator = null;
        initNodes();
        currentNode = 0;
    }

    private void initNodes(){
        InputStream is = null;
        try {
            String url = context.getEntityAttribute(URL);
            logger.info("URL " + url);
            HttpOAIClient client = new HttpOAIClient(url);
            Parameters parameters = new Parameters();
            parameters.withVerb(Verb.Type.ListRecords);
            parameters.withResumptionToken(null);
            String metadataPrefix = context.getEntityAttribute(PREFIX);
            logger.info("Metadata prefix " + metadataPrefix);
            parameters.withMetadataPrefix(metadataPrefix);
            Calendar fromCalendar = Calendar.getInstance();
            fromCalendar.set(2015, 6, 1);
            parameters.withFrom(fromCalendar.getTime());
            is = client.execute(parameters);
            
            
            
            XPath xpath = XPathFactory.newInstance().newXPath();
            InputSource inputSource = new InputSource(is);
            
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();
            Document document = db.parse(inputSource);
            
            //String expression = "/OAI-PMH/ListRecords/record";
            String expression = context.getEntityAttribute(FOR_EACH);
            logger.info("For each expression " + expression);
            nodes = (NodeList) xpath.evaluate(expression, document, XPathConstants.NODESET);
        } catch (HttpException ex) {
            logger.error("",ex);
        } catch (ParserConfigurationException ex) {
            logger.error("",ex);
        } catch (SAXException ex) {
            logger.error("",ex);
        } catch (IOException ex) {
            logger.error("",ex);
        } catch (XPathExpressionException ex) {
            logger.error("",ex);
        }
        

    }
    
    @Override
    public Map<String, Object> nextRow() {
        XPath xpath = XPathFactory.newInstance().newXPath();
        Map<String,Object> result = new HashMap<>();


        int len = nodes.getLength();
        logger.info("Len = " + len);
        logger.info("Current node = " + currentNode);
        if(currentNode >= len) return null;
        Node node = nodes.item(currentNode);
        for (Map<String, String> field : context.getAllEntityFields()) {
            try {
                if (field.get(XPATH) == null)
                    continue;
                String expression = field.get(XPATH);
                    
                String value = xpath.evaluate(expression,node);
                result.put(field.get("column"), value);
            } catch (XPathExpressionException ex) {
            }
        }
        currentNode++;
        return result;
    }
  
  //Document fields
  public static final String ID = "id";

  public static final String TITLE = "title";

  public static final String DESCRIPTION = "description";

  public static final String PUBLISHER = "publisher";

  public static final String IDENTIFIER = "identifier";

  public static final String SUBJECT = "subject";

  public static final String DATE = "date";

  public static final String INSTRUMENT = "instrument";

  public static final String FOR_EACH = "forEach";

  public static final String URL = "url";
  
  public static final String PREFIX = "prefix";
  
  public static final String XPATH = "xpath";

  public static final String HAS_MORE = "$hasMore";

  public static final String NEXT_URL = "$nextUrl";

}

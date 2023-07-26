/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.stsci.registry.solr;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

/**
 *
 * @author sweissman
 */
public class TestOAI {
    public static final String BASE_URL = "http://vao.stsci.edu/directory/oai.aspx";
    public static void main(String[] args){
        InputStream is = null;
        CloseableHttpResponse response = null;
        CloseableHttpClient httpClient = HttpClients.createDefault();
        try {
            Calendar fromCalendar = Calendar.getInstance();
            fromCalendar.set(2014, Calendar.JANUARY, 1);
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
            String fromDate = df.format(fromCalendar.getTime());
            URI uri = new URIBuilder(BASE_URL)
                        .setParameter("verb", "ListRecords")
                        .setParameter("metadataPrefix", "ivo_vor")
                        .setParameter("from", fromDate)
                        .build();
            //parameters.withFrom(fromCalendar.getTime());
            System.out.println("From time: " + fromCalendar.getTime().toString());
            HttpGet httpget = new HttpGet(uri);
            response = httpClient.execute(httpget);
            HttpEntity entity = response.getEntity();
            is = entity.getContent();


            /*
            BufferedReader br = new BufferedReader(new InputStreamReader(is, "UTF-8"));
            String line = null;
            while((line = br.readLine()) != null){
                System.out.println(line);
            }
            */
            
            
            XPath xpath = XPathFactory.newInstance().newXPath();
            InputSource inputSource = new InputSource(is);
            
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();
            Document document = db.parse(inputSource);

            /*
            
            <field column="id" xpath="/OAI-PMH/ListRecords/record/header/identifier"/>
      <field column="title" xpath="/OAI-PMH/ListRecords/record/metadata/Resource/title"/>
      <field column="description" xpath="/OAI-PMH/ListRecords/record/metadata/Resource/content/description"/>
      <field column="publisher" xpath="/OAI-PMH/ListRecords/record/metadata/Resource/curation/publisher"/>
      <field column="identifier" xpath="/OAI-PMH/ListRecords/record/metadata/Resource/identifier"/>
      <field column="subject" xpath="/OAI-PMH/ListRecords/record/metadata/Resource/content/subject"/>
      <field column="date" xpath="/OAI-PMH/ListRecords/record/header/datestamp" dateTimeFormat="yyyy-MM-dd'T'hh:mm:ss"/>
      <field column="instrument" xpath="/OAI-PMH/ListRecords/record/metadata/Resource/instrument"/>

            */
            String expression = "/OAI-PMH/ListRecords/record";
            NodeList nodes = (NodeList) xpath.evaluate("/OAI-PMH/ListRecords/resumptionToken", document, XPathConstants.NODESET);
            for(int i=0;i<nodes.getLength();i++) {
                Node node = nodes.item(i);
                System.out.println("Resumption token: " + node.getTextContent());
            }
            nodes = (NodeList) xpath.evaluate(expression, document, XPathConstants.NODESET);
            //String responseDate = xpath.evaluate(expression, document);
            //System.out.println("response date " + responseDate);

            int len = nodes.getLength();
            System.out.println("------------------------>Len = " + len);
            for(int i=0;i<nodes.getLength();i++){
                Node node = nodes.item(i);
                String id = xpath.evaluate("header/identifier", node);
                System.out.println("Id = " + id);
                String description = xpath.evaluate("metadata/Resource/title", node);
                System.out.println("Description = " + description);
                String publisher = xpath.evaluate("metadata/Resource/curation/publisher", node);
                System.out.println("Publisher = " + publisher);
                String identifier = xpath.evaluate("metadata/Resource/identifier", node);
                System.out.println("Identifier " + identifier);
                String subject = xpath.evaluate("metadata/Resource/content/subject", node);
                System.out.println("Subject " + subject);
                String date = xpath.evaluate("header/datestamp", node);
                System.out.println("Date " + date);
                String instrument = xpath.evaluate("metadata/Resource/instrument", node);
                System.out.println("Instrument " + instrument);
            }
        } catch (Exception ex) {
            ex.printStackTrace(System.err);
        } finally {
            try {
                if(is != null){
                    is.close();
                }
            } catch (IOException ex) {
               
            }
        }


    }
}

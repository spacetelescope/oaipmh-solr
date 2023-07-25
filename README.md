# oaipmh-solr
A solr plugin for ingesting OAI-PMH records.

## Overview

This plug-in implements an OAI-PMH data import handler for Solr. The data import handler indexes OAI-PMH records from a specified provider. The features provided by this plug-in include:

* Resumption token handling
* Delta queries
* Deleted record handling
* Wait times

**Note**: While this library has been successfully used to harvest records from registries using oai\_dc and ivo\_vo metadata formats, it is a work in progress and is still in testing. Please report any bugs in the issues section.

## Requirements

Version 2.0 of this library is built for solr 8 and tested against solr 8.11. Note that the data import library has been deprecated and will be removed in Solr 9.

## Installation

To install the OAI-PMH , simply download the library and run

    mvn package

from the command line.
Then copy the generated jar from the targets directory to the lib directory of your core.

You may want to modify pom.xml Solr dependencies to correspond to your version of Solr. This plugin has been tested against 5.2.1 and 4.10.2.

## Configuration

Configuration is defined in the standard way for Solr data import handlers, using the standard data-config.xml file, which should be placed in the configuration directory for your Solr core. To enable Solr's data import handler feature, follow the directions on the [Solr wiki](http://wiki.apache.org/solr/DataImportHandler)

Sample configuration files, based on the configuration files distributed with Blacklight's example project, can be found in the sample/config/oai_dc directory in the source code.

Each entity defines fields using a column name that is mapped to a Solr field in Solr's schema.xml and an xpath string that defines the path to the data in the XML document relative to a top level path to a record defined in the forEach attribute of entity element. For standard OAI-PMH provider records in oai\_dc format, the xpath strings defined below in the Dublin Core example section should work without modification. Users of this library may want to define field columns according to their own naming preference and indexing schemes. Here we have defined column names with a suffix that indicates the datatype.

For other metadata types, the user will have to define the paths and change the prefix attribute accordingly.

Date fields should include a dateTimeFormat attribute using java.text.SimpleDateFormat syntax.

To import records from multiple sources add additional entity nodes.

Entity attributes:

* **name** - *Required.* A field required by Solr.
* **catalog** - (Used on conjunction with the catalogField attribute.) The name of the catalog. This value is stored in the field specified by catalogField.
* **catalogField** - (Used in conjunction with the catalog attribute.) Name of the Solr field where the catalog name is stored.
* **from** - Date to start record request from.
* **url** - *Required.* The URL of the OAI-PMH provider interface.
* **processor** - *Required.* Must be set to "edu.stsci.registry.solr.OAIPMHEntityProcessor"
* **forEach** - *Required.* The path to the record element in the XML document.
* **prefix** - *Required.* The metadata prefix to request.
* **wait** - Wait time in seconds between OAI-PMH continuation requests. By default there is no wait time. If the provider provides a wait time, the provider wait time will be used.
* **idCol** - *Required for deleted records to be recognized.* The name of the column used as a unique record identifier for Solr.

### Dublin Core example

    <dataConfig>
      <dataSource type="edu.stsci.registry.solr.OAIPMHDataSource"/>
        <document>
          <entity name="record"
    	    catalog="mycat"
    	    catalogField="catalog_facet"
    	    from="2012-01-01"
    	    url="http://mysite.edu/oai2"
    	    processor="edu.stsci.registry.solr.OAIPMHEntityProcessor"
    	    forEach="/OAI-PMH/ListRecords/record"
    	    prefix="oai_dc"
    	    wait="4"
    	    idCol="id">
            <field column="id" xpath="header/identifier"/>
            <field column="title_t" xpath="metadata/dc/title"/>
            <field column="author_s" xpath="metadata/dc/creator/name"/>
            <field column="creator_s" xpath="metadata/dc/creator/name"/>
            <field column="subject_topic_facet" xpath="metadata/dc/subject"/>
            <field column="description_t" xpath="metadata/dc/description"/>
            <field column="publisher_s" xpath="metadata/dc/publisher"/>
            <field column="contributor_s" xpath="metadata/dc/contributor"/>
            <field column="date_dt" xpath="header/datestamp" dateTimeFormat="yyyy-MM-dd"/>
            <field column="type_facet" xpath="metadata/dc/type"/>
            <field column="format_facet" xpath="metadata/dc/format"/>
            <field column="identifier_s" xpath="metadata/dc/identifier"/>
            <field column="source_s" xpath="metadata/dc/source"/>
            <field column="language_facet" xpath="metadata/dc/language"/>
            <field column="rights_s" xpath="metadata/dc/rights"/>
        </entity>
      </document>
    </dataConfig>


### VO Metadata example

    <dataConfig>
      <dataSource type="edu.stsci.registry.solr.OAIPMHDataSource" />
      <document>
        <entity name="record"
    	    url="http://mysite.edu/oai2"
    	    processor="edu.stsci.registry.solr.OAIPMHEntityProcessor"
    	    forEach="/OAI-PMH/ListRecords/record"
    	    prefix="ivo_vor"
    	    idCol="identifier">
          <field column="id" xpath="header/identifier"/>
          <field column="title_t" xpath="metadata/Resource/title"/>
          <field column="shortName_t" xpath="metadata/Resource/shortName"/>
          <field column="description_s" xpath="metadata/Resource/content/description"/>
          <field column="publisher_t" xpath="metadata/Resource/curation/publisher"/>
          <field column="publisherIVO_facet" xpath="metadata/Resource/curation/publisher/@ivo-id"/>
          <field column="identifier" xpath="metadata/Resource/identifier"/>
          <field column="subject_facet_lc" xpath="metadata/Resource/content/subject"/>
          <field column="type_facet" xpath="metadata/Resource/content/type"/>
          <field column="contentLevel_facet" xpath="metadata/Resource/content/contentLevel"/>
          <field column="date" xpath="header/datestamp" dateTimeFormat="yyyy-MM-dd'T'hh:mm:ss"/>
          <field column="facility_facet"  xpath="metadata/Resource/facility"/>
          <field column="instrument_facet" xpath="metadata/Resource/instrument"/>
          <field column="waveband_facet" xpath="metadata/Resource/coverage/waveband"/>
          <field column="validationLevel_facet" xpath="metadata/Resource/validationLevel"/>
          <field column="rights_facet" xpath="metadata/Resource/rights"/>
          <field column="capabilityType_facet" xpath="metadata/Resource/capability/@type"/>
          <field column="capabilityDescription_t" xpath="metadata/Resource/capability/description"/>
          <field column="resultType_facet" xpath="metadata/Resource/capability/interface/resultType"/>
    <!--
    Additional VO fields that aren't as useful for indexing.
          <field column="creator" xpath="metadata/Resource/curation/creator/name"/>
          <field column="contributor" xpath="metadata/Resource/curation/contributor"/>
          <field column="version" xpath="metadata/Resource/curation/version"/>
          <field column="contactName" xpath="metadata/Resource/curation/contact/name"/>
          <field column="contactAddress" xpath="metadata/Resource/curation/contact/address"/>
          <field column="contactEmail" xpath="metadata/Resource/curation/contact/email"/>
          <field column="contactTelephone" xpath="metadata/Resource/curation/contact/telephone"/>
          <field column="source" xpath="metadata/Resource/content/source"/>
          <field column="referenceUrl" xpath="metadata/Resource/content/referenceURL"/>
          <field column="relationshipType" xpath="metadata/Resource/content/relationship/relationshipType"/>
          <field column="relatedResource" xpath="metadata/Resource/content/relationship/relatedResource"/>
         <field column="footprint" xpath="metadata/Resource/coverage/footprint"/>
         <field column="validationLevel" xpath="metadata/Resource/capability/validationLevel"/>
    -->
        </entity>
      </document>
    </dataConfig>

## Test Script

There is a simple test script that demonstrates how to parse the output of the MAST registry OAI-PMH endpoint.

Once the jar is built, you should be able to run this script with the following command:

    java -cp target/lib/httpclient-4.4.1.jar:target/lib/httpcore-4.4.14.jar:target/lib/commons-logging-1.2.jar src/test/java/edu/stsci/registry/solr/TestOAI.java

The script does a single call, parses fields out of the records and also prints the resumption token.

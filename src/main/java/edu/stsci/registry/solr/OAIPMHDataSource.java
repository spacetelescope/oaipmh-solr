/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.stsci.registry.solr;

import com.lyncode.xoai.serviceprovider.client.HttpOAIClient;
import java.util.Properties;
import org.apache.solr.handler.dataimport.Context;
import org.apache.solr.handler.dataimport.DataSource;

/**
 *
 * @author sweissman
 */
public class OAIPMHDataSource extends DataSource<HttpOAIClient> {

    @Override
    public void init(Context cntxt, Properties prprts) {
    }

    @Override
    public HttpOAIClient getData(String query) {
        return null;
    }

    @Override
    public void close() {
    }
    
}

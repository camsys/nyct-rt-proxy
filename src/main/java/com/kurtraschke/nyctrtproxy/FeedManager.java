/*
 * Copyright (C) 2017 Cambridge Systematics, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.kurtraschke.nyctrtproxy;

import org.apache.http.Header;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.params.CoreProtocolPNames;
import org.apache.http.params.HttpProtocolParams;
import org.onebusaway.cloud.api.Credential;
import org.onebusaway.cloud.api.ExternalServices;
import org.onebusaway.cloud.api.ExternalServicesBridgeFactory;
import org.onebusaway.cloud.api.InputStreamConsumer;
import org.slf4j.LoggerFactory;


import javax.annotation.PreDestroy;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

/**
 * Manage feed requests, their protocols and their associated credentials mechanisms.
 */
public class FeedManager {

    private static final org.slf4j.Logger _log = LoggerFactory.getLogger(FeedManager.class);

    private Credential _defaultCredential;
    private CloseableHttpClient _httpClient;
    private int DEFAULT_TIME_OUT = 5000;
    private int _timeout = DEFAULT_TIME_OUT;
    public void setTimeout(int timeout) {
        _timeout = timeout;
    }
    private HttpClientConnectionManager _connectionManager;
    private static final String BASE_URL = "http://datamine.mta.info/mta_esi.php";
    private String _url = BASE_URL;
    public String getBaseUrl() { return _url; }
    public void setBaseUrl(String url) {
        _url = url;
    }
    public void setDefaultCredential(Credential credential) {
        _defaultCredential = credential;
    }
    private Map<String, String> _feedToUrlOverride = new HashMap<>();
    public java.util.Map<String, String> getOverrideFeedToUrlMap() {
        return _feedToUrlOverride;
    }
    public void setFeedToUrlOverride(Map<String, String> kv) {
        for (String key: kv.keySet()) {
            _feedToUrlOverride.put(key, kv.get(key));
        }
    }
    public String getFeedOrDefault(String feedId) {
        if (_feedToUrlOverride.containsKey(feedId)) {
            return _feedToUrlOverride.get(feedId);
        }
        return _url;
    }
    private ExternalServices _es;
    private ExternalServices getExternalServices() {
        if (_es == null) {
            _es = new ExternalServicesBridgeFactory().getExternalServices();
        }
        return _es;
    }

    private Map<String, Credential> _feedToCredentialOverride = new HashMap<String, Credential>();
    public Map<String, Credential> getFeedToCredentialOverride() {
        return _feedToCredentialOverride;
    }
    public void setFeedToCredentialOverride(Map<String, Credential> kv) {
        for (String key: kv.keySet()) {
            _feedToCredentialOverride.put(key, kv.get(key));
        }
    }

    public Credential getCredentialOrDefault(String feedId) {
        if (_feedToCredentialOverride.containsKey(feedId)) {
            _log.debug("feedId " + feedId + " returning credential " + _feedToCredentialOverride.get(feedId));
            return _feedToCredentialOverride.get(feedId);
        }
        _log.debug("feedId " + feedId + " returning default credential " + _defaultCredential);
        return _defaultCredential;
    }

    public void setHttpClientConnectionManager(HttpClientConnectionManager connectionManager) {
        _connectionManager = connectionManager;
    }

    public CloseableHttpClient getHttpClient() {
        if (_httpClient == null) {
            if (_connectionManager == null) {
                throw new IllegalStateException("_connectionManager is null");
            }
            // avoid stuck connections
            RequestConfig requestConfig = RequestConfig.custom()
                    .setSocketTimeout(_timeout)
                    .setConnectTimeout(_timeout)
                    .setConnectionRequestTimeout(_timeout)
                    .setStaleConnectionCheckEnabled(true).build();
            SocketConfig socketConfig = SocketConfig.custom().setSoTimeout(_timeout).build();
            _httpClient = HttpClientBuilder.create()
                    .setConnectionManager(_connectionManager)
                    .setDefaultRequestConfig(requestConfig)
                    .setDefaultSocketConfig(socketConfig)
                    .build();
        }
        return _httpClient;
    }

    @PreDestroy
    public void stop() {
        _connectionManager.shutdown();
    }


    public InputStream getStream(String feedUrl, String feedId) throws IOException {
        return getStream(feedUrl,feedId,null);
    }

    public InputStream getStream(String feedUrl, String feedId, String userAgent) throws IOException {
        Credential credential = getCredentialOrDefault(feedId);

        if (Credential.CredentialType.API_KEY_HEADER.equals(credential.getType())) {
            HttpGet get = new HttpGet(feedUrl);
            _log.debug("header " +  credential.getKeyName() + " = " + credential.getKey() );
            get.setHeader(credential.getKeyName(), credential.getKey());//"x-api-key"
            _log.debug("getStream [header](" + get.getRequestLine().getUri() + ", " + feedId + ") cred=" + credential);
            return getUrl(get,userAgent);
        } else if (Credential.CredentialType.API_KEY_PARAM.equals(credential.getType())) {
            try {
                URIBuilder ub = new URIBuilder(_url);
                // use api key as param
                ub.addParameter("key", credential.getKey());
                ub.addParameter("feed_id", feedId);
                feedUrl = ub.build().toString();
                HttpGet get = new HttpGet(feedUrl);
                _log.debug("getStream [param](" + get.getRequestLine().getUri() + ", " + feedId + ") cred=" + credential);
                return getUrl(get);
            } catch (URISyntaxException use) {
                _log.error("invalid uri ", use);
                throw new IOException(use);
            }
        } else if (Credential.CredentialType.EXTERNAL_PROFILE.equals(credential.getType())) {
            ExternalServices es = getExternalServices();
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            _log.debug("getStream(S3)[" + feedUrl + ")");
            es.getFileAsStream(feedUrl.toString(), new InputStreamConsumer() {
                        @Override
                        public void accept(InputStream inputStream) throws IOException {
                            byte[] buffer = new byte[1024];
                            int len;
                            while ((len = inputStream.read(buffer)) > -1) {
                                baos.write(buffer, 0, len);
                            }
                            baos.flush();
                        }
                    }, /*profile*/ credential.getKey(),
                    /*region*/ credential.getValue());
            return new ByteArrayInputStream(baos.toByteArray());
        } else if (Credential.CredentialType.NO_OP.equals(credential.getType())) {
            HttpGet get = new HttpGet(feedUrl);
            _log.debug("getStream[noop](" + feedUrl + ")");
            return getUrl(get);
        } else {
            throw new UnsupportedOperationException("unknown credential type " + credential.getType());
        }
    }



    private InputStream getUrl(HttpGet get) throws IOException {
        return getUrl(get,null);
    }

    private InputStream getUrl(HttpGet get,String userAgent) throws IOException {
        if (getHttpClient() == null)  throw new IllegalStateException("_httpClient is null!");
        CloseableHttpClient client = getHttpClient();
        CloseableHttpResponse response = client.execute(get);
        if(userAgent!=null) {
            response.setHeader("User-Agent", userAgent);
        }
        if (response.getStatusLine().getStatusCode() != 200) {
            _log.error("" + get.getRequestLine() + " (" +
                    + response.getStatusLine().getStatusCode() + ") failed with: " + response.getEntity().getContent().toString());
        }
        InputStream streamContent = response.getEntity().getContent();
        return streamContent;
    }

}

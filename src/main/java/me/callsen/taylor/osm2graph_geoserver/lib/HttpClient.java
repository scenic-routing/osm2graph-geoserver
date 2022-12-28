package me.callsen.taylor.osm2graph_geoserver.lib;

import java.io.IOException;

import org.apache.hc.client5.http.HttpResponseException;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;

// based on https://reflectoring.io/comparison-of-java-http-clients/ Synchronous POST Request with Apahce HTTP Client
public class HttpClient {

  public static String invokePost(String url, String authHeader, String postBody, String contentType) throws IOException, HttpException {
   
    StringEntity stringEntity = new StringEntity(postBody);
    HttpPost httpPost = new HttpPost(url);

    httpPost.setEntity(stringEntity);
    httpPost.setHeader("Content-type", contentType);
    httpPost.setHeader("Authorization", authHeader);

    CloseableHttpClient httpClient = HttpClients.createDefault();
    
    CloseableHttpResponse response = httpClient.execute(httpPost);

    String result = "";
    HttpEntity entity = response.getEntity();
    if (entity != null) {
      result = EntityUtils.toString(entity);
    }

    if (response.getCode() < 200 || response.getCode() > 299) {
      // add response body into exception, if set
      throw new HttpResponseException(response.getCode(), result);
    }

    return result;
  }
  
}

package me.callsen.taylor.osm2graph_geoserver.data;

import java.io.IOException;
import java.util.Base64;

import javax.ws.rs.core.MediaType;

import org.apache.hc.client5.http.HttpResponseException;
import org.apache.hc.core5.http.HttpException;
import org.json.JSONObject;

import me.callsen.taylor.osm2graph_geoserver.Config;
import me.callsen.taylor.osm2graph_geoserver.lib.HttpClient;

public class GeoServerRestApi {

  private JSONObject geoserverConfig;

  private String workspaceName;

  private String storeName;

  private String baseUrl;

  private String authHeader;

  private HttpClient httpClient;

  public GeoServerRestApi(Config appConfig) {
    this(appConfig, new HttpClient());
  }

  // added to allow mocking of http client during tests
  public GeoServerRestApi(Config appConfig, HttpClient httpClient) {
    this.geoserverConfig = appConfig.getGeoServerConfig();
    this.workspaceName = geoserverConfig.getString("workspaceName");
    this.storeName = geoserverConfig.getString("storeName");

    // set base url
    String host = this.geoserverConfig.getString("host");
    String port = this.geoserverConfig.getString("port");
    this.baseUrl = String.format("http://%s:%s/geoserver/rest", host, port);

    // set auth header
    String username = this.geoserverConfig.getString("user");
    String password = this.geoserverConfig.getString("password");
    this.authHeader = getBasicAuthenticationHeader(username, password);
    
    this.httpClient = httpClient;

    createWorkspace();
    createPostGisStore(appConfig);
  }

  public void createWorkspace() {
    String postBody = "<workspace>" +
      "<name>" + this.workspaceName + "</name>" +
    "</workspace>";

    String workspacesUrl = String.format("%s/workspaces", this.baseUrl);

    try {
      this.httpClient.invokePost(workspacesUrl, this.authHeader, postBody, MediaType.APPLICATION_XML);
    } catch (IOException | HttpException e) {
      
      if (e instanceof HttpResponseException && ((HttpResponseException) e).getStatusCode() == 409) {
        System.out.println("geoserver workspace '" + this.workspaceName + "' already exists");
      } else {
        System.out.println("FAILED to create geoserver workspace: " + e.getMessage());
        e.printStackTrace();
      }
    }

  }

  public void createPostGisStore(Config appConfig) {

    JSONObject postgisConfig = appConfig.getDbConfig();

    String postBody = "<dataStore>" +
      "<name>" + this.storeName + "</name>" +
      "<connectionParameters>" +
        "<host>" + postgisConfig.getString("host") + "</host>" +
        "<port>" + postgisConfig.getString("port") + "</port>" +
        "<database>" + postgisConfig.getString("database") + "</database>" +
        "<user>" + postgisConfig.getString("user") + "</user>" +
        "<passwd>" + postgisConfig.getString("password") + "</passwd>" +
        "<schema>" + postgisConfig.getString("schema") + "</schema>" +
        "<dbtype>postgis</dbtype>" +
      "</connectionParameters>" +
    "</dataStore>";

    String datastoresUrl = String.format("%s/workspaces/%s/datastores", this.baseUrl, this.workspaceName);

    try {
      this.httpClient.invokePost(datastoresUrl, this.authHeader, postBody, MediaType.APPLICATION_XML);
    } catch (HttpException | IOException e) {
      
      if (e instanceof HttpResponseException && ((HttpResponseException) e).getMessage().contains("already exists")) {
        System.out.println("geoserver store '" + this.storeName + "' already exists");
      } else {
        System.out.println("FAILED to create postgis store '" + this.storeName + "' in geoserver with message: " + e.getMessage());
        e.printStackTrace();
      }
    }
  }

  public void createFeatureType(String featureTypeName) {
    String postBody = "<featureType>" +
      "<name>" + featureTypeName + "</name>" +
      "<enabled>true</enabled>" +
      "<nativeCRS>EPSG:4326</nativeCRS>" +
      "<srs>EPSG:3857</srs>" +
      "<projectionPolicy>REPROJECT_TO_DECLARED</projectionPolicy>" +
      "<recalculate>nativebbox,latlonbbox</recalculate>" +
    "</featureType>";

    String featureTypesUrl = String.format("%s/workspaces/%s/datastores/%s/featuretypes", this.baseUrl, this.workspaceName, this.storeName);

    try {
      this.httpClient.invokePost(featureTypesUrl, this.authHeader, postBody, MediaType.APPLICATION_XML);
    } catch (HttpException | IOException e) {
      if (e instanceof HttpResponseException && ((HttpResponseException) e).getMessage().contains("already exists")) {
        System.out.println("geoserver featureType '" + featureTypeName + "' already exists");
      } else {
        System.out.println("FAILED to create geoserver featuretype '" + featureTypeName + "' with message: " + e.getMessage());
        e.printStackTrace();
      }
    }

  }

  public String getBaseUrl() {
    return this.baseUrl;
  }

  // https://www.baeldung.com/java-httpclient-basic-auth
  private String getBasicAuthenticationHeader(String username, String password) {
    String valueToEncode = username + ":" + password;
    return "Basic " + Base64.getEncoder().encodeToString(valueToEncode.getBytes());
  }
  
}

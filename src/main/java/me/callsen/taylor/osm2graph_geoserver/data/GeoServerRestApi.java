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

  public static void initializeWorkspace(Config appConfig) {
    JSONObject geoserverConfig = appConfig.getGeoServerConfig();
    String workspaceName = geoserverConfig.getString("workspaceName");
    String storeName = geoserverConfig.getString("storeName");

    String baseUrl = getBaseUrl(geoserverConfig);
    String authHeader = getAuthHeader(geoserverConfig);
    
    createWorkspace(baseUrl, authHeader, "routing");
    createPostGisStore(baseUrl, authHeader, workspaceName, storeName, appConfig);
  }

  public static void createWorkspace(String baseUrl, String authHeader, String workspaceName) {
    String postBody = "<workspace>" +
      "<name>" + workspaceName + "</name>" +
    "</workspace>";

    String workspacesUrl = String.format("%s/workspaces", baseUrl);

    try {
      HttpClient.invokePost(workspacesUrl, authHeader, postBody, MediaType.APPLICATION_XML);
    } catch (IOException | HttpException e) {
      
      if (e instanceof HttpResponseException && ((HttpResponseException) e).getStatusCode() == 409) {
        System.out.println("geoserver workspace '" + workspaceName + "' already exists");
      } else {
        System.out.println("FAILED to create geoserver workspace: " + e.getMessage());
        e.printStackTrace();
      }
    }

  }

  public static void createPostGisStore(String baseUrl, String authHeader, String workspaceName, String datastoreName, Config appConfig) {

    JSONObject postgisConfig = appConfig.getDbConfig();

    String postBody = "<dataStore>" +
      "<name>" + datastoreName + "</name>" +
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

    String datastoresUrl = String.format("%s/workspaces/%s/datastores", baseUrl, workspaceName);

    try {
      HttpClient.invokePost(datastoresUrl, authHeader, postBody, MediaType.APPLICATION_XML);
    } catch (HttpException | IOException e) {
      
      if (e instanceof HttpResponseException && ((HttpResponseException) e).getMessage().contains("already exists")) {
        System.out.println("geoserver store '" + datastoreName + "' already exists");
      } else {
        System.out.println("FAILED to create postgis store '" + datastoreName + "' in geoserver with message: " + e.getMessage());
        e.printStackTrace();
      }
    }
  }

  public static void createFeatureType(String baseUrl, String authHeader, String workspaceName, String datastoreName, String featureTypeName) {
    String postBody = "<featureType>" +
      "<name>" + featureTypeName + "</name>" +
      "<enabled>true</enabled>" +
      "<nativeCRS>EPSG:4326</nativeCRS>" +
      "<srs>EPSG:3857</srs>" +
	    "<projectionPolicy>REPROJECT_TO_DECLARED</projectionPolicy>" +
    "</featureType>";

    String featureTypesUrl = String.format("%s/workspaces/%s/datastores/%s/featuretypes", baseUrl, workspaceName, datastoreName);

    try {
      HttpClient.invokePost(featureTypesUrl, authHeader, postBody, MediaType.APPLICATION_XML);
    } catch (HttpException | IOException e) {
      if (e instanceof HttpResponseException && ((HttpResponseException) e).getMessage().contains("already exists")) {
        System.out.println("geoserver featureType '" + featureTypeName + "' already exists");
      } else {
        System.out.println("FAILED to create geoserver featuretype '" + featureTypeName + "' with message: " + e.getMessage());
        e.printStackTrace();
      }
    }

  }

  public static String getBaseUrl(JSONObject geoserverConfig) {
    String host = geoserverConfig.getString("host");
    String port = geoserverConfig.getString("port");
    return String.format("http://%s:%s/geoserver/rest", host, port);
  }

  public static String getAuthHeader(JSONObject geoserverConfig) {
    String username = geoserverConfig.getString("user");
    String password = geoserverConfig.getString("password");
    return getBasicAuthenticationHeader(username, password);
  }

  // https://www.baeldung.com/java-httpclient-basic-auth
  private static String getBasicAuthenticationHeader(String username, String password) {
    String valueToEncode = username + ":" + password;
    return "Basic " + Base64.getEncoder().encodeToString(valueToEncode.getBytes());
  }
  
}

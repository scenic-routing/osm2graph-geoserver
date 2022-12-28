package me.callsen.taylor.osm2graph_geoserver.data;

import static me.callsen.taylor.osm2graph_geoserver.Main.ASSOCIATED_DATA_PROPERTY;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;
import org.neo4j.graphdb.Relationship;

import me.callsen.taylor.osm2graph_geoserver.Config;

public class PostgisDb {

  private String postgisSchema;

  private Connection conn;
  
  public PostgisDb( Config appConfig, String postgisUrl ) throws Exception {
    
    //initialize connection to Postgis Shape Source
    this.postgisSchema = appConfig.getDbConfig().getString("schema");

    conn = DriverManager.getConnection(postgisUrl,appConfig.getDbConfig().getString("user"), appConfig.getDbConfig().getString("password"));
    System.out.println( "PostgisDb initialized and will persist through app completion" );

    // ensure postgis schema exists
    String schemaName = appConfig.getDbConfig().getString("schema");
    String sequenceSql = String.format("CREATE SCHEMA IF NOT EXISTS %s", schemaName);
    System.out.println( String.format("ensure schema '%s' exists: %d", schemaName, executeUpdate(sequenceSql)) );
  }

  public void createVisualizationTable(String associatedDataProperty, String associatedDataPropertyTableName) {

    System.out.println(String.format("creating visualization table for property %s named %s", associatedDataProperty, associatedDataPropertyTableName));
    
    // create id sequence
    String sequenceSql = String.format("CREATE SEQUENCE IF NOT EXISTS %s.%s_id_seq INCREMENT 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1;", this.postgisSchema, associatedDataPropertyTableName);
    System.out.println( String.format("sequence creation: %d", executeUpdate(sequenceSql)) );
    
    // create table - must double quote some column names to ensure camelcase
    String tableSql = String.format("CREATE TABLE IF NOT EXISTS %s.\"%s\"( id bigint NOT NULL DEFAULT nextval('%s.%s_id_seq'::regclass), osm_id bigint, geom geometry(Geometry,4326), \"%s\" json, \"relationshipData\" json )",
      this.postgisSchema,
      associatedDataPropertyTableName,
      this.postgisSchema,
      associatedDataPropertyTableName,
      ASSOCIATED_DATA_PROPERTY);
    System.out.println( String.format("table creation: %d", executeUpdate(tableSql)) );      
  }

  public void writeVisualizationTableRow(String associatedDataPropertyTableName, String associatedDataProperty, Relationship rel) {

    // derive optional values
    long rel_osm_id = rel.getProperty("osm_id") instanceof Integer ? (Integer) rel.getProperty("osm_id") : (Long)rel.getProperty("osm_id");
    JSONArray associatedData = new JSONArray( (String) rel.getProperty(associatedDataProperty) );
    String wayGeometry = (String) rel.getProperty("way");

    // flatten associatedData to JSON array
    JSONArray associdatedDataArray = new JSONArray();
    for (int i=0; i < associatedData.length(); ++i) {
      JSONObject associatedDataEntry = associatedData.getJSONObject(i);
      associdatedDataArray.put(associatedDataEntry);
    }

    // flatten relationship to JSON object
    JSONObject relationshipJson = new JSONObject();
    for (Map.Entry<String, Object> entry : rel.getAllProperties().entrySet()) {
      String propertyName = entry.getKey();
      // skip associatedData and geom properties since written on row already
      if (propertyName.equals(ASSOCIATED_DATA_PROPERTY) || 
          propertyName.equals(associatedDataPropertyTableName) ||  
          propertyName.equals("way")) {
        continue;
      }
      relationshipJson.put(propertyName, entry.getValue());
    }
    
    //escape single quotes (required by postgis)
    String associatedDataJsonString = associdatedDataArray.toString().replaceAll("'","''");
    String relationshipJsonString = relationshipJson.toString().replaceAll("'","''");

    //execute insert statement
    String insertSql = String.format("INSERT INTO %s.%s (osm_id, geom, \"%s\", \"relationshipData\") VALUES (%s, ST_GeomFromText('%s',4326), '%s'::json, '%s'::json);", 
      this.postgisSchema,
      associatedDataPropertyTableName,
      ASSOCIATED_DATA_PROPERTY,
      rel_osm_id,
      wayGeometry,
      associatedDataJsonString,
      relationshipJsonString);

    executeUpdate( insertSql );
  }
  
  public String executeQuery( String sql, int columnNum ) {
    
    String returnString = null;
    
    try {
    
      Statement st = this.conn.createStatement();
      ResultSet rs = st.executeQuery(sql);
      if (!rs.next()) return "";
      returnString = rs.getString(columnNum);
      rs.close();
      st.close();
      
    } catch(Exception e) { e.printStackTrace(); }
    
    return returnString;
    
  }
  
  public int executeUpdate( String sql ) {
    
    int responseInt = -1;
    
    try {
    
      Statement st = this.conn.createStatement();
      responseInt = st.executeUpdate(sql);
      
    } catch(Exception e) { System.out.println( "ERROR executing Update Query:" + sql ); }
    
    return responseInt;

  }

  public void close() throws SQLException {
    System.out.println("PostgisDb shutdown");
    this.conn.close();
  }
  
}

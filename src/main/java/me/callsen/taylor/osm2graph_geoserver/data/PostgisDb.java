package me.callsen.taylor.osm2graph_geoserver.data;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import org.json.JSONObject;
import org.neo4j.graphdb.Relationship;

import me.callsen.taylor.osm2graph_geoserver.Config;

import static me.callsen.taylor.osm2graph_geoserver.Main.ASSOCIATED_DATA_PROPERTY;

public class PostgisDb {

  private String postgisSchema;

  private String osmId;

  private Connection conn;

  private Set<String> associatedDataTablesCreated = new HashSet<>();
  
  public PostgisDb( Config appConfig, String postgisUrl ) throws Exception {
    
    //initialize connection to Postgis Shape Source
    this.postgisSchema = appConfig.getDbConfig().getString("schema");
    this.osmId = appConfig.getString("osmId");

    conn = DriverManager.getConnection(postgisUrl,appConfig.getDbConfig().getString("user"), appConfig.getDbConfig().getString("password"));
    System.out.println( "PostgisDb initialized and will persist through app completion" );
  }

  public String ensureVisualizationTableExists(String associatedDataProperty) {

    String associatedDataPropertyTableName = String.format("%s_vis_%s", this.osmId, associatedDataProperty);

    // ensure table hasn't already been created by checking local cached object
    if (!associatedDataTablesCreated.contains(associatedDataProperty)) {
      
      System.out.println(String.format("creating visualization table for property %s named %s", associatedDataProperty, associatedDataPropertyTableName));
      
      // create id sequence
      String sequenceSql = String.format(" CREATE SEQUENCE IF NOT EXISTS %s.%s_id_seq INCREMENT 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1;", this.postgisSchema, associatedDataPropertyTableName);
      System.out.println( String.format("sequence creation: %d", executeUpdate(sequenceSql)) );
      
      // create table - must double quote some column names to ensure camelcase
      String tableSql = String.format("CREATE TABLE IF NOT EXISTS %s.\"%s\"( id bigint NOT NULL DEFAULT nextval('%s.%s_id_seq'::regclass), osm_id bigint, \"%s\" character varying, geom geometry(Geometry,4326), \"relationshipData\" hstore DEFAULT ''::hstore )",
        this.postgisSchema,
        associatedDataPropertyTableName,
        this.postgisSchema,
        associatedDataPropertyTableName,
        ASSOCIATED_DATA_PROPERTY);
      System.out.println( String.format("table creation: %d", executeUpdate(tableSql)) );      
      
      associatedDataTablesCreated.add(associatedDataProperty);
    }

    return associatedDataPropertyTableName;
  }

  public void writeVisualizationTableRow(String associatedDataPropertyTableName, String associatedDataProperty, Relationship rel) {

    //derive optional values
    long rel_osm_id = rel.getProperty("osm_id") instanceof Integer ? (Integer) rel.getProperty("osm_id") : (Long)rel.getProperty("osm_id");
    String associatedData = (String) rel.getProperty(associatedDataProperty);
    String wayGeometry = (String) rel.getProperty("way");

    // TODO: flatten relationship to JSON object
    JSONObject relationshipJson = new JSONObject();
    
    // TODO: figure out how to insert visualization row

    //execute insert statement
    String insertSql = String.format("INSERT INTO %s.%s (osm_id, \"%s\", geom, \"relationshipData\") VALUES (%s, %s, ST_GeomFromText('%s',4326), '%s');", 
      this.postgisSchema,
      associatedDataPropertyTableName,
      ASSOCIATED_DATA_PROPERTY,
      rel_osm_id,
      associatedData,
      wayGeometry,
      relationshipJson.toString());

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

package org.apache.cloudbigtable;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Before;

public class JdbcTest extends TestCase {

  @Before
  public void setUp() throws ClassNotFoundException {
    /*
    final Logger logger = (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
    logger.setLevel(Level.INFO);
    */
  }

  public void testInsert() throws SQLException {
    Connection con = getJDBCConnection();
    String query = "upsert into us_population4(state, city, population) values ('NY', 'New York', 200)";
    //+ "upsert into us_population(state, city, population) values ('WA', 'Washington', 2)";
    try (Statement createStm = con.createStatement()) {
      createStm.execute(query);
    }
    con.commit();

  }
  public void testInsertIntoPopulation() throws SQLException {
    Connection con = getJDBCConnection();
    String query = "upsert into us_city_population(city, population) values ('Seattle', 200)";
    //+ "upsert into us_population(state, city, population) values ('WA', 'Washington', 2)";
    try (Statement createStm = con.createStatement()) {
      createStm.execute(query);
    }
    con.commit();

  }

  private org.apache.hadoop.hbase.client.Connection createHBaseConnection() throws IOException {
    Configuration hBaseConfig = HBaseConfiguration.create();
    hBaseConfig.set("hbase.zookeeper.quorum", "localhost");
    hBaseConfig.set("hbase.zookeeper.property.clientPort", "2181");
    return ConnectionFactory.createConnection(hBaseConfig);
  }

  private org.apache.hadoop.hbase.client.Connection createCbtConnection() {
    return BigtableConfiguration.connect(System.getenv("GCP_PROJECT_ID"),
        System.getenv("GCP_INSTANCE_ID"));
  }

  private org.apache.hadoop.hbase.client.Connection createRawConnection() throws IOException {
    return System.getenv("IS_CBT") != null ? createCbtConnection() : createHBaseConnection();
  }

  public void testScanRaw() throws IOException {
    try (org.apache.hadoop.hbase.client.Connection c = createRawConnection()) {
      Table t = c.getTable(TableName.valueOf("US_POPULATION4"));
      ResultScanner s = t.getScanner(new Scan());
      Result r = null;
      while ((r = s.next()) != null) {
        for (Cell cell : r.listCells()) {
          System.out.print(
              bytesToHex(cell.getQualifierArray(), cell.getQualifierOffset(),
                  cell.getQualifierLength()) + " " + bytesToHex(cell.getValueArray(),
                  cell.getValueOffset(), cell.getValueLength()) + " ");
        }
        System.out.println("-----");
      }
    }
  }

  private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

  public static String bytesToHex(byte[] bytes, int start, int length) {
    char[] hexChars = new char[length * 2];
    for (int j = start; j < bytes.length && j < start + length; j++) {
      int v = bytes[j] & 0xFF;
      hexChars[(j - start) * 2] = HEX_ARRAY[v >>> 4];
      hexChars[(j - start) * 2 + 1] = HEX_ARRAY[v & 0x0F];
    }
    return new String(hexChars);
  }

  public void testJoin() throws SQLException {
    Connection con = getJDBCConnection();
    String query =
        "select a.city, b.population from US_POPULATION4 a inner join us_city_population b on\n"
            + "    a.state = b.city\n"
            + "    order by a.state";
    try (Statement stmt = con.createStatement()) {
      ResultSet rs = stmt.executeQuery(query);
      while (rs.next()) {
        String city = rs.getString("a.city");
        int population = rs.getInt("b.population");
        System.out.println(city + " " + population);
      }
    }

  }

  public void testSelect() throws SQLException {
    Connection con = getJDBCConnection();
    String query = "select state, city, population from US_POPULATION4 where state = 'NY'";
    try (Statement stmt = con.createStatement()) {
      ResultSet rs = stmt.executeQuery(query);
      while (rs.next()) {
        String state = rs.getString("state");
        String city = rs.getString("city");
        int population = rs.getInt("population");
        System.out.println(state + " " + city + " " + population);
      }
    }
  }

  public void testCreateTable() throws SQLException {
    Connection con = getJDBCConnection();
    String createQuery = "CREATE TABLE IF NOT EXISTS us_population4 (\n"
        + "    state CHAR(2) NOT NULL,\n"
        + "    city VARCHAR,\n"
        + "    population BIGINT\n"
        + "    CONSTRAINT my_pk PRIMARY KEY (state))";
    try (Statement createStm = con.createStatement()) {
      createStm.execute(createQuery);
    }
  }

  public void testCreatePopulationTable() throws SQLException {
    Connection con = getJDBCConnection();
    String createQuery = "CREATE TABLE IF NOT EXISTS us_city_population (\n"
        + "    city VARCHAR NOT NULL,\n"
        + "    population BIGINT\n"
        + "    CONSTRAINT my_pk PRIMARY KEY (city))";
    try (Statement createStm = con.createStatement()) {
      createStm.execute(createQuery);
    }
  }

  public Connection getJDBCConnection() throws SQLException {
    Properties connectionProps = new Properties();
    Connection conn = DriverManager.getConnection(System.getenv("IS_CBT") != null ?
            "google" :
            "jdbc:phoenix:localhost:2181:/hbase",
        connectionProps);
    System.out.println("Connected to database");
    return conn;
  }

}

import org.junit.Test;
import org.junit.Assert;

import java.io.*;
import java.util.*;
import java.text.SimpleDateFormat;
import java.text.ParseException;


import java.io.IOException;

import static org.junit.Assert.assertEquals;

import ch.scigility.kafka.StreamProcessor;
import ch.scigility.kafka.StreamProducer;
import ch.scigility.kafka.ConsumerDelta;
import ch.scigility.kafka.canonical.Partners;
import ch.scigility.kafka.canonical.avro.ContractsSchema;
import ch.scigility.kafka.canonical.Landing;
import ch.scigility.kafka.canonical.ChangedFieldsList;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import oracle.jdbc.pool.OracleDataSource;
import java.sql.Connection;
public class StreamProducerTest {
  String contractJSON = "{\n  \"commandScn\": \"1195604\",\n  "+
  "\"commandCommitScn\": \"1195604\",\n  \"commandSequence\": \"0\",\n  "+
  "\"commandType\": \"INSERT\",\n  \"commandTimestamp\": \"2017-08-08 09:36:43+00:000\",\n  "+
  "\"objectDBName\": \"DB1\",\n  \"objectSchemaName\": \"POC1\",\n  "+
  "\"objectId\": \"CORE_CONTRACTS\",\n  \"changedFieldsList\": [\n    {\n      "+
  "\"fieldId\": \"COCO_ID\",\n      \"fieldType\": \"NUMBER\",\n      "+
  "\"fieldValue\": \"1\",\n      \"fieldChanged\": \"Y\"\n    },\n    "+
  "{\n      \"fieldId\": \"COCO_TYPE\",\n      \"fieldType\": \"NUMBER\",\n"+
  "      \"fieldValue\": \"2\",\n      \"fieldChanged\": \"Y\"\n    },\n  "+
  "  {\n      \"fieldId\": \"COOC_COVERAGE\",\n      "+
  "\"fieldType\": \"NUMBER\",\n      \"fieldValue\": \"6771897\",\n  "+
  "    \"fieldChanged\": \"Y\"\n    },\n "+
  "   {\n      \"fieldId\": \"COCO_ANNUAL_PREMIUM\",\n    "+
  "  \"fieldType\": \"NUMBER\",\n      \"fieldValue\": \"67718\",\n   "+
  "   \"fieldChanged\": \"Y\"\n    },\n   "+
  " {\n      \"fieldId\": \"COCO_START_DATE\",\n   "+
  "   \"fieldType\": \"DATE\",\n  "+
  "    \"fieldValue\": \"2017-07-31 14:32:00+00:000\",\n   "+
  "   \"fieldChanged\": \"Y\"\n    },\n "+
  "   {\n      \"fieldId\": \"COCO_END_DATE\",\n   "+
  "   \"fieldType\": \"DATE\",\n  "+
  "    \"fieldValue\": \"2023-07-30 14:32:00+00:000\",\n  "+
  "    \"fieldChanged\": \"Y\"\n    },\n "+
  "   {\n      \"fieldId\": \"COCO_COCU_ID\",\n    "+
  "  \"fieldType\": \"NUMBER\",\n      \"fieldValue\": \"80\",\n  "+
  "    \"fieldChanged\": \"Y\"\n    },\n  "+
  "  {\n      \"fieldId\": \"COCO_COAG_ID\",\n   "+
  "   \"fieldType\": \"NUMBER\",\n      \"fieldValue\": \"484\",\n  "+
  "    \"fieldChanged\": \"Y\"\n    },\n  "+
  "  {\n      \"fieldId\": \"COCO_TOTAL_PAID_PREMIUMS\",\n  "+
  "    \"fieldType\": \"NUMBER\",\n      \"fieldValue\": \"135436\",\n   "+
  "   \"fieldChanged\": \"Y\"\n    },\n  "+
  "  {\n      \"fieldId\": \"COCO_TOTAL_PAID_CLAIMS\",\n   "+
  "   \"fieldType\": \"NUMBER\",\n      \"fieldValue\": \"6456\",\n  "+
  "    \"fieldChanged\": \"Y\"\n    }\n  ],\n  \"conditionFieldsList\": []\n}";


  @Test
  public void OracleConnection() {
    System.out.println("OracleConnection:BEGIN");
    String oraUser="inn_poc1";
    String oraPwd="inn_poc1";
    String oraHostname="52.59.141.49";
    String oraPort="1522";
    String oraDbname="DB2";
    String ConnectionURl= "jdbc:oracle:thin:@//" +
      oraHostname + ":" +
      oraPort + "/" +
      oraDbname;
    try {
      OracleDataSource oraDataSource=new OracleDataSource();
      oraDataSource.setUser(oraUser);
      oraDataSource.setPassword(oraPwd);
      oraDataSource.setURL(ConnectionURl);

      Connection oraConnection = oraDataSource.getConnection();

      // DatabaseMetaData dbmd = connection.getMetaData();
      // System.out.println("Driver Name: " + dbmd.getDriverName());
      // System.out.println("Driver Version: " + dbmd.getDriverVersion());
      // // Print some connection properties
      // System.out.println("Default Row Prefetch Value is: " +
      //    connection.getDefaultRowPrefetch());
      // System.out.println("Database Username is: " + connection.getUserName());
      // System.out.println();
      // Statement and ResultSet are AutoCloseable and closed automatically.
      // Statement statement = connection.createStatement();
      // ResultSet resultSet = statement.executeQuery("select first_name, last_name from employees");
      // while (resultSet.next())
      //     System.out.println(resultSet.getString(1) + " "+ resultSet.getString(2) + " ");
    } catch (Exception e){
      System.out.println("SQLException error");
      e.printStackTrace();
    }
    System.out.println("OracleConnection:END");
  }

  @Test
  public void OracleInsert() {
    System.out.println("OracleInsert:BEGIN");

    System.out.println("OracleInsert:END");
  }

  @Test
  public void ConsumerTopicTest() {
    System.out.println("ConsumerTopicTest:");
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          String[] args = {"co_full_1", "co_full_contracts"};
          ConsumerDelta.main(args);
        } catch (IOException e) {
          // TODO something here
        }
      }
    });
    thread.start();
    int runFor = 10*1000;
    int sleepFor = 1000;
    long endTimeMillis = System.currentTimeMillis() + runFor;
    while (thread.isAlive()) {
      if (System.currentTimeMillis() > endTimeMillis) {
        System.out.println("end of this batch");
        break;
      }
    }
  }

  @Test
  public void validateJSONDeserializerTest() {
    System.out.println("DeserializerTest:BEGIN");

    try {
      Landing landing = new ObjectMapper().readValue(contractJSON, Landing.class);
      System.out.println(landing.toString());

      //Base Atributes
      assertEquals("1195604", landing.getCommandScn());
      assertEquals("1195604", landing.getCommandCommitScn());
      assertEquals("0", landing.getCommandSequence());
      assertEquals("INSERT", landing.getCommandType());
      assertEquals("2017-08-08 09:36:43+00:000", landing.getCommandTimestamp());
      assertEquals("DB1", landing.getObjectDBName());
      assertEquals("POC1", landing.getObjectSchemaName());
      assertEquals("CORE_CONTRACTS", landing.getObjectId());

      assertEquals(10,landing.getChangedFieldsList().size());

      ChangedFieldsList change = landing.getChangedFieldsList().get(0);
      assertEquals("COCO_ID", change.getFieldId());
      assertEquals("NUMBER", change.getFieldType());
      assertEquals("1", change.getFieldValue());
      assertEquals("Y", change.getFieldChanged());

      change = landing.getChangedFieldsList().get(1);
      assertEquals("COCO_TYPE", change.getFieldId());
      assertEquals("NUMBER", change.getFieldType());
      assertEquals("2", change.getFieldValue());
      assertEquals("Y", change.getFieldChanged());

      change = landing.getChangedFieldsList().get(2);
      assertEquals("COOC_COVERAGE", change.getFieldId());
      assertEquals("NUMBER", change.getFieldType());
      assertEquals("6771897", change.getFieldValue());
      assertEquals("Y", change.getFieldChanged());

      change = landing.getChangedFieldsList().get(3);
      assertEquals("COCO_ANNUAL_PREMIUM", change.getFieldId());
      assertEquals("NUMBER", change.getFieldType());
      assertEquals("67718", change.getFieldValue());
      assertEquals("Y", change.getFieldChanged());

      change = landing.getChangedFieldsList().get(4);
      assertEquals("COCO_START_DATE", change.getFieldId());
      assertEquals("DATE", change.getFieldType());
      assertEquals("2017-07-31 14:32:00+00:000", change.getFieldValue());
      assertEquals("Y", change.getFieldChanged());

      change = landing.getChangedFieldsList().get(5);
      assertEquals("COCO_END_DATE", change.getFieldId());
      assertEquals("DATE", change.getFieldType());
      assertEquals("2023-07-30 14:32:00+00:000", change.getFieldValue());
      assertEquals("Y", change.getFieldChanged());

      change = landing.getChangedFieldsList().get(6);
      assertEquals("COCO_COCU_ID", change.getFieldId());
      assertEquals("NUMBER", change.getFieldType());
      assertEquals("80", change.getFieldValue());
      assertEquals("Y", change.getFieldChanged());

      change = landing.getChangedFieldsList().get(7);
      assertEquals("COCO_COAG_ID", change.getFieldId());
      assertEquals("NUMBER", change.getFieldType());
      assertEquals("484", change.getFieldValue());
      assertEquals("Y", change.getFieldChanged());

      change = landing.getChangedFieldsList().get(8);
      assertEquals("COCO_TOTAL_PAID_PREMIUMS", change.getFieldId());
      assertEquals("NUMBER", change.getFieldType());
      assertEquals("135436", change.getFieldValue());
      assertEquals("Y", change.getFieldChanged());

      change = landing.getChangedFieldsList().get(9);
      assertEquals("COCO_TOTAL_PAID_CLAIMS", change.getFieldId());
      assertEquals("NUMBER", change.getFieldType());
      assertEquals("6456", change.getFieldValue());
      assertEquals("Y", change.getFieldChanged());

      assertEquals(0,landing.getConditionFieldsList().size());
    } catch (IOException e) {
      Assert.fail("exception duting the Deserialization.");
    }
    System.out.println("DeserializerTest:END");
  }

  @Test
  public void validateJSONtoAvroTest() {
    System.out.println("validateJSONtoAvroTest:BEGIN");

    try {
      Landing landing = new ObjectMapper().readValue(contractJSON, Landing.class);
      System.out.println("Schema:");
      System.out.println(ContractsSchema.SCHEMA$);
      System.out.println("Avro:");
      System.out.println(landing.toAvroCanonical().toString());

    } catch (IOException e) {
      Assert.fail("exception duting the Deserialization.");
    }
    System.out.println("validateJSONtoAvroTest:END");
  }

  // @Test
  // public void StreamProcessorTest() {
  //   System.out.println("ConsumerTopicTest:");
  //   Thread thread = new Thread(new Runnable() {
  //       @Override
  //       public void run() {
  //           try {
  //             Partners.main();
  //           } catch (IOException e) {
  //               // Do something here
  //           }
  //       }
  //   });
  //   thread.start();
  //   int runFor = 5*60*1000;
  //   int sleepFor = 1000;
  //   long endTimeMillis = System.currentTimeMillis() + runFor;
  //   while (thread.isAlive()) {
  //       if (System.currentTimeMillis() > endTimeMillis) {
  //           System.out.println("end of this batch");
  //           break;
  //       }
  //   }
  // }
  //@Before
  //@After
  //@BeforeClass
  //@AfterClass
}

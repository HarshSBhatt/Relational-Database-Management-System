package org.group15.database;

import org.group15.util.AppConstants;
import org.group15.util.Helper;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class SQLDump {

  FileWriter eventLogsWriter;

  File dumpFile;

  File dumpFolder;

  private List<Column> columns;

  Table tableObj;

  public SQLDump(FileWriter eventLogsWriter) {
    this.columns = new ArrayList<>();
    this.eventLogsWriter = eventLogsWriter;
    tableObj = new Table(eventLogsWriter);
  }

  public boolean generateDump(String schemaName) throws Exception {
    String schemaPath = Helper.getSchemaPath(schemaName);
    File schemaFolder = new File(schemaPath.concat("/table_metadata"));
    File[] tables = schemaFolder.listFiles();

    this.dumpFolder =
        new File(AppConstants.DUMP_ROOT_FOLDER_PATH);

    if (this.dumpFolder.mkdir()) {
      System.out.println("DUMP folder created!");
    }

    // Dump file for particular schema
    String tableFilePath =
        AppConstants.DUMP_ROOT_FOLDER_PATH + "/" + schemaName + ".dp15";
    this.dumpFile =
        new File(tableFilePath);

    if (this.dumpFile.createNewFile()) {
      System.out.println("DUMP file created!");
    }

    if (!schemaFolder.exists()) {
      throw new Exception("Database with name: " + schemaName + " not found");
    }

    if (tables.length < 1) {
      throw new Exception("No tables exist in database with name: " + schemaName + " not found");
    }
    // Looping through each table inside particular folder
    for (File table : tables) {
      String tableName = table.getName();
      String tablePath = table.getPath();
      // We will read the particular table based on the table path
      File tableFile = new File(tablePath);
      System.out.println(tablePath);
      BufferedReader br =
          new BufferedReader(new FileReader(tableFile));

      // Here, we are reading all data from particular table
      String line;
      while ((line = br.readLine()) != null) {
        // Generating column obj from the line
        Column column = tableObj.getColumnObjFromLine(line);

        this.eventLogsWriter.append("Metadata fetched successfully of " +
            "table: ").append(tableName).append(" while generating DUMP").append("\n");
        this.columns.add(column);
      }
      // TODO
      // Generate Dump here
    }
    this.eventLogsWriter.append("All tables detail fetched successfully to " +
        "generate DUMP").append("\n");
    return true;
  }

}

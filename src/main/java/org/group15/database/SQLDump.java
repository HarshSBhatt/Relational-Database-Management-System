package org.group15.database;

import org.group15.util.AppConstants;
import org.group15.util.Helper;

import java.io.*;
import java.util.*;

public class SQLDump {

  FileWriter eventLogsWriter;

  File dumpFile;

  File dumpFolder;

  Formatter fmtCon;

  Formatter fmtFile;

  private List<Column> columns;

  Table tableObj;

  public SQLDump(FileWriter eventLogsWriter) {
    this.columns = new ArrayList<>();
    this.eventLogsWriter = eventLogsWriter;
    tableObj = new Table(eventLogsWriter);
    this.fmtCon = new Formatter(System.out);
  }

  public boolean generateDump(String schemaName) throws Exception {
    String schemaPath = Helper.getSchemaPath(schemaName);
    File schemaFolder = new File(schemaPath.concat("/table_metadata"));
    File[] tables = schemaFolder.listFiles();

    if (!schemaFolder.exists()) {
      throw new Exception("Database with name: " + schemaName + " not found");
    }

    if (tables.length < 1) {
      throw new Exception("No tables exist in database with name: " + schemaName);
    }

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

    // Here, we will not append erd, but replace content of existing file  if file is not empty
    this.fmtFile = new Formatter(new FileOutputStream(tableFilePath));

    // Looping through each table inside particular folder
    for (File table : tables) {
      String tableName = table.getName();
      String tablePath = table.getPath();
      // We will read the particular table based on the table path
      File tableFile = new File(tablePath);

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
      writeDumpToFile(schemaName, tableName, tablePath);
    }
    System.out.println("SQL Dump created successfully");
    this.fmtCon.close();
    this.fmtFile.close();
    this.eventLogsWriter.append("All tables detail fetched successfully to " +
        "generate DUMP").append("\n");
    return true;
  }

  private void writeDumpToFile(String schemaName, String tableName,
                               String tablePath) throws IOException {

    Map<String, Object> columnAndValueArray = new HashMap<>();

    String sName = "Schema: ".concat(schemaName);
    String tName = "Table: ".concat(tableName);
    String tPath = "Path: ".concat(tablePath);

    String lineSeparator =
        "=============================================================================================================================================";

    String headingSeparator =
        "---------------------------------------------------------------------------------------------------------------------------------------------";

    this.fmtFile.format(lineSeparator.concat("\n"));

    this.fmtFile.format("%20s%30s%60s\n", sName, tName, tPath);

    this.fmtFile.format(lineSeparator.concat("\n"));

    int i = 0;
    StringBuilder dumpContent = new StringBuilder("Columns: (");

    for (Column column : this.columns) {
      if (i == this.columns.size() - 1) {
        dumpContent.append(column.getColumnName()).append(")\n");
      } else {
        dumpContent.append(column.getColumnName()).append(",");
      }
      i++;
      ArrayList<Object> columnValues =
          tableObj.getValuesOfParticularColumn(schemaName,
              tableName.split("\\.")[0], column);
      columnAndValueArray.put(column.getColumnName(), columnValues);
    }

    i = 0;
    dumpContent.append("Data types: (");
    for (Column column : this.columns) {
      if (i == this.columns.size() - 1) {
        dumpContent.append(column.getColumnDataType()).append(")\n");
      } else {
        dumpContent.append(column.getColumnDataType()).append(",");
      }
      i++;
    }

    this.fmtFile.format(String.valueOf(dumpContent).concat("\n"));
    this.fmtFile.format(headingSeparator.concat("\n\n"));

    for (String key : columnAndValueArray.keySet()) {
      this.fmtFile.format(key.concat(": ").concat(String.valueOf(columnAndValueArray.get(key))).concat(
          "\n"));
    }
    this.fmtFile.format(headingSeparator.concat("\n"));

    // Emptied the current column data for the next iteration
    this.columns = new ArrayList<>();
  }

}

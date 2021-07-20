package org.group15.sql;

import org.group15.database.Schema;
import org.group15.io.SchemaIO;
import org.group15.io.TableIO;
import org.group15.util.Helper;

import java.util.HashMap;
import java.util.HashSet;

public class Create {

  SchemaIO schemaIO;

  TableIO tableIO;


  public Create() {
    this.schemaIO = new SchemaIO();
    this.tableIO = new TableIO();
  }

  public boolean parseCreateSchemaStatement(int size, String[] queryParts) {
    boolean isValidSyntax = false;
    if (size == 3 && queryParts[1].equalsIgnoreCase("SCHEMA")) {
      String schemaName = queryParts[2].toLowerCase();
      isValidSyntax = schemaIO.create(schemaName);
    } else {
      System.out.println("Syntax error: Please check your syntax for Create Schema");
    }
    return isValidSyntax;
  }

  public boolean parseCreateTableStatement(int size, String[] queryParts, String schemaName) {
    boolean isValidSyntax = false;
    if (size >= 1 && queryParts[0].equalsIgnoreCase("create_table")) {
      String tableName = queryParts[1].toLowerCase();
      StringBuilder tableQuery = new StringBuilder();
      for (String str : queryParts) {
        tableQuery.append(str).append(" ");
      }
      int start = tableQuery.indexOf("(");
      int end = tableQuery.lastIndexOf(")");
      String columnNames = tableQuery.substring(start + 1, end);
      String[] columns = columnNames.split(",");
      HashMap<String, String> columnDataTypeMapping = new HashMap<>();
      HashSet<String> dt = Helper.getDataTypes();
      for (String column : columns) {
        String[] nameAndDataType = column.split(" ");
        if (dt.contains(nameAndDataType[1].toUpperCase())) {
          columnDataTypeMapping.put(nameAndDataType[0], nameAndDataType[1]);
        } else {
          System.out.println("Data type not supported");
          break;
        }
      }
      System.out.println(columnDataTypeMapping);
      System.out.println(columnNames);
      isValidSyntax = tableIO.create(schemaName, tableName);
    } else {
      System.out.println("Syntax error: Please check your syntax for Create Table");
    }
    return isValidSyntax;
  }

}

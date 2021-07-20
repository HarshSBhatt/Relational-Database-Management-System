package org.group15.parser;

import org.group15.database.Schema;
import org.group15.io.SchemaIO;
import org.group15.io.TableIO;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;

public class SyntaxParser {

  SchemaIO schemaIO;

  TableIO tableIO;

  Schema schema;

  public SyntaxParser() {
    this.schemaIO = new SchemaIO();
    this.tableIO = new TableIO();
  }

  // USE schema
  public boolean parseUseSchemaStatement(int size, String[] queryParts) {
    if (size == 2) {
      String schemaName = queryParts[1].toLowerCase();
      if (schemaIO.isExist(schemaName)) {
        System.out.println(schemaName + " selected");
        return true;
      } else {
        System.out.println("Schema: " + schemaName + " does not exist");
      }
    } else {
      System.out.println("Syntax error: Please check your syntax for USE Schema");
    }
    return false;
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

  public enum DataType {
    VARCHAR,
    INT
  }

  public void parseShowSchemaStatement(int size, String[] queryParts) {
    if (size == 2 && queryParts[1].equalsIgnoreCase("DATABASES")) {
      schemaIO.list();
    } else {
      System.out.println("Syntax error: Please check your syntax for SHOW Schema");
    }
  }

  public static HashSet<String> getDataTypes() {

    HashSet<String> values = new HashSet<>();

    for (DataType dt : DataType.values()) {
      values.add(dt.name());
    }

    return values;
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
      HashSet<String> dt = getDataTypes();
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

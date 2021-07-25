package org.group15.sql;

import org.group15.database.Table;
import org.group15.io.SchemaIO;
import org.group15.io.TableIO;

import java.io.FileWriter;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Select {

  SchemaIO schemaIO;

  TableIO tableIO;

  FileWriter eventLogsWriter;

  Table table;

  public Select(FileWriter eventLogsWriter) {
    this.schemaIO = new SchemaIO();
    this.tableIO = new TableIO();
    this.eventLogsWriter = eventLogsWriter;
    table = new Table(eventLogsWriter);
  }

  public boolean parseUseSchemaStatement(int size, String[] queryParts) throws IOException {
    if (size == 2) {
      String schemaName = queryParts[1].toLowerCase();
      if (schemaIO.isExist(schemaName)) {
        System.out.println(schemaName + " selected");
        this.eventLogsWriter.append("Schema: ").append(schemaName).append(" " +
            "selected").append("\n");
        return true;
      } else {
        this.eventLogsWriter.append("Schema: ").append(schemaName).append(" does " +
            "not exist").append("\n");
        System.out.println("Schema: " + schemaName + " does not exist");
      }
    } else {
      this.eventLogsWriter.append("Syntax error: Please check your syntax for USE Schema").append("\n");
      System.out.println("Syntax error: Please check your syntax for USE Schema");
    }
    return false;
  }

  public boolean parseSelectStatement(String query, String schemaName) throws Exception {
    Pattern nonConditionalPattern = Pattern.compile("select\\s+(.*?)" +
            "\\s*from\\s+(.*?)$",
        Pattern.CASE_INSENSITIVE);
    Pattern conditionalPattern = Pattern.compile("select\\s+(.*?)\\s*from\\s+" +
            "(.*?)\\s*where\\s+(.*?)$",
        Pattern.CASE_INSENSITIVE);

    Matcher nonConditionalMatcher = nonConditionalPattern.matcher(query);
    Matcher conditionalMatcher = conditionalPattern.matcher(query);

    if (query.toUpperCase().contains("WHERE")) {
      if (conditionalMatcher.find()) {
        String columns = conditionalMatcher.group(1).trim();
        String tableName = conditionalMatcher.group(2).trim();
        String conditions = conditionalMatcher.group(3).trim();

        table.fetchTableInfo(columns, schemaName, tableName, conditions);
      }
    } else {
      if (nonConditionalMatcher.find()) {
        String columns = nonConditionalMatcher.group(1).trim();
        String tableName = nonConditionalMatcher.group(2).trim();

        table.fetchTableInfo(columns, schemaName, tableName);
      }
    }

    return true;
  }

}

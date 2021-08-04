package org.group15.sql;

import org.group15.database.Table;

import java.io.FileWriter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Delete {

  FileWriter eventLogsWriter;

  Table table;

  boolean isTransaction;

  boolean isBulkOperation;

  public Delete(FileWriter eventLogsWriter, boolean isTransaction, boolean isBulkOperation) {
    this.eventLogsWriter = eventLogsWriter;
    this.isTransaction = isTransaction;
    this.isBulkOperation = isBulkOperation;
    table = new Table(eventLogsWriter);
  }

  public boolean parseDeleteStatement(String query, String schemaName) throws Exception {
    Pattern deletePattern = Pattern.compile("delete\\s+from\\s+" +
            "(.*?)\\s*where\\s+(.*?)$",
        Pattern.CASE_INSENSITIVE);

    Matcher patternMatcher = deletePattern.matcher(query);

    if (patternMatcher.find()) {
      String tableName = patternMatcher.group(1).trim();
      String condition = patternMatcher.group(2).trim();

      String[] conditionString = condition.split("\\s*=\\s*");

      if (!isTransaction) {
        return table.delete(schemaName, tableName, conditionString, isBulkOperation);
      }
      return true;
    } else {
      return false;
    }
  }

}

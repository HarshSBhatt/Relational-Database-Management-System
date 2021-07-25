package org.group15.sql;

import org.group15.database.Table;

import java.io.FileWriter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Delete {

  FileWriter eventLogsWriter;

  Table table;

  public Delete(FileWriter eventLogsWriter) {
    this.eventLogsWriter = eventLogsWriter;
    table = new Table(eventLogsWriter);
  }

  public boolean parseDeleteStatement(String query, String schemaName) throws Exception {
    String[] queryParts = query.split("\\s+");
    Pattern deletePattern = Pattern.compile("delete\\s+from\\s+" +
            "(.*?)\\s*where\\s+(.*?)$",
        Pattern.CASE_INSENSITIVE);

    Matcher patternMatcher = deletePattern.matcher(query);

    if (patternMatcher.find()) {
      String tableName = patternMatcher.group(1).trim();
      String condition = patternMatcher.group(2).trim();

      String[] conditionString = condition.split("\\s*=\\s*");

      return table.delete(schemaName, tableName, conditionString);
    } else {
      return false;
    }
  }

}

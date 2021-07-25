package org.group15.sql;

import java.io.FileWriter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Delete {

  FileWriter eventLogsWriter;

  public Delete(FileWriter eventLogsWriter) {
    this.eventLogsWriter = eventLogsWriter;
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
      System.out.println(tableName);
      System.out.println(condition);
    } else {
      return false;
    }
    return true;
  }

}

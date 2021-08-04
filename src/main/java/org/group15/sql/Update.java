package org.group15.sql;

import org.group15.database.Column;
import org.group15.database.Table;

import java.io.FileWriter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Update {

  Table table;

  FileWriter eventLogsWriter;

  Column primaryKeyColumn;

  Column foreignKeyColumn;

  boolean isTransaction;

  boolean isBulkOperation;

  public Update(FileWriter eventLogsWriter, boolean isTransaction, boolean isBulkOperation) {
    this.eventLogsWriter = eventLogsWriter;
    this.isTransaction = isTransaction;
    this.isBulkOperation = isBulkOperation;
    table = new Table(eventLogsWriter);
    this.primaryKeyColumn = null;
    this.foreignKeyColumn = null;
  }

  /**
   * Assumptions: No whitespace between column names and values of columns
   * For example: update rajz set first_name='raj',last_name='valand' where user_id=1
   * This way if we split query on white space, we will get exact 6 parts
   * For example: [updae , rajz, set, first_name='raj',lastname='valand' , where, user_id=1]
   */

  public boolean parseUpdateTableStatement(String query, String schemaName) throws Exception {

    // update users set first_name='Harsh' where user_id=2
    Pattern updatePattern = Pattern.compile("update\\s+(.*?)\\s*set\\s+" +
            "(.*?)\\s*where\\s+(.*?)$",
        Pattern.CASE_INSENSITIVE);

    Matcher patternMatcher = updatePattern.matcher(query);

    if (patternMatcher.find()) {
      String tableName = patternMatcher.group(1).trim();
      String columns = patternMatcher.group(2).trim();
      String condition = patternMatcher.group(3).trim();

      String[] conditionString = condition.split("\\s*=\\s*");

      if (!isTransaction) {
        return table.update(schemaName, tableName, columns, conditionString,
            isBulkOperation);
      }
      return true;
    } else {
      return false;
    }
  }

}

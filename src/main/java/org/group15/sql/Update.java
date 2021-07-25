package org.group15.sql;

import org.group15.database.Column;
import org.group15.database.Table;
import org.group15.io.TableIO;
import org.group15.util.AppConstants;
import org.group15.util.Helper;

import java.io.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


// String updateTableQuery = "update users set last_name='raj',first_name='valand' where user_id=1";
public class Update {

    private TableIO tableIO;


    Table table;
    private Map<String, Column> columns;


    FileWriter eventLogsWriter;
    Column primaryKeyColumn;
    Column foreignKeyColumn;

    public Update(FileWriter eventLogsWriter) {

        this.eventLogsWriter = eventLogsWriter;
        table = new Table(eventLogsWriter);
        this.columns = new HashMap<>();
        this.tableIO = new TableIO();
        this.primaryKeyColumn = null;
        this.foreignKeyColumn = null;
    }


    public boolean getTableMetadata(String schemaName, String tableName) throws Exception {
        String path = Helper.getTableMetadataPath(schemaName, tableName);
        File metadataFile = new File(path);

        if (metadataFile.exists()) {
            BufferedReader br =
                    new BufferedReader(new FileReader(metadataFile));

            String line;

            while ((line = br.readLine()) != null) {
                Column column = new Column();
                String[] columnInfo = line.split(AppConstants.DELIMITER_TOKEN);

                //  Looping through file data and creating metadata of type COLUMN
                for (String info : columnInfo) {
                    String[] columnKeyValue = info.split("=");
                    String columnKey = columnKeyValue[0];
                    if (columnKey.equalsIgnoreCase(AppConstants.COLUMN_NAME)) {
                        column.setColumnName(columnKeyValue[1]);
                    } else if (columnKey.equalsIgnoreCase(AppConstants.COLUMN_DATA_TYPE)) {
                        column.setColumnDataType(columnKeyValue[1]);
                    } else if (columnKey.equalsIgnoreCase(AppConstants.COLUMN_SIZE)) {
                        column.setColumnSize(Integer.parseInt(columnKeyValue[1]));
                    } else if (columnKey.equalsIgnoreCase(AppConstants.PK)) {
                        column.setPrimaryKey(true);
                        this.primaryKeyColumn = column;
                    } else if (columnKey.equalsIgnoreCase(AppConstants.FK)) {
                        column.setForeignKey(true);
                        String[] tableAndColumn = columnKeyValue[1].split("\\.");
                        String foreignTable = tableAndColumn[0];
                        String foreignColumn = tableAndColumn[1];
                        column.setForeignKeyTable(foreignTable);
                        column.setForeignKeyColumn(foreignColumn);
                        this.foreignKeyColumn = column;
                    } else if (columnKey.equalsIgnoreCase(AppConstants.AI)) {
                        column.setAutoIncrement(true);
                    } else {
                        this.eventLogsWriter.append("Something went wrong near: ").append(columnKey).append("\n");
                        throw new Exception("Something went wrong near: " + columnKey);
                    }
                }
                this.columns.put(column.getColumnName(), column);
            }
            br.close();
            this.eventLogsWriter.append("Metadata fetched successfully for table: ").append(tableName).append("\n");
            return true;
        }

        return false;
    }

    public boolean validateColWithVal(String[] columnsArray,
                                      String[] valuesArray) throws NumberFormatException, IOException {
        for (String col : columnsArray) {
            Set<String> key = this.columns.keySet();

            if (!this.columns.containsKey(col)) {
                return false;
            }

            if (this.primaryKeyColumn != null) {
                if (!key.contains(this.primaryKeyColumn.getColumnName())) {
                    this.eventLogsWriter.append("Insert fail: Value for primary key is null").append("\n");
                    System.out.println("Insert fail: Value for primary key is null");
                    return false;
                }
            }

            if (this.foreignKeyColumn != null) {
                if (!key.contains(this.foreignKeyColumn.getColumnName())) {
                    this.eventLogsWriter.append("Insert fail: Value for foreign key is null").append("\n");
                    System.out.println("Insert fail: Value for foreign key is null");
                    return false;
                }
            }
        }
        for (int i = 0; i < columnsArray.length; i++) {
            String colName = columnsArray[i];
            String colValue = valuesArray[i];
            Column metadata = this.columns.get(colName);
            String columnDataType = metadata.getColumnDataType().toLowerCase();
            Object columnValue;
            switch (columnDataType) {
                case "int":
                    columnValue = Integer.parseInt(colValue);
                    metadata.setColumnValue(columnValue);
                    break;
                case "float":
                    columnValue = Float.parseFloat(colValue);
                    metadata.setColumnValue(columnValue);
                    break;
                default:
                    int startIndex = colValue.indexOf("'");
                    int endIndex = colValue.lastIndexOf("'");
                    if (startIndex > 0 || endIndex < colValue.length() - 1) {
                        this.eventLogsWriter.append("Something went wrong while extracting value").append("\n");
                        System.out.println("Something went wrong while extracting value");
                        return false;
                    }
                    // If value has only '', then it will have length 2, that is empty
                    if (colValue.length() == 2) {
                        if (colName.equals(this.primaryKeyColumn.getColumnName())) {
                            this.eventLogsWriter.append("Insert fail: Value for primary key" +
                                    " is null").append("\n");
                            System.out.println("Insert fail: Value for primary key is null");
                            return false;
                        }
                        if (colName.equals(this.foreignKeyColumn.getColumnName())) {
                            this.eventLogsWriter.append("Insert fail: Value for foreign key is null").append("\n");
                            System.out.println("Insert fail: Value for foreign key is null");
                            return false;
                        }
                    }
                    metadata.setColumnValue(colValue.substring(1, colValue.length() - 1));
                    break;
            }
        }
        this.eventLogsWriter.append("Columns and its value validated " +
                "successfully").append("\n");
        return true;
    }

    /**
     * Assumptions: No whitespace between column names and values of columns
     * For example: update rajz set first_name='raj',last_name='valand' where user_id=1
     * This way if we split query on white space, we will get exact 6 parts
     * For example: [updae , rajz, set, first_name='raj',lastname='valand' , where, user_id=1]
     */

    public boolean parseUpdateTableStatement(String query, String schemaName) throws Exception {
        boolean isValidSyntax = false;
        String[] queryParts = query.split("\\s+");

        if (queryParts.length != 6) {
            this.eventLogsWriter.append("Syntax error: Error while parsing update query").append("\n");
            throw new Exception("Syntax error: Error while parsing update query");
        }

        if (!queryParts[2].equalsIgnoreCase("SET")) {
            this.eventLogsWriter.append("Syntax error: SET keyword not found " +
                    "in Update query").append("\n");
            throw new Exception("Syntax error: SET keyword not found in " +
                    "Update query");
        }

        if (!queryParts[4].equalsIgnoreCase("WHERE")) {
            this.eventLogsWriter.append("Syntax error: WHERE keyword not found" +
                    " in Update query").append("\n");
            throw new Exception("Syntax error: WHERE keyword not found in " +
                    "Update query");
        }

        String tableName = queryParts[1];
        String queryColumns = "";
        String queryValues = "";

        String queryColumnsAndValues = queryParts[3];
//    System.out.println(queryColumnsAndValues);

        String[] testing = queryColumnsAndValues.split(",");
        for (int i = 0; i < testing.length; i++) {
            Integer j = 0;
            String[] columnAndValue = testing[i].split("=");
            if (i == testing.length - 1) {
                queryColumns += columnAndValue[j];
                queryValues += columnAndValue[j + 1];
            } else {
                queryColumns += columnAndValue[j] + ",";
                queryValues += columnAndValue[j + 1] + ",";
            }

        }
//        System.out.println(queryColumns);
//        System.out.println(queryValues);

        if (tableIO.isTableExist(schemaName, tableName)) {
            String[] columnsArray =
                    queryColumns.split(",");
            String[] valuesArray =
                    queryValues.split(",");
//            for (int i = 0; i < columnsArray.length; i++) {
//                System.out.println(columnsArray[i]);
//            }
//            for (int i = 0; i < valuesArray.length; i++) {
//                System.out.println(valuesArray[i]);
//            }
            if (columnsArray.length == valuesArray.length) {
                boolean isTableDataLoaded = getTableMetadata(schemaName, tableName);
                if (isTableDataLoaded) {
                    // Checking if values has correct type and constraint related to table
                    boolean isColAndValCorrectlyMapped =
                            validateColWithVal(columnsArray, valuesArray);
                    if (isColAndValCorrectlyMapped) {
                        Set<String> key = this.columns.keySet();
                        for (String str : key) {
//                            System.out.println(this.columns.get(str));
                            Column col = this.columns.get(str);
//                            System.out.println(col.getColumnName() + " // ");
//                            System.out.println(col.getColumnValue());

                        }
                        if (isColAndValCorrectlyMapped) {
                            // If it is true, then we will insert data to the file/table
                            boolean isDataWritten = table.update(schemaName, tableName,
                                    columnsArray, this.columns, this.primaryKeyColumn, this.foreignKeyColumn);

                        }
                    }
                } else {
                    this.eventLogsWriter.append("Error: Something went wrong while " +
                            "fetching table data").append("\n");
                    throw new Exception("Error: Something went wrong while " +
                            "fetching table data");
                }

            } else {
                this.eventLogsWriter.append("Error: Columns and its value " +
                        "mismatch").append("\n");
                throw new Exception("Error: Columns and its value mismatch");
            }

            // If column and value length is different, then e will throw error


        }
        return true;
    }


}


// String updateTableQuery = "update users set last_name='raj',first_name='valand' where user_id=1";

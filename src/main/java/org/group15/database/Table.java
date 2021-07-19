package org.group15.database;

import org.group15.io.TableIO;

public class Table {

    private String tableName;
    TableIO tableIO = new TableIO();

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void create(String schemaName, String tableName) {
        tableIO.create(schemaName, tableName);
    }

    public void isExist(String schemaName, String tableName) {
        if (tableIO.isExist(schemaName, tableName)) {
            System.out.println("Table exists");
        } else {
            System.out.println("Table does not exist");
        }
    }
}

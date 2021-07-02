package org.group15.parser;

import org.group15.database.Schema;

public class QueryParser {
    Schema schema = new Schema();

    public void parse(String query) {
        String[] list = query.split(" ");
        String dbOperation = list[0].toUpperCase();


        switch (dbOperation) {
            case "CREATE":
                String schemaName = list[2].toLowerCase();
                schema.create(schemaName);
                break;
            case "USE":
                String schemaname = list[1].toLowerCase();
                int size = list.length;
                schema.isUseExist(schemaname , size);
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + dbOperation);
        }
    }
}

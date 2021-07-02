package org.group15.io;

import org.group15.util.AppConstants;
import org.group15.util.Helper;

import java.io.File;

public class SchemaIO {
    public boolean create(String schemaName) {
        try {
            String path = Helper.getSchemaPath(schemaName);
            File directory = new File(path);
            if (directory.exists()) {
                System.out.println("Schema already exists");
                return false;
            } else {
                directory.mkdirs();
                File tables = new File(Helper.getSchemaPath(schemaName + "/tables"));
                File relations = new File(Helper.getSchemaPath(schemaName + "/relations"));
                tables.mkdir();
                relations.mkdir();
                System.out.println("Schema created");
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public void list() {
        File schemaPath = new File(AppConstants.ROOT_FOLDER_PATH);
        String[] schemas = schemaPath.list();
        if (schemas == null) {
            System.out.println("No schemas found! create one");
        } else {
            for (String schema : schemas) {
                System.out.println(schema);
            }
        }
    }

    public boolean isExist(String schemaName) {
        String path = Helper.getSchemaPath(schemaName);
        File directory = new File(path);
        return directory.exists();
    }
}

package com.infoworks;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

class SpannerRead {

    static void createTable() throws SQLException {
        // TODO(developer): Replace these variables before running the sample.
        String projectId = "gcp-cs-shared-resources";
        String instanceId = "google-spanner-csv-poc";
        String databaseId = "finance-db";
        createTable(projectId, instanceId, databaseId);
    }

    static void createTable(String projectId, String instanceId, String databaseId)
            throws SQLException {

        String connectionUrl =
                String.format(
                        "jdbc:cloudspanner:/projects/%s/instances/%s/databases/%s",
                        projectId, instanceId, databaseId);
        try
        {
            Class.forName("com.google.cloud.spanner.jdbc.JdbcDriver");
        }
        catch (Exception ex)
        {
            System.err.println("Driver not found");
        }
        connectionUrl = connectionUrl + "?credentials=svc_account.json;autocommit=true";
        System.out.println("Trying to connect to "+connectionUrl);
        try (Connection connection = DriverManager.getConnection(connectionUrl)) {
            try (Statement statement = connection.createStatement()) {
//                statement.execute(
//                        "CREATE TABLE CSV_DEMO_TEST_COLS (\n"
//                                + "  COL1   INT64,\n"
//                                + "  COL2   FLOAT64,\n"
//                                + "  COL3   FLOAT64,\n"
//                                + "  COL5   NUMERIC,\n"
//                                + "  COL6  STRING(MAX),\n"
//                                + "  COL7   BYTES(MAX),\n"
//                                + ") PRIMARY KEY (COL1)\n");

                String dropTableQuery = String.format("DROP TABLE %s ","CSV_DEMO_AUTOMATIC_1");
                String dropindex = String.format("DROP INDEX %s ","CSV_DEMO_AUTOMATIC_1Index");
                try {
                    statement.execute(dropindex);
                    statement.execute(dropTableQuery);
                } catch (Exception  exp)
                {
                    exp.printStackTrace();
                }

            }
        }
        System.out.println("Created table [Singers]");
    }

    public static void main(String[] args) throws SQLException {
        createTable();
    }

}


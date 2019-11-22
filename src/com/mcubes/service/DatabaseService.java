/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mcubes.service;

import com.mcubes.bean.CrudOperation;
import com.mcubes.connection.DatabaseConnection;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author A.A.MAMUN
 */
public class DatabaseService {

    private static boolean TRUE_FALSE;
    private static DatabaseService service = null;
    private static Connection connection;
    private static Statement statement;
    private static ResultSet resultSet;

    private DatabaseService(DatabaseConnection databaseConnection) {
        try {
            connection = databaseConnection.getConnection();
            statement = connection.createStatement();
        } catch (SQLException ex) {
            Logger.getLogger(DatabaseService.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static DatabaseService getInstanceDatabaseService(DatabaseConnection databaseConnection) {
        if (service == null) {
            service = new DatabaseService(databaseConnection);
        }
        return service;
    }

    public Connection getConnection() {
        return connection;
    }

    public Statement getStatement() {
        return statement;
    }

    public boolean createNativeQuery(String sql) throws SQLException {
        return statement.execute(sql);
    }

    public ResultSet executeNativeQuery(String sql) throws SQLException {
        resultSet = statement.executeQuery(sql);
        return resultSet;
    }

    public Object nativeQuery(String sql) throws SQLException {
        String sql2 = sql.trim().toLowerCase();
        if (sql2.startsWith("update") || sql2.startsWith("delete") || sql2.startsWith("drop")) {
            return statement.execute(sql);
        } else {
            List<Map<String, Object>> dataList = new ArrayList<>();
            resultSet = statement.executeQuery(sql);
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int columnCount = resultSetMetaData.getColumnCount();
            while (resultSet.next()) {
                Map<String, Object> map = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    String className = resultSetMetaData.getColumnClassName(i);
                    String columnName = resultSetMetaData.getColumnName(i);
                    map.put(columnName, resultSet.getObject(columnName));
                }
                dataList.add(map);
            }
            return dataList;
        }
    }
}

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mcubes.connection;

import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author A.A.MAMUN
 */
public class DatabaseConnection {

    public static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
    public static final String DERBY_DRIVER = "org.apache.derby.jdbc.ClientDriver";
    public static final String POSTGRE_DRIVER = "org.postgresql.Driver";
    //public static final String ORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver";
    //public static final String MICROSOFT_SQL_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";

    private static boolean isDBConnected = false;
    private static String driver;
    private static Connection connection = null;
    private static Statement statement;
    private static DatabaseConnection databaseConnection = null;

    private DatabaseConnection(String driver, String url) {
        try {
            Class.forName(driver);
            DatabaseConnection.driver = driver;
            connection = DriverManager.getConnection(url);
            statement = connection.createStatement();
            isDBConnected = true;
        } catch (ClassNotFoundException | SQLException ex) {
            isDBConnected = false;
            ex.printStackTrace();
        }
    }

    private DatabaseConnection(String driver, String url, String user, String password) {
        try {
            Class.forName(driver);
            DatabaseConnection.driver = driver;
            connection = DriverManager.getConnection(url, user, password);
            statement = connection.createStatement();
            isDBConnected = true;
        } catch (ClassNotFoundException | SQLException ex) {
            isDBConnected = false;
            ex.printStackTrace();
        }
    }

    
    public static DatabaseConnection getInstanceConnection(String driver, String url) {
        if (connection == null) {
            databaseConnection = new DatabaseConnection(driver, url);
        }
        mCubes();
        return databaseConnection;
    }
    
    private static void mCubes(){
        
         System.out.println("           ___          _            ___ \n" +
"  _ __    / __|  _  _  | |__   ___  / __|\n" +
" | '  \\  | (__  | || | | '_ \\ / -_) \\__ \\\n" +
" |_|_|_|  \\___|  \\_,_| |_.__/ \\___| |___/");
         System.out.println("***DAML library from mCubeS :::::: Copyright \u00a9 2019***");
         System.out.println("***Database connection is successfully established.***");
         System.out.println("------------------------------------------------------");
         System.out.println("------------------------------------------------------\n");
       
    }

    public boolean isDatabaseConnected() {
        return isDBConnected;
    }

    public static DatabaseConnection getInstanceConnection(String driver, String url, String user, String password) {
        if (databaseConnection == null) {
            databaseConnection = new DatabaseConnection(driver, url, user, password);
        }
        return databaseConnection;
    }

    public static String getDriver() {
        return driver;
    }

    public Connection getConnection() {
        return connection;
    }

    public Statement getStatement() {
        return statement;
    }

    public void close() {
        try {
            connection.close();
            databaseConnection = null;
        } catch (SQLException ex) {
            Logger.getLogger(DatabaseConnection.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static class DatabaseConnectionBuilder {

        private String driverClassName;
        private String url;
        private String user;
        private String password;

        public DatabaseConnectionBuilder setDriverClassName(String className) {
            this.driverClassName = className;
            return this;
        }
        
        public DatabaseConnectionBuilder setUrl(String url){
            this.url = url;
            return this;
        }
        
         public DatabaseConnectionBuilder setUser(String user){
            this.user = user;
            return this;
        }
         
         
        public DatabaseConnectionBuilder setPassword(String password){
            this.password = password;
            return this;
        }
        
        public DatabaseConnection build(){
            return DatabaseConnection.getInstanceConnection(this.driverClassName
                    , this.url
                    , this.user
                    , this.password);
        }

    }

}

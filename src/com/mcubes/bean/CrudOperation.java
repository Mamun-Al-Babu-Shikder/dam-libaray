/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mcubes.bean;

/**
 *
 * @author A.A.MAMUN
 */
import com.mcubes.anotations.Column;
import com.mcubes.anotations.NotColumn;
import com.mcubes.anotations.PrimaryKey;
import com.mcubes.anotations.Table;
import com.mcubes.connection.DatabaseConnection;
import com.mcubes.exception.ColumnNotFoundException;
import com.mcubes.exception.InvalidTypeException;
import com.mcubes.exception.MultiplePrimaryKeyException;
import com.mcubes.exception.PrimaryKeyNotFoundException;
import com.mcubes.exception.TableNotFoundException;
import com.mcubes.query.DeleteQuery;
import com.mcubes.query.SelectQuery;
import com.mcubes.query.UpdateQuery;
import com.mcubes.service.DatabaseService;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CrudOperation<T> {

    private static boolean TRUE_FALSE;
    private final DatabaseService databaseService;
    private ResultSet resultSet;
    private final static String driver;

    private final List<T> iterableList = new ArrayList<>();
    private List<MetaData> metaDataList = new ArrayList<>();

    private static final Map<String, String> dataTypeConverter = new HashMap<>();
    private static final Map<String, Integer> dataTypeDefaultSize = new HashMap<>();

    static {
        driver = DatabaseConnection.getDriver();

        dataTypeConverter.clear();
        dataTypeDefaultSize.clear();

        switch (driver) {

            case DatabaseConnection.DERBY_DRIVER:
                /*
                 @ Convert java to sql data type
                 */
                dataTypeConverter.put("boolean", "boolean");
                dataTypeConverter.put("java.lang.Boolean", "boolean");
                dataTypeConverter.put("char", "char");
                dataTypeConverter.put("java.lang.Character", "char");
                dataTypeConverter.put("short", "SMALLINT");
                dataTypeConverter.put("java.lang.Short", "SMALLINT");
                dataTypeConverter.put("int", "int");
                dataTypeConverter.put("java.lang.Integer", "int");
                dataTypeConverter.put("long", "numeric");
                dataTypeConverter.put("java.lang.Long", "numeric");
                dataTypeConverter.put("float", "float");
                dataTypeConverter.put("java.lang.Float", "float");
                dataTypeConverter.put("double", "float");
                dataTypeConverter.put("java.lang.Double", "float");
                dataTypeConverter.put("byte", "numeric");
                dataTypeConverter.put("java.lang.Byte", "numeric");
                dataTypeConverter.put("byte[]", "blob");
                dataTypeConverter.put("java.math.BigDecimal", "numeric");
                dataTypeConverter.put("java.lang.String", "varchar");
                dataTypeConverter.put("java.sql.Time", "time");
                dataTypeConverter.put("java.sql.Date", "date");
                dataTypeConverter.put("java.sql.Timestamp", "timestamp");
                dataTypeConverter.put("java.sql.Blob", "blob");
                dataTypeConverter.put("java.io.InputStream", "blob");

                /*
                 @ Define default size
                 */
                dataTypeDefaultSize.put("boolean", -1);
                dataTypeDefaultSize.put("java.lang.Boolean", -1);
                dataTypeDefaultSize.put("char", 1);
                dataTypeDefaultSize.put("java.lang.Character", 1);
                dataTypeDefaultSize.put("short", 0);
                dataTypeDefaultSize.put("java.lang.Short", 0);
                dataTypeDefaultSize.put("int", -1);
                dataTypeDefaultSize.put("java.lang.Integer", -1);
                dataTypeDefaultSize.put("long", 20);
                dataTypeDefaultSize.put("java.lang.Long", 20);
                dataTypeDefaultSize.put("float", 11);
                dataTypeDefaultSize.put("java.lang.Float", 11);
                dataTypeDefaultSize.put("double", 0);
                dataTypeDefaultSize.put("java.lang.Double", 0);
                dataTypeDefaultSize.put("byte", 2);
                dataTypeDefaultSize.put("java.lang.Byte", 2);
                dataTypeDefaultSize.put("byte[]", 10);
                dataTypeDefaultSize.put("java.math.BigDecimal", 20);
                dataTypeDefaultSize.put("java.lang.String", 255);
                dataTypeDefaultSize.put("java.sql.Timestamp", -1);
                dataTypeDefaultSize.put("java.sql.Blob", 65535);
                dataTypeDefaultSize.put("java.io.InputStream", 65535);
                break;

            case DatabaseConnection.MYSQL_DRIVER:
                /*
                 @ Convert java to sql data type
                 */
                dataTypeConverter.put("boolean", "boolean");
                dataTypeConverter.put("java.lang.Boolean", "boolean");
                dataTypeConverter.put("char", "char");
                dataTypeConverter.put("java.lang.Character", "char");
                dataTypeConverter.put("short", "SMALLINT");
                dataTypeConverter.put("java.lang.Short", "SMALLINT");
                dataTypeConverter.put("int", "int");
                dataTypeConverter.put("java.lang.Integer", "int");
                dataTypeConverter.put("long", "bigint");
                dataTypeConverter.put("java.lang.Long", "bigint");
                dataTypeConverter.put("float", "float");
                dataTypeConverter.put("java.lang.Float", "float");
                dataTypeConverter.put("double", "double");
                dataTypeConverter.put("java.lang.Double", "double");
                dataTypeConverter.put("byte", "tinyint");
                dataTypeConverter.put("java.lang.Byte", "tinyint");
                dataTypeConverter.put("byte[]", "blob");
                dataTypeConverter.put("java.math.BigDecimal", "decimal");
                dataTypeConverter.put("java.lang.String", "varchar");
                dataTypeConverter.put("java.sql.Time", "time");
                dataTypeConverter.put("java.sql.Date", "date");
                dataTypeConverter.put("java.sql.Timestamp", "timestamp");
                dataTypeConverter.put("java.sql.Blob", "blob");
                dataTypeConverter.put("java.io.InputStream", "blob");

                /*
                 @ Define default size
                 */
                dataTypeDefaultSize.put("boolean", -1);
                dataTypeDefaultSize.put("java.lang.Boolean", -1);
                dataTypeDefaultSize.put("char", 1);
                dataTypeDefaultSize.put("java.lang.Character", 1);
                dataTypeDefaultSize.put("short", 0);
                dataTypeDefaultSize.put("java.lang.Short", 0);
                dataTypeDefaultSize.put("int", 11);
                dataTypeDefaultSize.put("java.lang.Integer", 11);
                dataTypeDefaultSize.put("long", 20);
                dataTypeDefaultSize.put("java.lang.Long", 20);
                dataTypeDefaultSize.put("float", 11);
                dataTypeDefaultSize.put("java.lang.Float", 11);
                dataTypeDefaultSize.put("double", 0);
                dataTypeDefaultSize.put("java.lang.Double", 0);
                dataTypeDefaultSize.put("byte", 2);
                dataTypeDefaultSize.put("java.lang.Byte", 2);
                dataTypeDefaultSize.put("byte[]", 4);
                dataTypeDefaultSize.put("java.math.BigDecimal", 0);
                dataTypeDefaultSize.put("java.lang.String", 255);
                dataTypeDefaultSize.put("java.sql.Timestamp", 0);
                dataTypeDefaultSize.put("java.sql.Blob", 65535);
                dataTypeDefaultSize.put("java.io.InputStream", 65535);

                break;

            case DatabaseConnection.POSTGRE_DRIVER:

                /*
                 @ Convert java to sql data type
                 */
                dataTypeConverter.put("boolean", "boolean");
                dataTypeConverter.put("java.lang.Boolean", "boolean");
                dataTypeConverter.put("char", "char");
                dataTypeConverter.put("java.lang.Character", "char");
                dataTypeConverter.put("short", "SMALLINT");
                dataTypeConverter.put("java.lang.Short", "SMALLINT");
                dataTypeConverter.put("int", "int");
                dataTypeConverter.put("java.lang.Integer", "int");
                dataTypeConverter.put("long", "numeric");
                dataTypeConverter.put("java.lang.Long", "numeric");
                dataTypeConverter.put("float", "float");
                dataTypeConverter.put("java.lang.Float", "float");
                dataTypeConverter.put("double", "float");
                dataTypeConverter.put("java.lang.Double", "float");
                dataTypeConverter.put("byte", "numeric");
                dataTypeConverter.put("java.lang.Byte", "numeric");
                dataTypeConverter.put("byte[]", "bytea");
                dataTypeConverter.put("java.math.BigDecimal", "numeric");
                dataTypeConverter.put("java.lang.String", "varchar");
                dataTypeConverter.put("java.sql.Time", "time");
                dataTypeConverter.put("java.sql.Date", "date");
                dataTypeConverter.put("java.sql.Timestamp", "timestamp");
                dataTypeConverter.put("java.sql.Blob", "bytea");
                dataTypeConverter.put("java.io.InputStream", "bytea");

                /*
                 @ Define default size
                 */
                dataTypeDefaultSize.put("boolean", -1);
                dataTypeDefaultSize.put("java.lang.Boolean", -1);
                dataTypeDefaultSize.put("char", 1);
                dataTypeDefaultSize.put("java.lang.Character", 1);
                dataTypeDefaultSize.put("short", 0);
                dataTypeDefaultSize.put("java.lang.Short", 0);
                dataTypeDefaultSize.put("int", -1);
                dataTypeDefaultSize.put("java.lang.Integer", -1);
                dataTypeDefaultSize.put("long", 20);
                dataTypeDefaultSize.put("java.lang.Long", 20);
                dataTypeDefaultSize.put("float", 11);
                dataTypeDefaultSize.put("java.lang.Float", 11);
                dataTypeDefaultSize.put("double", 0);
                dataTypeDefaultSize.put("java.lang.Double", 0);
                dataTypeDefaultSize.put("byte", 2);
                dataTypeDefaultSize.put("java.lang.Byte", 2);
                dataTypeDefaultSize.put("byte[]", 10);
                dataTypeDefaultSize.put("java.math.BigDecimal", 20);
                dataTypeDefaultSize.put("java.lang.String", 255);
                dataTypeDefaultSize.put("java.sql.Timestamp", 0);

                break;
        }

    }

    public CrudOperation() {
        DatabaseConnection databaseConnection = DatabaseConnection.getInstanceConnection(null, null);
        databaseService = DatabaseService.getInstanceDatabaseService(databaseConnection);
    }

    public CrudOperation(DatabaseConnection databaseConnection) {
        databaseService = DatabaseService.getInstanceDatabaseService(databaseConnection);
    }

    /*
     @ Create Table Method to create Data Table to Database
     */
    public synchronized void createTable(Class<T> c) //throws PrimaryKeyNotFoundException
    {
        try {
            String tableName = getTableName(c);
            metaDataList = getMetaDataList(c);
            String colums = "";
            for (MetaData m : metaDataList) {

                colums += m.getName();
                colums += " " + m.getType();
                if (m.getLength() != 0) {
                    colums += "(" + m.getLength() + ")";
                }
                if (!m.isNullAble) {
                    colums += " NOT NULL";
                }
                if (m.isPrimaryKey) {
                    colums += " PRIMARY KEY";
                    if (m.isIsAutoIncrement()) {
                        if (driver.equals(DatabaseConnection.DERBY_DRIVER)) {
                            colums += " GENERATED ALWAYS AS IDENTITY";
                        } else if (driver.equals(DatabaseConnection.POSTGRE_DRIVER)) {
                            colums += " GENERATED BY DEFAULT AS IDENTITY";
                        } else if (driver.equals(DatabaseConnection.MYSQL_DRIVER)) {
                            colums += " AUTO_INCREMENT";
                        }
                    }
                }
                colums += ", ";
            }
            colums = colums.substring(0, colums.length() - 2);
            String sql = "CREATE TABLE " + tableName + " (" + colums + ")";
            System.out.println("SQL : " + sql);
            databaseService.createNativeQuery(sql);

        } catch (Exception ex) {
            TRUE_FALSE = false;
            ex.printStackTrace();
        }
    }

    /*
     @ Drop table method to drop Data Table
     */
    public synchronized boolean dropTable(Class<T> c) {
        TRUE_FALSE = true;
        try {
            String tableName = getTableName(c);
            String sql = "DROP TABLE " + tableName;
            System.out.println("SQL : " + sql);
            databaseService.createNativeQuery(sql);
        } catch (Exception ex) {
            TRUE_FALSE = false;
            ex.printStackTrace();
        }
        return TRUE_FALSE;
    }

    /*
     @ Get total row from data table
     */
    public synchronized long rowCount(Class<T> c) {
        long total_row = 0;
        try {
            String tableName = getTableName(c);
            String sql = "select count(*) as total_row from " + tableName;
            resultSet = databaseService.executeNativeQuery(sql);
            resultSet.next();
            total_row = resultSet.getLong("total_row");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return total_row;
    }

    /*
     @ Fetch All Table Rows, This method is use to fetche all rows of given 'Table'.
     */
    public synchronized Iterable<T> getAllRow(Class<T> c) {
        try {
            String tableName = getTableName(c);
            String sql = "SELECT * FROM " + tableName;
            getRow(c, sql, null);
        } catch (Exception ex) {
            Logger.getLogger(CrudOperation.class.getName()).log(Level.SEVERE, null, ex);
        }
        return iterableList;
    }

    /*
     @ Fetch row by primary key
     */
    public synchronized T getRowByPrimaryKey(Class<T> c, Object value) {
        try {
            String tableName = getTableName(c);
            metaDataList = getMetaDataList(c);
            for (MetaData m : metaDataList) {
                if (m.isPrimaryKey) {

                    String sql = "SELECT * FROM " + tableName + " WHERE " + m.getName() + " = ?";
                    PreparedStatement ps = databaseService.getConnection().prepareStatement(sql);

                    Field f = m.getField();
                    int index = 1;

                    if (f.getType().getName().equals("boolean")
                            || f.getType().getName().equals("java.lang.Boolean")) {
                        ps.setBoolean(index, (Boolean) value);
                    } else if (f.getType().getName().equals("short")
                            || f.getType().getName().equals("java.lang.Short")) {
                        ps.setShort(index, (Short) value);
                    } else if (f.getType().getName().equals("java.lang.String")) {
                        ps.setString(index, value.toString());
                    } else if (f.getType().getName().equals("int")
                            || f.getType().getName().equals("java.lang.Integer")) {
                        ps.setInt(index, (Integer) value);
                    } else if (f.getType().getName().equals("long")
                            || f.getType().getName().equals("java.lang.Long")) {
                        ps.setLong(index, (Long) value);
                    } else if (f.getType().getName().equals("float")
                            || f.getType().getName().equals("java.lang.Float")) {
                        ps.setFloat(index, (Float) value);
                    } else if (f.getType().getName().equals("double")
                            || f.getType().getName().equals("java.lang.Double")) {
                        ps.setDouble(index, (Double) value);
                    } else if (f.getType().getName().equals("java.math.BigDecimal")) {
                        ps.setBigDecimal(index, (BigDecimal) value);
                    } else if (f.getType().getName().equals("byte")
                            || f.getType().getName().equals("java.lang.Byte")) {
                        ps.setByte(index, (Byte) value);
                    } else if (f.getType().getName().equals("[B")) {
                        ps.setBytes(index, convertToBytes(value));
                    } else if (f.getType().getName().equals("char")
                            || f.getType().getName().equals("java.lang.Character")) {
                        ps.setString(index, value.toString());
                    } else if (f.getType().getName().equals("java.sql.Date")) {
                        ps.setDate(index, (Date) value);
                    } else if (f.getType().getName().equals("java.sql.Blob")) {
                        ps.setBlob(index, (Blob) value);
                    } else if (f.getType().getName().equals("java.io.InputStream")) {
                        ps.setBinaryStream(index, (InputStream) value);
                    } else if (f.getType().getName().equals("java.sql.Time")) {
                        ps.setTime(index, (Time) value);
                    } else if (f.getType().getName().equals("java.sql.Timestamp")) {
                        ps.setTimestamp(index, (Timestamp) value);
                    }

                    getRow(c, sql, ps.executeQuery());
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (iterableList.isEmpty()) {
            return null;
        }
        return iterableList.get(0);
    }

    /*
     @ Fetch row by column with value
     */
    public synchronized Iterable<T> getRowByColumn(Class<T> c, String columnName, Object value) {
        iterableList.clear();
        try {
            String tableName = getTableName(c);
            metaDataList = getMetaDataList(c);
            for (MetaData m : metaDataList) {

                if (m.getName().equals(columnName)) {

                    String sql = "SELECT * FROM " + tableName + " WHERE " + columnName + " = ?";
                    PreparedStatement ps = databaseService.getConnection().prepareStatement(sql);

                    Field f = m.getField();
                    int index = 1;

                    if (f.getType().getName().equals("boolean")
                            || f.getType().getName().equals("java.lang.Boolean")) {
                        ps.setBoolean(index, (Boolean) value);
                    } else if (f.getType().getName().equals("short")
                            || f.getType().getName().equals("java.lang.Short")) {
                        ps.setShort(index, (Short) value);
                    } else if (f.getType().getName().equals("java.lang.String")) {
                        ps.setString(index, value.toString());
                    } else if (f.getType().getName().equals("int")
                            || f.getType().getName().equals("java.lang.Integer")) {
                        ps.setInt(index, (Integer) value);
                    } else if (f.getType().getName().equals("long")
                            || f.getType().getName().equals("java.lang.Long")) {
                        ps.setLong(index, (Long) value);
                    } else if (f.getType().getName().equals("float")
                            || f.getType().getName().equals("java.lang.Float")) {
                        ps.setFloat(index, (Float) value);
                    } else if (f.getType().getName().equals("double")
                            || f.getType().getName().equals("java.lang.Double")) {
                        ps.setDouble(index, (Double) value);
                    } else if (f.getType().getName().equals("java.math.BigDecimal")) {
                        ps.setBigDecimal(index, (BigDecimal) value);
                    } else if (f.getType().getName().equals("byte")
                            || f.getType().getName().equals("java.lang.Byte")) {
                        ps.setByte(index, (Byte) value);
                    } else if (f.getType().getName().equals("[B")) {
                        ps.setBytes(index, convertToBytes(value));
                    } else if (f.getType().getName().equals("char")
                            || f.getType().getName().equals("java.lang.Character")) {
                        ps.setString(index, value.toString());
                    } else if (f.getType().getName().equals("java.sql.Date")) {
                        ps.setDate(index, (Date) value);
                    } else if (f.getType().getName().equals("java.sql.Blob")) {
                        ps.setBlob(index, (Blob) value);
                    } else if (f.getType().getName().equals("java.io.InputStream")) {
                        ps.setBinaryStream(index, (InputStream) value);
                    } else if (f.getType().getName().equals("java.sql.Time")) {
                        ps.setTime(index, (Time) value);
                    } else if (f.getType().getName().equals("java.sql.Timestamp")) {
                        ps.setTimestamp(index, (Timestamp) value);
                    }

                    getRow(c, sql, ps.executeQuery());
                    break;
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return iterableList;
    }

    /*
     @ Fetch row by range
     */
    public synchronized Iterable<T> getRowByRange(Class<T> c, long fromRow, int count) {
        try {
            String tableName = getTableName(c);
            String sql = "";
            if (driver.equals(DatabaseConnection.DERBY_DRIVER)) {
                sql = "SELECT * FROM " + tableName + " OFFSET " + (fromRow - 1) + " ROWS FETCH NEXT " + count + " ROWS ONLY";
            } else {
                sql = "SELECT * FROM " + tableName + " LIMIT " + count + " OFFSET " + (fromRow - 1);
            }
            getRow(c, sql, null);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return iterableList;
    }

    /*
     @ Select with paramiterized qiery
     */
    public synchronized Iterable<T> getRowByParameterQuery(Class<T> c, SelectQuery selectQuery) {

        TRUE_FALSE = true;
        try {
            PreparedStatement ps = null;
            String tableName = getTableName(c);
            String sql = "SELECT * FROM " + tableName + " WHERE";
            Map<String, Object> map = selectQuery.getQueryValueWithColumnName();
            if (!map.isEmpty()) {

                Set<String> keys = map.keySet();
                getMetaDataList(c);
                Map<String, MetaData> map2 = new HashMap<>();
                metaDataList.forEach((MetaData v) -> {
                    map2.put(v.getName(), v);
                });

                for (String key : keys) {
                    MetaData m = map2.get(key);
                    if (m == null) {
                        throw new ColumnNotFoundException("Column '" + key + "' not found in '" + tableName + "' table");
                    } else {
                        sql += " " + m.getName() + " =? AND";
                    }
                }

                sql = sql.substring(0, sql.length() - 3);
                ps = databaseService.getConnection().prepareStatement(sql);

                int index = 0;
                for (String key : keys) {

                    MetaData m = map2.get(key);
                    Field f = m.getField();
                    index = index + 1;
                    Object value = map.get(key);

                    if (f.getType().getName().equals("boolean")
                            || f.getType().getName().equals("java.lang.Boolean")) {
                        ps.setBoolean(index, (Boolean) value);
                    } else if (f.getType().getName().equals("short")
                            || f.getType().getName().equals("java.lang.Short")) {
                        ps.setShort(index, (Short) value);
                    } else if (f.getType().getName().equals("java.lang.String")) {
                        ps.setString(index, value.toString());
                    } else if (f.getType().getName().equals("int")
                            || f.getType().getName().equals("java.lang.Integer")) {
                        ps.setInt(index, (Integer) value);
                    } else if (f.getType().getName().equals("long")
                            || f.getType().getName().equals("java.lang.Long")) {
                        ps.setLong(index, (Long) value);
                    } else if (f.getType().getName().equals("float")
                            || f.getType().getName().equals("java.lang.Float")) {
                        ps.setFloat(index, (Float) value);
                    } else if (f.getType().getName().equals("double")
                            || f.getType().getName().equals("java.lang.Double")) {
                        ps.setDouble(index, (Double) value);
                    } else if (f.getType().getName().equals("java.math.BigDecimal")) {
                        ps.setBigDecimal(index, (BigDecimal) value);
                    } else if (f.getType().getName().equals("byte")
                            || f.getType().getName().equals("java.lang.Byte")) {
                        ps.setByte(index, (Byte) value);
                    } else if (f.getType().getName().equals("[B")) {
                        ps.setBytes(index, convertToBytes(value));
                    } else if (f.getType().getName().equals("char")
                            || f.getType().getName().equals("java.lang.Character")) {
                        ps.setString(index, value.toString());
                    } else if (f.getType().getName().equals("java.sql.Date")) {
                        ps.setDate(index, (Date) value);
                    } else if (f.getType().getName().equals("java.sql.Blob")) {
                        ps.setBlob(index, (Blob) value);
                    } else if (f.getType().getName().equals("java.io.InputStream")) {
                        ps.setBinaryStream(index, (InputStream) value);
                    } else if (f.getType().getName().equals("java.sql.Time")) {
                        ps.setTime(index, (Time) value);
                    } else if (f.getType().getName().equals("java.sql.Timestamp")) {
                        ps.setTimestamp(index, (Timestamp) value);
                    }
                }
            } else {
                TRUE_FALSE = false;
                sql = "SELECT * FROM " + tableName;
            }

            if (TRUE_FALSE) {
                getRow(c, sql, ps.executeQuery());
            } else {
                getRow(c, sql, null);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return iterableList;
    }

    /*
     @ Private method to fetch rows
     */
    private Iterable<T> getRow(Class<T> c, String sql, ResultSet resultSet) throws TableNotFoundException, InvalidTypeException, PrimaryKeyNotFoundException, SQLException, InstantiationException, IllegalAccessException, MultiplePrimaryKeyException {

        iterableList.clear();
        System.out.println("SQL : " + sql);
        metaDataList = getMetaDataList(c);
        if (resultSet == null) {
            resultSet = databaseService.executeNativeQuery(sql);
        }
        while (resultSet.next()) {
            T obj = c.newInstance();
            for (MetaData m : metaDataList) {
                Field f = m.getField();
                f.setAccessible(true);
                if (f.getType().getName().equals("boolean")
                        || f.getType().getName().equals("java.lang.Boolean")) {
                    f.set(obj, resultSet.getBoolean(m.getName()));
                } else if (f.getType().getName().equals("short")
                        || f.getType().getName().equals("java.lang.Short")) {
                    f.set(obj, resultSet.getShort(m.getName()));
                } else if (f.getType().getName().equals("java.lang.String")) {
                    f.set(obj, resultSet.getString(m.getName()));
                } else if (f.getType().getName().equals("int")
                        || f.getType().getName().equals("java.lang.Integer")) {
                    f.set(obj, resultSet.getInt(m.getName()));
                } else if (f.getType().getName().equals("long")
                        || f.getType().getName().equals("java.lang.Long")) {
                    f.set(obj, resultSet.getLong(m.getName()));
                } else if (f.getType().getName().equals("float")
                        || f.getType().getName().equals("java.lang.Float")) {
                    f.set(obj, resultSet.getFloat(m.getName()));
                } else if (f.getType().getName().equals("double")
                        || f.getType().getName().equals("java.lang.Double")) {
                    f.set(obj, resultSet.getDouble(m.getName()));
                } else if (f.getType().getName().equals("java.math.BigDecimal")) {
                    f.set(obj, resultSet.getBigDecimal(m.getName()));
                } else if (f.getType().getName().equals("byte")
                        || f.getType().getName().equals("java.lang.Byte")) {
                    f.set(obj, resultSet.getByte(m.getName()));
                } else if (f.getType().getName().equals("[B")) {
                    f.set(obj, resultSet.getBytes(m.getName()));
                } else if (f.getType().getName().equals("char")
                        || f.getType().getName().equals("java.lang.Character")) {
                    f.setChar(obj, resultSet.getString(m.getName()) == null ? ' '
                            : resultSet.getString(m.getName()).charAt(0));
                } else if (f.getType().getName().equals("java.sql.Date")) {
                    f.set(obj, resultSet.getDate(m.getName()));
                } else if (f.getType().getName().equals("java.sql.Blob")) {
                    f.set(obj, resultSet.getBlob(m.getName()));
                } else if (f.getType().getName().equals("java.io.InputStream")) {
                    f.set(obj, resultSet.getBinaryStream(m.getName()));
                } else if (f.getType().getName().equals("java.sql.Time")) {
                    f.set(obj, resultSet.getTime(m.getName()));
                } else if (f.getType().getName().equals("java.sql.Timestamp")) {
                    f.set(obj, resultSet.getTimestamp(m.getName()));
                }

            }
            iterableList.add(obj);
        }
        return iterableList;
    }

    /*
     @ Delete Row by Object
     */
    public synchronized <S extends T> void deleteRowByObject(S obj) {
        try {
            metaDataList = getMetaDataList((Class<T>) obj.getClass());
            Object value = null;
            for (MetaData m : metaDataList) {
                if (m.isIsPrimaryKey()) {
                    value = m.getField().get(obj);
                    break;
                }
            }
            deleteRowByPrimaryKey((Class<T>) obj.getClass(), value);
        } catch (Exception ex) {
            TRUE_FALSE = false;
            ex.printStackTrace();
        }
    }

    /*
     @ Delete row by primary key
     */
    public synchronized void deleteRowByPrimaryKey(Class<T> c, Object value) {
        try {
            String tableName = getTableName(c);
            metaDataList = getMetaDataList(c);
            for (MetaData m : metaDataList) {
                if (m.isPrimaryKey) {
                    String sql = "DELETE FROM " + tableName + " WHERE " + m.getName() + " = ?";
                    PreparedStatement ps = databaseService.getConnection().prepareStatement(sql);
                    Field f = m.getField();
                    int index = 1;
                    if (f.getType().getName().equals("boolean")
                            || f.getType().getName().equals("java.lang.Boolean")) {
                        ps.setBoolean(index, (Boolean) value);
                    } else if (f.getType().getName().equals("short")
                            || f.getType().getName().equals("java.lang.Short")) {
                        ps.setShort(index, (Short) value);
                    } else if (f.getType().getName().equals("java.lang.String")) {
                        ps.setString(index, value.toString());
                    } else if (f.getType().getName().equals("int")
                            || f.getType().getName().equals("java.lang.Integer")) {
                        ps.setInt(index, (Integer) value);
                    } else if (f.getType().getName().equals("long")
                            || f.getType().getName().equals("java.lang.Long")) {
                        ps.setLong(index, (Long) value);
                    } else if (f.getType().getName().equals("float")
                            || f.getType().getName().equals("java.lang.Float")) {
                        ps.setFloat(index, (Float) value);
                    } else if (f.getType().getName().equals("double")
                            || f.getType().getName().equals("java.lang.Double")) {
                        ps.setDouble(index, (Double) value);
                    } else if (f.getType().getName().equals("java.math.BigDecimal")) {
                        ps.setBigDecimal(index, (BigDecimal) value);
                    } else if (f.getType().getName().equals("byte")
                            || f.getType().getName().equals("java.lang.Byte")) {
                        ps.setByte(index, (Byte) value);
                    } else if (f.getType().getName().equals("[B")) {
                        ps.setBytes(index, convertToBytes(value));
                    } else if (f.getType().getName().equals("char")
                            || f.getType().getName().equals("java.lang.Character")) {
                        ps.setString(index, value.toString());
                    } else if (f.getType().getName().equals("java.sql.Date")) {
                        ps.setDate(index, (Date) value);
                    } else if (f.getType().getName().equals("java.sql.Blob")) {
                        ps.setBlob(index, (Blob) value);
                    } else if (f.getType().getName().equals("java.io.InputStream")) {
                        ps.setBinaryStream(index, (InputStream) value);
                    } else if (f.getType().getName().equals("java.sql.Time")) {
                        ps.setTime(index, (Time) value);
                    } else if (f.getType().getName().equals("java.sql.Timestamp")) {
                        ps.setTimestamp(index, (Timestamp) value);
                    }
                    System.out.println("SQL : " + sql);
                    ps.execute();
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /*
     @ Delete row with parameterized query
     */
    public synchronized void deleteRowByParameterQuery(Class<T> c, DeleteQuery deleteQuery) {

        try {
            String tableName = getTableName(c);
            String sql = "DELETE FROM " + tableName + " WHERE";
            Map<String, Object> map = deleteQuery.getQueryValueWithColumnName();
            if (!map.isEmpty()) {
                Set<String> keys = map.keySet();
                getMetaDataList(c);
                Map<String, MetaData> map2 = new HashMap<>();
                metaDataList.forEach((MetaData v) -> {
                    map2.put(v.getName(), v);
                });
                for (String key : keys) {
                    MetaData m = map2.get(key);
                    if (m == null) {
                        throw new ColumnNotFoundException("Column '" + key + "' not found in '" + tableName + "' table");
                    } else {
                        Field field = m.getField();
                        sql += " " + m.getName() + " =? AND";
                    }
                }
                sql = sql.substring(0, sql.length() - 3);
                PreparedStatement ps = databaseService.getConnection().prepareStatement(sql);
                int index = 0;
                for (String key : keys) {
                    index = index + 1;
                    MetaData m = map2.get(key);
                    Field f = m.getField();
                    Object value = map.get(key);
                    if (f.getType().getName().equals("boolean")
                            || f.getType().getName().equals("java.lang.Boolean")) {
                        ps.setBoolean(index, (Boolean) value);
                    } else if (f.getType().getName().equals("short")
                            || f.getType().getName().equals("java.lang.Short")) {
                        ps.setShort(index, (Short) value);
                    } else if (f.getType().getName().equals("java.lang.String")) {
                        ps.setString(index, value.toString());
                    } else if (f.getType().getName().equals("int")
                            || f.getType().getName().equals("java.lang.Integer")) {
                        ps.setInt(index, (Integer) value);
                    } else if (f.getType().getName().equals("long")
                            || f.getType().getName().equals("java.lang.Long")) {
                        ps.setLong(index, (Long) value);
                    } else if (f.getType().getName().equals("float")
                            || f.getType().getName().equals("java.lang.Float")) {
                        ps.setFloat(index, (Float) value);
                    } else if (f.getType().getName().equals("double")
                            || f.getType().getName().equals("java.lang.Double")) {
                        ps.setDouble(index, (Double) value);
                    } else if (f.getType().getName().equals("java.math.BigDecimal")) {
                        ps.setBigDecimal(index, (BigDecimal) value);
                    } else if (f.getType().getName().equals("byte")
                            || f.getType().getName().equals("java.lang.Byte")) {
                        ps.setByte(index, (Byte) value);
                    } else if (f.getType().getName().equals("[B")) {
                        ps.setBytes(index, convertToBytes(value));
                    } else if (f.getType().getName().equals("char")
                            || f.getType().getName().equals("java.lang.Character")) {
                        ps.setString(index, value.toString());
                    } else if (f.getType().getName().equals("java.sql.Date")) {
                        ps.setDate(index, (Date) value);
                    } else if (f.getType().getName().equals("java.sql.Blob")) {
                        ps.setBlob(index, (Blob) value);
                    } else if (f.getType().getName().equals("java.io.InputStream")) {
                        ps.setBinaryStream(index, (InputStream) value);
                    } else if (f.getType().getName().equals("java.sql.Time")) {
                        ps.setTime(index, (Time) value);
                    } else if (f.getType().getName().equals("java.sql.Timestamp")) {
                        ps.setTimestamp(index, (Timestamp) value);
                    }
                }
                System.out.println("SQL : " + sql);
                ps.execute();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    /*
     @ Delete all rows from given Data Table
     */
    public synchronized boolean deleteAllRows(Class<T> c) {
        TRUE_FALSE = true;
        try {
            String tableName = getTableName(c);
            String sql = "DELETE FROM " + tableName;
            databaseService.createNativeQuery(sql);
        } catch (TableNotFoundException | SQLException ex) {
            TRUE_FALSE = false;
            ex.printStackTrace();
        }
        return TRUE_FALSE;
    }


    /*
     @ Save Object to Data Table 
     */
    public synchronized <S extends T> boolean save(S obj) {
        TRUE_FALSE = true;
        try {
            String tableName = getTableName((Class<T>) obj.getClass());
            String sql = "INSERT INTO " + tableName;
            String columns = "";
            String values = "";
            getMetaDataList((Class<T>) obj.getClass());
            for (MetaData m : metaDataList) {
                Field field = m.getField();
                field.setAccessible(true);
                if (field.getAnnotation(PrimaryKey.class) != null && field.getAnnotation(PrimaryKey.class).autoIncrement()) {
                    continue;
                }
                columns += m.getName() + ", ";
                values += "?, ";
            }
            columns = columns.substring(0, columns.length() - 2);
            values = values.substring(0, values.length() - 2);
            sql += "(" + columns + ") VALUES(" + values + ")";
            System.out.println("SQL : " + sql);
            PreparedStatement ps = databaseService.getConnection().prepareCall(sql);
            int index = 0;
            for (MetaData m : metaDataList) {
                Field f = m.getField();
                f.setAccessible(true);
                if (f.getAnnotation(PrimaryKey.class) != null && f.getAnnotation(PrimaryKey.class).autoIncrement()) {
                    continue;
                }
                index = index + 1;
                if (f.getType().getName().equals("boolean")
                        || f.getType().getName().equals("java.lang.Boolean")) {
                    ps.setBoolean(index, f.getBoolean(obj));
                } else if (f.getType().getName().equals("short")
                        || f.getType().getName().equals("java.lang.Short")) {
                    ps.setShort(index, (Short) f.getShort(obj));
                } else if (f.getType().getName().equals("java.lang.String")) {
                    ps.setString(index, f.get(obj).toString());
                } else if (f.getType().getName().equals("int")
                        || f.getType().getName().equals("java.lang.Integer")) {
                    ps.setInt(index, (Integer) f.get(obj));
                } else if (f.getType().getName().equals("long")
                        || f.getType().getName().equals("java.lang.Long")) {
                    ps.setLong(index, (Long) f.get(obj));
                } else if (f.getType().getName().equals("float")
                        || f.getType().getName().equals("java.lang.Float")) {
                    ps.setFloat(index, (Float) f.get(obj));
                } else if (f.getType().getName().equals("double")
                        || f.getType().getName().equals("java.lang.Double")) {
                    ps.setDouble(index, (Double) f.get(obj));
                } else if (f.getType().getName().equals("java.math.BigDecimal")) {
                    ps.setBigDecimal(index, (BigDecimal) f.get(obj));
                } else if (f.getType().getName().equals("byte")
                        || f.getType().getName().equals("java.lang.Byte")) {
                    ps.setByte(index, (Byte) f.get(obj));
                } else if (f.getType().getName().equals("[B")) {
                    ps.setBytes(index, convertToBytes(f.get(obj)));
                } else if (f.getType().getName().equals("char")
                        || f.getType().getName().equals("java.lang.Character")) {
                    ps.setString(index, f.get(obj).toString());
                } else if (f.getType().getName().equals("java.sql.Date")) {
                    ps.setDate(index, (Date) f.get(obj));
                } else if (f.getType().getName().equals("java.sql.Blob")) {
                    ps.setBlob(index, (Blob) f.get(obj));
                } else if (f.getType().getName().equals("java.io.InputStream")) {
                    ps.setBinaryStream(index, (InputStream) f.get(obj));
                } else if (f.getType().getName().equals("java.sql.Time")) {
                    ps.setTime(index, (Time) f.get(obj));
                } else if (f.getType().getName().equals("java.sql.Timestamp")) {
                    ps.setTimestamp(index, (Timestamp) f.get(obj));
                }
            }
            ps.execute();
        } catch (Exception ex) {
            TRUE_FALSE = false;
            ex.printStackTrace();
        }
        return TRUE_FALSE;
    }

    private byte[] convertToBytes(Object object) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutput out = new ObjectOutputStream(bos)) {
            out.writeObject(object);
            return bos.toByteArray();
        }

    }


    /*
     @ Update row using 'UpdateQuery'
     */
    public synchronized void updateRowByParameterQuery(Class<T> c, UpdateQuery updateQuery) {

        int index = 0;
        try {
            String tableName = getTableName(c);
            String sql = "UPDATE " + tableName + " SET";
            Map<String, Object> map = updateQuery.getUpdateColumnNameWithValue();
            Map<String, Object> map2 = updateQuery.getConditionalColumnNameWithValue();

            if (!map.isEmpty()) {
                Set<String> keys = map.keySet();
                getMetaDataList(c);
                Map<String, MetaData> map3 = new HashMap<>();
                metaDataList.forEach((MetaData v) -> {
                    map3.put(v.getName(), v);
                });
                for (String key : keys) {
                    MetaData m = map3.get(key);
                    if (m == null) {
                        throw new ColumnNotFoundException("Column '" + key + "' not found in '" + tableName + "' table");
                    } else {
                        Field field = m.getField();
                        sql += " " + m.getName() + " =?,";
                    }
                }
                sql = sql.substring(0, sql.length() - 1);
            }
            if (!map2.isEmpty()) {

                Set<String> keys = map2.keySet();
                getMetaDataList(c);
                Map<String, MetaData> map3 = new HashMap<>();
                metaDataList.forEach((MetaData v) -> {
                    map3.put(v.getName(), v);
                });
                sql += " WHERE";
                for (String key : keys) {

                    MetaData m = map3.get(key);
                    if (m == null) {
                        throw new ColumnNotFoundException("Column '" + key + "' not found in '" + tableName + "' table");
                    } else {
                        Field field = m.getField();
                        sql += " " + m.getName() + " =? AND";
                    }
                }
                sql = sql.substring(0, sql.length() - 3);
            }
            System.out.println("SQL : " + sql);
            PreparedStatement ps = databaseService.getConnection().prepareStatement(sql);
            if (!map.isEmpty()) {
                Set<String> keys = map.keySet();
                getMetaDataList(c);
                Map<String, MetaData> map3 = new HashMap<>();
                metaDataList.forEach((MetaData v) -> {
                    map3.put(v.getName(), v);
                });
                for (String key : keys) {

                    MetaData m = map3.get(key);
                    Field f = m.getField();
                    index = index + 1;
                    Object value = map.get(key);

                    if (f.getType().getName().equals("boolean")
                            || f.getType().getName().equals("java.lang.Boolean")) {
                        ps.setBoolean(index, (Boolean) value);
                    } else if (f.getType().getName().equals("short")
                            || f.getType().getName().equals("java.lang.Short")) {
                        ps.setShort(index, (Short) value);
                    } else if (f.getType().getName().equals("java.lang.String")) {
                        ps.setString(index, value.toString());
                    } else if (f.getType().getName().equals("int")
                            || f.getType().getName().equals("java.lang.Integer")) {
                        ps.setInt(index, (Integer) value);
                    } else if (f.getType().getName().equals("long")
                            || f.getType().getName().equals("java.lang.Long")) {
                        ps.setLong(index, (Long) value);
                    } else if (f.getType().getName().equals("float")
                            || f.getType().getName().equals("java.lang.Float")) {
                        ps.setFloat(index, (Float) value);
                    } else if (f.getType().getName().equals("double")
                            || f.getType().getName().equals("java.lang.Double")) {
                        ps.setDouble(index, (Double) value);
                    } else if (f.getType().getName().equals("java.math.BigDecimal")) {
                        ps.setBigDecimal(index, (BigDecimal) value);
                    } else if (f.getType().getName().equals("byte")
                            || f.getType().getName().equals("java.lang.Byte")) {
                        ps.setByte(index, (Byte) value);
                    } else if (f.getType().getName().equals("[B")) {
                        ps.setBytes(index, convertToBytes(value));
                    } else if (f.getType().getName().equals("char")
                            || f.getType().getName().equals("java.lang.Character")) {
                        ps.setString(index, value.toString());
                    } else if (f.getType().getName().equals("java.sql.Date")) {
                        ps.setDate(index, (Date) value);
                    } else if (f.getType().getName().equals("java.sql.Blob")) {
                        ps.setBlob(index, (Blob) value);
                    } else if (f.getType().getName().equals("java.io.InputStream")) {
                        ps.setBinaryStream(index, (InputStream) value);
                    } else if (f.getType().getName().equals("java.sql.Time")) {
                        ps.setTime(index, (Time) value);
                    } else if (f.getType().getName().equals("java.sql.Timestamp")) {
                        ps.setTimestamp(index, (Timestamp) value);
                    }
                }
            }
            if (!map2.isEmpty()) {
                Set<String> keys = map2.keySet();
                getMetaDataList(c);
                Map<String, MetaData> map3 = new HashMap<>();
                metaDataList.forEach((MetaData v) -> {
                    map3.put(v.getName(), v);
                });
                for (String key : keys) {
                    MetaData m = map3.get(key);
                    Field f = m.getField();
                    index = index + 1;
                    Object value = map2.get(key);
                    if (f.getType().getName().equals("boolean")
                            || f.getType().getName().equals("java.lang.Boolean")) {
                        ps.setBoolean(index, (Boolean) value);
                    } else if (f.getType().getName().equals("short")
                            || f.getType().getName().equals("java.lang.Short")) {
                        ps.setShort(index, (Short) value);
                    } else if (f.getType().getName().equals("java.lang.String")) {
                        ps.setString(index, value.toString());
                    } else if (f.getType().getName().equals("int")
                            || f.getType().getName().equals("java.lang.Integer")) {
                        ps.setInt(index, (Integer) value);
                    } else if (f.getType().getName().equals("long")
                            || f.getType().getName().equals("java.lang.Long")) {
                        ps.setLong(index, (Long) value);
                    } else if (f.getType().getName().equals("float")
                            || f.getType().getName().equals("java.lang.Float")) {
                        ps.setFloat(index, (Float) value);
                    } else if (f.getType().getName().equals("double")
                            || f.getType().getName().equals("java.lang.Double")) {
                        ps.setDouble(index, (Double) value);
                    } else if (f.getType().getName().equals("java.math.BigDecimal")) {
                        ps.setBigDecimal(index, (BigDecimal) value);
                    } else if (f.getType().getName().equals("byte")
                            || f.getType().getName().equals("java.lang.Byte")) {
                        ps.setByte(index, (Byte) value);
                    } else if (f.getType().getName().equals("[B")) {
                        ps.setBytes(index, convertToBytes(value));
                    } else if (f.getType().getName().equals("char")
                            || f.getType().getName().equals("java.lang.Character")) {
                        ps.setString(index, value.toString());
                    } else if (f.getType().getName().equals("java.sql.Date")) {
                        ps.setDate(index, (Date) value);
                    } else if (f.getType().getName().equals("java.sql.Blob")) {
                        ps.setBlob(index, (Blob) value);
                    } else if (f.getType().getName().equals("java.io.InputStream")) {
                        ps.setBinaryStream(index, (InputStream) value);
                    } else if (f.getType().getName().equals("java.sql.Time")) {
                        ps.setTime(index, (Time) value);
                    } else if (f.getType().getName().equals("java.sql.Timestamp")) {
                        ps.setTimestamp(index, (Timestamp) value);
                    }
                }
            }
            ps.executeUpdate();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    /*
     @ Update data table
     */
    public synchronized <U extends T> void update(U obj) {

        try {
            Class<T> c = (Class<T>) obj.getClass();
            String tableName = getTableName(c);
            String sql = "UPDATE " + tableName + " SET";
            String pkName = "", updateString = "";
            Object pkValue = "";
            Field field = null;

            metaDataList = getMetaDataList(c);

            for (MetaData m : metaDataList) {
                Field f = m.getField();
                f.setAccessible(true);
                if (f.getAnnotation(PrimaryKey.class) != null) {
                    pkName = f.getName();
                    pkValue = f.get(obj);
                    field = f;
                } else {
                    sql += " " + m.getName() + " =?,";
                }
            }
            sql = sql.substring(0, sql.length() - 1) + " WHERE " + pkName + " =?";
            PreparedStatement ps = databaseService.getConnection().prepareStatement(sql);
            int index = 0;
            for (MetaData m : metaDataList) {
                Field f = m.getField();
                f.setAccessible(true);
                if (f.getAnnotation(PrimaryKey.class) != null) {
                    continue;
                }
                index = index + 1;
                if (f.getType().getName().equals("boolean")
                        || f.getType().getName().equals("java.lang.Boolean")) {
                    ps.setBoolean(index, f.getBoolean(obj));
                } else if (f.getType().getName().equals("short")
                        || f.getType().getName().equals("java.lang.Short")) {
                    ps.setShort(index, (Short) f.getShort(obj));
                } else if (f.getType().getName().equals("java.lang.String")) {
                    ps.setString(index, f.get(obj).toString());
                } else if (f.getType().getName().equals("int")
                        || f.getType().getName().equals("java.lang.Integer")) {
                    ps.setInt(index, (Integer) f.get(obj));
                } else if (f.getType().getName().equals("long")
                        || f.getType().getName().equals("java.lang.Long")) {
                    ps.setLong(index, (Long) f.get(obj));
                } else if (f.getType().getName().equals("float")
                        || f.getType().getName().equals("java.lang.Float")) {
                    ps.setFloat(index, (Float) f.get(obj));
                } else if (f.getType().getName().equals("double")
                        || f.getType().getName().equals("java.lang.Double")) {
                    ps.setDouble(index, (Double) f.get(obj));
                } else if (f.getType().getName().equals("java.math.BigDecimal")) {
                    ps.setBigDecimal(index, (BigDecimal) f.get(obj));
                } else if (f.getType().getName().equals("byte")
                        || f.getType().getName().equals("java.lang.Byte")) {
                    ps.setByte(index, (Byte) f.get(obj));
                } else if (f.getType().getName().equals("[B")) {
                    ps.setBytes(index, convertToBytes(f.get(obj)));
                } else if (f.getType().getName().equals("char")
                        || f.getType().getName().equals("java.lang.Character")) {
                    ps.setString(index, f.get(obj).toString());
                } else if (f.getType().getName().equals("java.sql.Date")) {
                    ps.setDate(index, (Date) f.get(obj));
                } else if (f.getType().getName().equals("java.sql.Blob")) {
                    ps.setBlob(index, (Blob) f.get(obj));
                } else if (f.getType().getName().equals("java.io.InputStream")) {
                    ps.setBinaryStream(index, (InputStream) f.get(obj));
                } else if (f.getType().getName().equals("java.sql.Time")) {
                    ps.setTime(index, (Time) f.get(obj));
                } else if (f.getType().getName().equals("java.sql.Timestamp")) {
                    ps.setTimestamp(index, (Timestamp) f.get(obj));
                }
            }
            Field f = field;
            Object value = pkValue;
            index = index + 1;
            if (f.getType().getName().equals("boolean")
                    || f.getType().getName().equals("java.lang.Boolean")) {
                ps.setBoolean(index, f.getBoolean(obj));
            } else if (f.getType().getName().equals("short")
                    || f.getType().getName().equals("java.lang.Short")) {
                ps.setShort(index, (Short) f.getShort(obj));
            } else if (f.getType().getName().equals("java.lang.String")) {
                ps.setString(index, f.get(obj).toString());
            } else if (f.getType().getName().equals("int")
                    || f.getType().getName().equals("java.lang.Integer")) {
                ps.setInt(index, (Integer) f.get(obj));
            } else if (f.getType().getName().equals("long")
                    || f.getType().getName().equals("java.lang.Long")) {
                ps.setLong(index, (Long) f.get(obj));
            } else if (f.getType().getName().equals("float")
                    || f.getType().getName().equals("java.lang.Float")) {
                ps.setFloat(index, (Float) f.get(obj));
            } else if (f.getType().getName().equals("double")
                    || f.getType().getName().equals("java.lang.Double")) {
                ps.setDouble(index, (Double) f.get(obj));
            } else if (f.getType().getName().equals("java.math.BigDecimal")) {
                ps.setBigDecimal(index, (BigDecimal) f.get(obj));
            } else if (f.getType().getName().equals("byte")
                    || f.getType().getName().equals("java.lang.Byte")) {
                ps.setByte(index, (Byte) f.get(obj));
            } else if (f.getType().getName().equals("[B")) {
                ps.setBytes(index, convertToBytes(f.get(obj)));
            } else if (f.getType().getName().equals("char")
                    || f.getType().getName().equals("java.lang.Character")) {
                ps.setString(index, f.get(obj).toString());
            } else if (f.getType().getName().equals("java.sql.Date")) {
                ps.setDate(index, (Date) f.get(obj));
            } else if (f.getType().getName().equals("java.sql.Blob")) {
                ps.setBlob(index, (Blob) f.get(obj));
            } else if (f.getType().getName().equals("java.io.InputStream")) {
                ps.setBinaryStream(index, (InputStream) f.get(obj));
            } else if (f.getType().getName().equals("java.sql.Time")) {
                ps.setTime(index, (Time) f.get(obj));
            } else if (f.getType().getName().equals("java.sql.Timestamp")) {
                ps.setTimestamp(index, (Timestamp) f.get(obj));
            }
            System.out.println("SQL : " + sql);
            ps.executeUpdate();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    /*
     @ count() method for count spesific column
     */
    public long count(Class<T> c, String columnName) {
        long count = 0;
        try {
            String tableName = getTableName(c);
            String sql = "SELECT COUNT(" + columnName + ") FROM " + tableName;
            System.out.println("SQL : " + sql);
            resultSet = databaseService.executeNativeQuery(sql);
            resultSet.next();
            count = resultSet.getLong(1);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return count;
    }

    /*
     @ avg() method to get avg value by spesific column
     */
    public double avg(Class<T> c, String columnName) {
        double avg = 0;
        try {
            String tableName = getTableName(c);
            String sql = "SELECT AVG(" + columnName + ") FROM " + tableName;
            System.out.println("SQL : " + sql);
            resultSet = databaseService.executeNativeQuery(sql);
            resultSet.next();
            avg = resultSet.getDouble(1);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return avg;
    }

    /*
     @ sum() method to get sum of spesific column
     */
    public double sum(Class<T> c, String columnName) {
        double sum = 0;
        try {
            String tableName = getTableName(c);
            String sql = "SELECT SUM(" + columnName + ") FROM " + tableName;
            System.out.println("SQL : " + sql);
            resultSet = databaseService.executeNativeQuery(sql);
            resultSet.next();
            sum = resultSet.getDouble(1);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return sum;
    }

    /*
     @ min() method to get minimum value of spesific column
     */
    public Object min(Class<T> c, String columnName) {
        Object value = null;
        try {
            String tableName = getTableName(c);
            String sql = "SELECT MIN(" + columnName + ") FROM " + tableName;
            System.out.println("SQL : " + sql);
            resultSet = databaseService.executeNativeQuery(sql);
            resultSet.next();
            value = resultSet.getObject(1);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return value;
    }

    /*
     @ max() method to get maximum value of spesific column
     */
    public Object max(Class<T> c, String columnName) {
        Object value = null;
        try {
            String tableName = getTableName(c);
            String sql = "SELECT MAX(" + columnName + ") FROM " + tableName;
            System.out.println("SQL : " + sql);
            resultSet = databaseService.executeNativeQuery(sql);
            resultSet.next();
            value = resultSet.getObject(1);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return value;
    }

    
    /*
    @ Like metod to get row by sql like operation
    */
    public synchronized Iterable<T> like(Class<T> c, String columnName, String likeCondition) {        
        try {
            String sql = "SELECT * FROM "+getTableName(c)+" WHERE "
                    +columnName.trim()+" LIKE '"+likeCondition.trim()+"'";
            return  getRow(c, sql, null);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

    /*
     @ Find that Table is exits or not.
     */
    public synchronized boolean isTableExists(Class<T> c) {
        TRUE_FALSE = true;
        try {
            String tableName = getTableName(c);
            String sql = "";
            if (driver.equals(DatabaseConnection.DERBY_DRIVER)) {
                sql = "SELECT * FROM " + tableName + " OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY";
            } else {
                sql = "SELECT * FROM " + tableName + " LIMIT 1";
            }
            databaseService.executeNativeQuery(sql);
        } catch (Exception ex) {
            TRUE_FALSE = false;
        }
        return TRUE_FALSE;
    }

    /*
     @ Private Method to get Accessable Table Name.
     */
    private synchronized String getTableName(Class<T> c) throws TableNotFoundException {
        /*
         @ Get table name as a data-table name.
         */
        String tableName = c.getSimpleName();
        /*
         @ Crete an exception if data-table class not anotated with @Table Annotation.
         */
        if (c.getAnnotation(Table.class) == null) {
            throw new TableNotFoundException("'" + tableName + "' is not a Database Table class, If you want to use this class as Data Table please annotated it as @Table");
        } else if (c.getAnnotation(Table.class).name().length() != 0) {
            /*
             @ Update data-table name if modify with @Table -> name.
             */
            tableName = c.getAnnotation(Table.class).name();
        }
        return tableName.trim();
    }

    /*
     @ Private method to access Data Table's Fields and return them with list
     */
    private synchronized List<MetaData> getMetaDataList(Class<T> c) throws InvalidTypeException, PrimaryKeyNotFoundException, TableNotFoundException, MultiplePrimaryKeyException {
        int pkCount = 0;
        metaDataList.clear();
        /*
         @ Find Metadata of Given Class
         */
        Field[] fields = c.getDeclaredFields();
        for (Field field : fields) {

            if (field.getAnnotation(NotColumn.class) == null) {

                field.setAccessible(true);
                MetaData metaData = new MetaData();
                metaData.setField(field);
                metaData.setIsColumn(true);
                String type = field.getType().getName();

                // System.out.println("Type : " + type);
                if (type.contains("[")) {
                    type = field.getType().getSimpleName();
                }
                String convertType = dataTypeConverter.get(type);
                if (convertType == null) {
                    throw new InvalidTypeException("Invalid type araise '" + field.getType().getSimpleName() + "', Please remove this type or use @NotColumn.");
                } else {
                    metaData.setType(convertType);
                }
                if (field.getAnnotation(Column.class) != null) {
                    String name = field.getAnnotation(Column.class).name().trim();
                    metaData.setName(name.length() == 0 ? field.getName().trim() : name);
                    Integer len = dataTypeDefaultSize.get(field.getType().getName());
                    metaData.setLength(len == null ? 0 : len < 0 ? 0 : field.getAnnotation(Column.class).length());
                    metaData.setIsNullAble(field.getAnnotation(Column.class).nullable());
                } else {
                    metaData.setName(field.getName().trim());
                    Integer len = dataTypeDefaultSize.get(field.getType().getName());
                    metaData.setLength(len == null ? 0 : len < 0 ? 0 : len);
                    metaData.setIsNullAble(true);
                }
                /*
                 @ Change length if any data type have fixed length
                 */
                if (field.getAnnotation(PrimaryKey.class) != null) {
                    pkCount++;
                    metaData.setIsNullAble(false);
                    metaData.setIsPrimaryKey(true);
                    if (field.getAnnotation(PrimaryKey.class).autoIncrement()) {
                        /*
                         @ Define auto_increment able data types
                         */
                        if (field.getType().getName().equals("int")
                                || field.getType().getName().equals("java.lang.Integer")
                                || field.getType().getName().equals("long")
                                || field.getType().getName().equalsIgnoreCase("java.lang.Long")) {
                            metaData.setIsAutoIncrement(true);
                        } else {
                            throw new InvalidTypeException("Invalid type specified "
                                    + "for auto_increment, column '"
                                    + metaData.getName()
                                    + "' The valid types for auto_increment columns are  int and long");
                        }
                    }
                }
                metaDataList.add(metaData);
            }
        }
        if (pkCount == 0) {
            throw new PrimaryKeyNotFoundException("No primary key specifier to '" + getTableName(c) + "' class. Please specify any field as primary key with @PrimaryKey annotation");
        } else if (pkCount > 1) {
            throw new MultiplePrimaryKeyException("Multiple primary key found at class '" + c.getName() + "'. You should define only one primary key");
        }
        return metaDataList;
    }

    /*
     @ Private and Static inner method to store and access Data Tables's 
     @ Fields attribute
     */
    private static class MetaData {

        public Field field;
        public String name;
        public String type;
        public long length;
        public boolean isColumn;
        public boolean isNullAble;
        public boolean isPrimaryKey;
        public boolean isAutoIncrement;

        public Field getField() {
            return field;
        }

        public void setField(Field field) {
            this.field = field;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public long getLength() {
            return length;
        }

        public void setLength(long length) {
            this.length = length;
        }

        public boolean isIsColumn() {
            return isColumn;
        }

        public void setIsColumn(boolean isColumn) {
            this.isColumn = isColumn;
        }

        public boolean isIsNullAble() {
            return isNullAble;
        }

        public void setIsNullAble(boolean isNullAble) {
            this.isNullAble = isNullAble;
        }

        public boolean isIsPrimaryKey() {
            return isPrimaryKey;
        }

        public void setIsPrimaryKey(boolean isPrimaryKey) {
            this.isPrimaryKey = isPrimaryKey;
        }

        public boolean isIsAutoIncrement() {
            return isAutoIncrement;
        }

        public void setIsAutoIncrement(boolean isAutoIncrement) {
            this.isAutoIncrement = isAutoIncrement;
        }

        @Override
        public String toString() {
            return "MetaData{" + "name=" + name + ", type=" + type + ", length=" + length + ", isColumn=" + isColumn + ", isNullAble=" + isNullAble + ", isPrimaryKey=" + isPrimaryKey + ", isAutoIncrement=" + isAutoIncrement + '}';
        }
    }

    public synchronized Object nativeQuery(String sql) {
        Object dataObject = null;
        try {
            dataObject = databaseService.nativeQuery(sql);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return dataObject;
    }

}

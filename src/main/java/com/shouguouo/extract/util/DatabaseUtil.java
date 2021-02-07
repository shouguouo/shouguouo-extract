package com.shouguouo.extract.util;


import com.shouguouo.extract.enums.DatabaseType;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author shouguouo~
 * @date 2020/8/22 - 9:55
 */
public class DatabaseUtil {
    private static final int TIMEOUT_SECONDS = 15;
    private static final int SOCKET_TIMEOUT_INSECOND = 172800;

    public static List<String> getTableColumns(DatabaseType dataBaseType,
                                               String jdbcUrl, String user, String pass, String tableNameWithOwner) {
        Connection conn = getConnection(dataBaseType, jdbcUrl, user, pass, String.valueOf(SOCKET_TIMEOUT_INSECOND * 1000));
        return getTableColumnsByConn(conn, tableNameWithOwner);
    }

    private static synchronized Connection getConnection(DatabaseType dataBaseType, String url, String user, String pass, String socketTimeout) {
        Properties prop = new Properties();
        prop.put("user", user);
        prop.put("password", pass);

        if (dataBaseType == DatabaseType.Oracle) {
            //oracle.net.READ_TIMEOUT for jdbc versions < 10.1.0.5 oracle.jdbc.ReadTimeout for jdbc versions >=10.1.0.5
            // unit ms
            prop.put("oracle.jdbc.ReadTimeout", socketTimeout);
        }

        return connect(dataBaseType, url, prop);
    }

    private static synchronized Connection connect(DatabaseType dataBaseType,
                                                   String url, Properties prop) {
        try {
            Class.forName(dataBaseType.getDriverClassName());
            DriverManager.setLoginTimeout(TIMEOUT_SECONDS);
            return DriverManager.getConnection(url, prop);
        } catch (SQLException | ClassNotFoundException e) {
            throw new ExtractException(e.getMessage(), e);
        }
    }

    public static List<String> getTableColumnsByConn(Connection conn, String tableNameWithOwner) {
        List<String> columns = new ArrayList<>();
        Statement statement = null;
        ResultSet rs = null;
        String queryColumnSql;
        try {
            statement = conn.createStatement();
            queryColumnSql = String.format("select * from %s where 1=2",
                tableNameWithOwner);
            rs = statement.executeQuery(queryColumnSql);
            ResultSetMetaData rsMetaData = rs.getMetaData();
            for (int i = 0, len = rsMetaData.getColumnCount(); i < len; i++) {
                columns.add(rsMetaData.getColumnName(i + 1));
            }
        } catch (SQLException e) {
            throw new ExtractException(e.getMessage(), e);
        } finally {
            DatabaseUtil.closeDBResources(rs, statement, null);
        }

        return columns;
    }

    public static Triple<List<String>, List<Integer>, List<String>> getColumnMetaData(
        Connection conn, String tableNameWithOwner) {
        Statement statement = null;
        ResultSet rs = null;

        Triple<List<String>, List<Integer>, List<String>> columnMetaData = new ImmutableTriple<List<String>, List<Integer>, List<String>>(
            new ArrayList<>(), new ArrayList<>(),
            new ArrayList<>());
        try {
            statement = conn.createStatement();
            String queryColumnSql = String.format("select * from %s where 1=2", tableNameWithOwner);

            rs = statement.executeQuery(queryColumnSql);
            ResultSetMetaData rsMetaData = rs.getMetaData();
            for (int i = 0, len = rsMetaData.getColumnCount(); i < len; i++) {
                columnMetaData.getLeft().add(rsMetaData.getColumnName(i + 1));
                columnMetaData.getMiddle().add(rsMetaData.getColumnType(i + 1));
                columnMetaData.getRight().add(
                    rsMetaData.getColumnTypeName(i + 1));
            }
            return columnMetaData;
        } catch (SQLException e) {
            throw new ExtractException(String.format("获取表:%s 的字段的元信息时失败. 请联系 DBA 核查该库、表信息.", tableNameWithOwner), e);
        } finally {
            closeDBResources(rs, statement, null);
        }
    }

    public static boolean tableColumnIsDateType(Connection conn, String tableNameWithOwner, String columnName) {
        Statement statement = null;
        ResultSet rs = null;
        String queryColumnSql;
        try {
            statement = conn.createStatement();
            queryColumnSql = String.format("select %s from %s where 1=2",
                columnName, tableNameWithOwner);
            rs = statement.executeQuery(queryColumnSql);
            ResultSetMetaData rsMetaData = rs.getMetaData();
            if (isDateType(rsMetaData.getColumnType(1))) {
                return true;
            }
        } catch (SQLException e) {
            throw new ExtractException(e.getMessage(), e);
        } finally {
            DatabaseUtil.closeDBResources(rs, statement, null);
        }
        return false;
    }

    public static int executeCountSql(Connection conn, String countSql) {
        Statement statement = null;
        ResultSet rs = null;
        int count = 0;
        try {
            statement = conn.createStatement();
            rs = statement.executeQuery(countSql);
            while (rs.next()) {
                count = rs.getInt(1);
            }
            return count;
        } catch (SQLException e) {
            throw new ExtractException(e.getMessage(), e);
        } finally {
            DatabaseUtil.closeDBResources(rs, statement, null);
        }
    }

    private static boolean isDateType(final int type) {
        return Types.DATE == type || Types.TIME == type || Types.TIMESTAMP == type;
    }

    public static void closeDBResources(ResultSet rs, Statement stmt,
                                        Connection conn) {
        if (null != rs) {
            try {
                rs.close();
            } catch (SQLException unused) {
            }
        }

        if (null != stmt) {
            try {
                stmt.close();
            } catch (SQLException unused) {
            }
        }

        if (null != conn) {
            try {
                conn.close();
            } catch (SQLException unused) {
            }
        }
    }

    public static void closeSilently(Connection conn) {
        if (conn == null) {
            return;
        }
        try {
            conn.close();
        } catch (Throwable e) {
            // omit
        }
    }

    public static void closeSilently(Statement statement) {
        if (statement == null) {
            return;
        }
        try {
            statement.close();
        } catch (Throwable e) {
            // omit
        }
    }

    public static void closeSilently(ResultSet rs) {
        if (rs == null) {
            return;
        }
        try {
            rs.close();
        } catch (Throwable e) {
            // omit
        }
    }
}

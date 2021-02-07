package com.shouguouo.extract.database.dialect;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * @author shouguouo~
 * @date 2020/8/21 - 16:14
 */
public interface Dialect {

    String getTableColumnsSql(String tableName);

    Properties getConnectionProperties();

    Properties getPoolProperties();

    String getURL(String hostname, int port, String databaseName);

    String getOwnerTableCombination(String owner, String tableName);

    String getLockTableSql(String tableName);

    String getUnlockTableSql();

    void setFetchSize(Statement statement) throws SQLException;

    String concatToDateSql(int date);

    String surroundKey(String key);
}

package com.shouguouo.extract.database.dialect;


import com.shouguouo.extract.enums.DatabaseType;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * @author shouguouo~
 * @date 2020/8/21 - 16:40
 */
public class MysqlDialect extends AbstractDialect {

    private static final String COLUMN_SQL_FORMATTER = "select column_name from information_schema.COLUMNS where TABLE_SCHEMA = SCHEMA() and upper(TABLE_NAME) = upper('%s')";

    private static final String SURROUND = "`%s`";

    @Override
    public String getTableColumnsSql(String tableName) {
        return String.format(COLUMN_SQL_FORMATTER, tableName);
    }

    @Override
    public String getURL(String hostname, int port, String databaseName) {
        return "jdbc:mysql://" + hostname + ":" + port + "/" + databaseName + "?useSSL=" + getConnectionProperties().getProperty("useSSL", "false");
    }

    @Override
    public String getOwnerTableCombination(String owner, String tableName) {
        return String.format("%s.%s", surroundKey(owner), surroundKey(tableName));
    }

    @Override
    public String surroundKey(String key) {
        return String.format(SURROUND, key.toLowerCase());
    }

    @Override
    public Properties getConnectionProperties() {
        return propertiesMap.get(DatabaseType.Mysql.getPropertiesFileName());
    }

    @Override
    public String getLockTableSql(String tableName) {
        return "LOCK TABLES " + surroundKey(tableName) + " WRITE";
    }

    @Override
    public String getUnlockTableSql() {
        return "UNLOCK TABLES";
    }

    @Override
    public void setFetchSize(Statement statement) throws SQLException {
        statement.setFetchSize(Integer.MIN_VALUE);
    }

    @Override
    public String concatToDateSql(int date) {
        return String.valueOf(date);
    }
}

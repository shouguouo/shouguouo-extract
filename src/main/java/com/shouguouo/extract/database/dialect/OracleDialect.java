package com.shouguouo.extract.database.dialect;



import com.shouguouo.extract.enums.DatabaseType;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * @author shouguouo~
 * @date 2020/8/21 - 16:19
 */
public class OracleDialect extends AbstractDialect {

    private static final String COLUMN_SQL_FORMATTER = "select column_name from user_tab_cols where upper(table_name) = upper('%s')";

    private static final String SURROUND = "\"%s\"";

    @Override
    public String getTableColumnsSql(String tableName) {
        return String.format(COLUMN_SQL_FORMATTER, tableName);
    }

    @Override
    public String getURL(String hostname, int port, String databaseName) {
        if (databaseName.startsWith("/")) {
            return "jdbc:oracle:thin:@//" + hostname + ":" + port + databaseName;
        }
        return "jdbc:oracle:thin:@" + hostname + ":" + port + ":" + databaseName;
    }

    @Override
    public String getOwnerTableCombination(String owner, String tableName) {
        return String.format("%s.%s", surroundKey(owner), surroundKey(tableName));
    }

    @Override
    public String surroundKey(String key) {
        return String.format(SURROUND, key.toUpperCase());
    }

    @Override
    public Properties getConnectionProperties() {
        return propertiesMap.get(DatabaseType.Oracle.getPropertiesFileName());
    }

    @Override
    public String getLockTableSql(String tableName) {
        return "LOCK TABLE " + surroundKey(tableName) + " IN EXCLUSIVE MODE";
    }

    @Override
    public void setFetchSize(Statement statement) throws SQLException {
        statement.setFetchSize(10000);
    }

    @Override
    public String concatToDateSql(int date) {
        return String.format("to_date(%s, 'YYYYMMDD')", date);
    }
}

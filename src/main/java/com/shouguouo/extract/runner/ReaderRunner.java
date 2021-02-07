package com.shouguouo.extract.runner;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Maps;
import com.shouguouo.extract.database.ConnectionPoolUtil;
import com.shouguouo.extract.database.DatabaseMeta;
import com.shouguouo.extract.entity.Record;
import com.shouguouo.extract.entity.TerminatedRecord;
import com.shouguouo.extract.util.DatabaseUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author shouguouo~
 * @date 2020/8/23 - 17:03
 */
public class ReaderRunner extends AbstractRunner {

    private static Logger logger = LogManager.getLogger(ReaderRunner.class);

    public ReaderRunner(String tableName, DatabaseMeta databaseMeta, Exchanger exchanger) {
        super(tableName, databaseMeta, exchanger);
    }

    @Override
    public void run() {
        Stopwatch stopwatch = Stopwatch.createStarted();
        Connection conn = null;
        Statement statement = null;
        ResultSet rs = null;
        try {
            Thread.currentThread().setName(String.format("%s Reader", tableName));
            conn = ConnectionPoolUtil.getConnection(databaseMeta);
            statement = conn.createStatement();

            databaseMeta.setFetchSize(statement);

            String condition = buildConditionSql(conn);
            String countSql = String.format("select count(*) from %s %s", tableNameWithOwner, condition);

            int rowCount = DatabaseUtil.executeCountSql(conn, countSql);
            logger.info("Reader Count: {}", rowCount);
            String sql = String.format("select %s from %s %s", combineColumnWithTrim(conn), tableNameWithOwner, condition);
            logger.debug(sql);
            rs = statement.executeQuery(sql);

            List<String> colList = getColumnList(rs);
            while (rs.next()) {
                Map<String, Object> rsMap = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
                for (String s : colList) {
                    rsMap.put(s, rs.getObject(s));
                }

                exchanger.put(new Record(rsMap));
            }
            exchanger.put(TerminatedRecord.get());
        } catch (Throwable e) {
            exchanger.updateDetailLog(e.getMessage());
            logger.error(e.getMessage(), e);
        } finally {
            DatabaseUtil.closeDBResources(rs, statement, conn);
            stopwatch.stop();
            logger.info("ReaderRunner elapsed : {}", stopwatch.elapsed(TimeUnit.MILLISECONDS)/1000.0);
            latch.countDown();
        }
    }

    private String combineColumnWithTrim(Connection connection) {
        return getTableColumns(connection)
            .stream()
            .map(databaseMeta::surroundKey)
            .collect(Collectors.joining(","));
    }

    private String buildConditionSql(Connection conn) {
        return "";
    }

    private List<String> getColumnList(ResultSet rs) throws SQLException {
        List<String> rsColumn = new ArrayList<>();
        ResultSetMetaData rsMetaData = rs.getMetaData();
        for (int i = 0, len = rsMetaData.getColumnCount(); i < len; i++) {
            rsColumn.add(rsMetaData.getColumnName(i + 1));
        }
        return rsColumn;
    }
}

package com.shouguouo.extract.runner;

import com.google.common.base.Stopwatch;
import com.shouguouo.extract.ExtractStart;
import com.shouguouo.extract.database.ConnectionPoolUtil;
import com.shouguouo.extract.database.DatabaseMeta;
import com.shouguouo.extract.entity.Record;
import com.shouguouo.extract.entity.TerminatedRecord;
import com.shouguouo.extract.enums.ExtractType;
import com.shouguouo.extract.util.DatabaseUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;
import java.sql.*;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


/**
 * @author shouguouo~
 * @date 2020/8/23 - 17:03
 */
public class WriterRunner extends AbstractRunner {

    private static Logger logger = LogManager.getLogger(WriterRunner.class);

    public WriterRunner(String tableName, DatabaseMeta databaseMeta, Exchanger exchanger) {
        super(tableName, databaseMeta, exchanger);
    }

    @Override
    public void run() {
        Stopwatch stopwatch = Stopwatch.createStarted();
        Connection conn = null;
        PreparedStatement preStatement = null;
        try {
            Thread.currentThread().setName(String.format("%s Writer", tableName));
            conn = ConnectionPoolUtil.getConnection(databaseMeta);
            if (ExtractStart.type == ExtractType.ALL) {
                int delSize = cleanTable(conn);
                logger.info("Clean Lines: {}", delSize);
            } else {
                logger.info("Skipping Clean Lines");
            }

            Triple<List<String>, List<Integer>, List<String>> resultSetMetaData = DatabaseUtil.getColumnMetaData(conn, tableNameWithOwner);
            List<String> rsColumn = resultSetMetaData.getLeft();

            conn.setAutoCommit(false);
            String sql = buildInsertSql(rsColumn);
            logger.debug(sql);

            preStatement = conn.prepareStatement(sql);
            databaseMeta.setFetchSize(preStatement);
            Record record;
            int size = 0;

            while ((record = exchanger.take()) != null) {
                if (record instanceof TerminatedRecord) {
                    break;
                }

                // 这里必须指定SQLType 否则在某些情况下OracleDriver不会走批处理导致性能问题。
                // XB抽取接口表结构与源表一致没有类型转换的问题。如需要其他类型的数据可在switch中添加。
                // 如果需要类型转换需自定义数据类型，提供安全的转换方式。
                for (int i = 0, len = rsColumn.size(); i < len; i++) {
                    Object value = record.getRecord().get(rsColumn.get(i));
                    if (value == null || StringUtils.isEmpty(value.toString())) { // 兼容mysql ''处理为null
                        preStatement.setNull(i + 1, resultSetMetaData.getMiddle().get(i));
                    } else if (value instanceof Timestamp) {
                        preStatement.setString(i + 1, timeStampToInt((Timestamp) value).toString());
                    } else {
                        switch (resultSetMetaData.getMiddle().get(i)){
                            case Types.CHAR:
                            case Types.NCHAR:
                            case Types.CLOB:
                            case Types.NCLOB:
                            case Types.VARCHAR:
                            case Types.LONGVARCHAR:
                            case Types.NVARCHAR:
                            case Types.LONGNVARCHAR:
                                preStatement.setString(i + 1, value.toString());
                                break;
                            case Types.SMALLINT:
                            case Types.INTEGER:
                            case Types.BIGINT:
                            case Types.NUMERIC:
                            case Types.DECIMAL:
                            case Types.FLOAT:
                            case Types.REAL:
                            case Types.DOUBLE:
                                preStatement.setBigDecimal(i + 1, new BigDecimal(value.toString()));
                                break;
                            default:
                                preStatement.setObject(i + 1, value, resultSetMetaData.getMiddle().get(i));
                                break;
                        }
                    }
                }

                preStatement.addBatch();
                size++;
                if (size % 10000 == 0) {
                    doInsertBatch(conn, preStatement, size);
                    logger.info("Current Write Lines: {}", size);
                }
            }
            doInsertBatch(conn, preStatement, size);
            logger.info("Total Write Lines: {}", size);
            exchanger.updateDetailLog(null);
        } catch (Throwable e) {
            exchanger.updateDetailLog(e.getMessage());
            logger.error(e.getMessage(), e);
        } finally {
            DatabaseUtil.closeDBResources(null, preStatement, conn);
            stopwatch.stop();
            logger.info("WriterRunner elapsed : {}", stopwatch.elapsed(TimeUnit.MILLISECONDS)/1000.0);
            latch.countDown();
        }

    }

    private Integer timeStampToInt(Timestamp timestamp) {
        return Integer.valueOf(timestamp.toLocalDateTime().toLocalDate().format(DateTimeFormatter.BASIC_ISO_DATE));
    }

    private void doInsertBatch(Connection conn, PreparedStatement preStatement, int size) throws SQLException {
        preStatement.executeBatch();
        conn.commit();
        preStatement.clearBatch();
    }

    public String buildInsertSql(List<String> rsColumn) {
        StringBuilder sb = new StringBuilder(String.format("insert into %s ( %s ) values ",
            tableNameWithOwner,
            rsColumn.stream().map(databaseMeta::surroundKey).collect(Collectors.joining(","))));
        sb.append("(");
        for (int i = 0; i < rsColumn.size(); i++) {
            if ((i + 1) != rsColumn.size()) {
                sb.append("?,");
            } else {
                sb.append("?");
            }
        }
        sb.append(")");
        return sb.toString();
    }

    private int cleanTable(Connection conn) throws SQLException {
        return truncateTable(conn);
    }

    private int truncateTable(Connection conn) throws SQLException {
        int size = 0;
        Statement statement = null;
        ResultSet rs = null;
        try {
            statement = conn.createStatement();
            rs = statement.executeQuery(String.format("select count(*) from %s", tableNameWithOwner));
            if (rs.next()) {
                size = rs.getInt(1);
            }
            String cleanSql = String.format("truncate table %s", tableNameWithOwner);
            statement.executeUpdate(cleanSql);
            return size;
        } finally {
            DatabaseUtil.closeDBResources(rs, statement, null);
        }

    }
}

package com.shouguouo.extract.util;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Oracle含Clob字段的Insert语句导出工具类
 * @author shouguouo~
 * @date 2020/9/30 - 15:23
 */
public class ExportClobUtil {

    public static void export(Connection connection, String tableName, String exportPath) throws SQLException, IOException {
        try (Statement st = connection.createStatement();
             ResultSet rs = st.executeQuery(String.format("select * from %s", tableName))) {
            ResultSetMetaData rsMetaData = rs.getMetaData();

            List<String> columns = new ArrayList<>();
            List<Integer> sqlType = new ArrayList<>();
            for (int i = 0, len = rsMetaData.getColumnCount(); i < len; i++) {
                columns.add(rsMetaData.getColumnName(i + 1));
                sqlType.add(rsMetaData.getColumnType(i + 1));
            }
            String insert = buildInsert(columns, tableName);
            StringBuilder ssb = new StringBuilder();
            while (rs.next()) {
                StringBuilder sb = new StringBuilder(insert);
                sb.append("(");
                for (int i = 0; i < columns.size(); i++) {
                    sb.append(buildValue(rs.getObject(i + 1), sqlType.get(i)));
                    if ((i + 1) != columns.size()) {
                        sb.append(",");
                    }
                }
                sb.append(");");
                ssb.append(sb);
                ssb.append("\n\n");
            }
            write(ssb.toString(), exportPath);
        }
    }

    private static void write(String sb, String exportPath) throws IOException {
        try (OutputStreamWriter outputStream = new OutputStreamWriter(new FileOutputStream(exportPath), StandardCharsets.UTF_8)) {
            sb += "commit;\n";
            outputStream.write(sb);
        }
    }

    private static String buildValue(Object obj, int sqlType) throws IOException, SQLException {
        if (obj == null) {
            return "null";
        }
        if (obj instanceof oracle.sql.TIMESTAMP) {
            return "null";
        }
        if (obj instanceof Number) {
            return obj.toString();
        } else if (sqlType == Types.CLOB) {
            return buildClob(clobToString((Clob) obj));
        } else {
            return "'" + obj.toString().replaceAll("'", "''") + "'";
        }
    }

    public static String clobToString(Clob clob) throws SQLException, IOException {
        String reString = "";
        Reader is = clob.getCharacterStream();
        BufferedReader br = new BufferedReader(is);
        String s = br.readLine();
        StringBuilder sb = new StringBuilder();
        while (s != null) {
            sb.append(s);
            s = br.readLine();
        }
        reString = sb.toString();
        br.close();
        is.close();
        return reString;
    }

    private static String buildClob(String sb) {
        int maxLength = 2000;
        int len = sb.length();
        StringBuilder builder = new StringBuilder();
        int pos = 0;
        if (len <= maxLength) {
            builder.append("'").append(sb.replaceAll("'", "''")).append("'");

        } else {
            while (pos < len) {
                builder.append("to_clob(").append("'");
                if (pos + maxLength > len) {
                    builder.append(sb.substring(pos).replaceAll("'", "''"));
                } else {
                    builder.append(sb.substring(pos, pos + maxLength).replaceAll("'", "''"));
                }
                builder.append("')");
                pos += maxLength;
                if (pos < len) {
                    builder.append(" || ");
                }
                builder.append("\n");
            }
        }
        return builder.toString();
    }

    private static String buildInsert(List<String> columns, String tableName) {
        return String.format("insert into %s ( %s ) values\n", tableName, String.join(",", columns));
    }
}

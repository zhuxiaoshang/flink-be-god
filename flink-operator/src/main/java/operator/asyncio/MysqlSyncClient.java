package operator.asyncio;

import java.sql.*;

/**
 * mysql 同步查询
 * sysout最好用日志处理
 * @param <T>
 */
public class MysqlSyncClient<T> {
    private static transient Connection connection;
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String URL = "jdbc:mysql://xxxx";
    private static final String USER = "xxxx";
    private static final String PASSWORD = "xxxx";

    static {
        init();
    }

    private static void init() {
        try {
            Class.forName(JDBC_DRIVER);
        } catch (ClassNotFoundException e) {
            System.out.println("Driver not found!" + e.getMessage());
        }
        try {
            connection = DriverManager.getConnection(URL, USER,
                    PASSWORD);
        } catch (SQLException e) {
            System.out.println("init connection failed!" + e.getMessage());
        }
    }

    public void close() {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            System.out.println("close connection failed!" + e.getMessage());
        }
    }

    public T query(T t)  {
        StoreInfo info = (StoreInfo) t;
        try{
        Statement statement = connection.createStatement();
        ResultSet resultSet =
                statement.executeQuery("select str_cd,crf_str_cd,crf_str_nm from t_shp_crf_str_cfg_ed_a where str_cd " +
                        "='" + info.getStrCd()+"';");
        if (resultSet != null && resultSet.next()) {
            info.setStrNm(resultSet.getString(3));
        }}
        catch (SQLException e ){
            System.out.println("query failed!"+e.getMessage());
        }
        return t;
    }


}

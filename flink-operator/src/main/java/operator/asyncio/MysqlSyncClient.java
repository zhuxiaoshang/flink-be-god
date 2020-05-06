package operator.asyncio;

import java.sql.*;

/**
 * mysql 同步查询
 * sysout最好用日志处理
 *
 * @param <T>
 */
public class MysqlSyncClient<T> {
    private static transient Connection connection;
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String URL = "jdbc:mysql://localhost:3306/flink";
    private static final String USER = "root";
    private static final String PASSWORD = "123456";

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

    public T query(T t) {
        CategoryInfo info = (CategoryInfo) t;
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet =
                    statement.executeQuery("select sub_category_id,parent_category_id from category where " +
                            "sub_category_id " +
                            "='" + info.getSubCategoryId() + "';");
            if (resultSet != null && resultSet.next()) {
                info.setParentCategoryId(resultSet.getLong(2));
            }
        } catch (SQLException e) {
            System.out.println("query failed!" + e.getMessage());
        }
        return t;
    }


}

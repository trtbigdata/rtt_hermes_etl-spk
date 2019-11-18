package com.hupu.hermes.utils;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidDataSourceFactory;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * @author yanjiajun
 */
public class DBUtil {
    public static Logger logger = LoggerFactory.getLogger(DBUtil.class);

    private static ThreadLocal<Connection> connectionThreadLocal = new ThreadLocal<>();
    private static DruidDataSource druidDataSource = null;

    static {
        Properties properties = loadPropertiesFile(Constants.JDBC_CONF_FILE);
        try {
            druidDataSource = (DruidDataSource)DruidDataSourceFactory.createDataSource(properties);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    // 获取一个jdbc的连接池
    public static Connection getConnection() {
        // 这边是先拿出来一个connection，如果没有就在下面给他赋值一个connection
        Connection connection = connectionThreadLocal.get();
        try {
            if (null == connection) {
                connection = druidDataSource.getConnection();
                // 给第一次数据库设置连接池
                connectionThreadLocal.set(connection);
            }
        } catch (SQLException e) {
            logger.error(e.getMessage());
        }
        return connection;
    }

    // 关闭连接
    public static void closeConnection() {
        Connection connection = connectionThreadLocal.get();
        if (null != connection) {
            try {
                connection.close();
                connectionThreadLocal.remove();
            } catch (SQLException e) {
                logger.error(e.getMessage());
            }
        }
    }

    // 这里设置不要自动提交sql命令，如果设置为false，那么需要我们手动去commit
    public static void startTransaction() {
        Connection conn = connectionThreadLocal.get();

        try {
            if (conn == null) {
                conn = getConnection();
                connectionThreadLocal.set(conn);
            }
            conn.setAutoCommit(false);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    // 提交命令
    public static void commit() {
        try {
            Connection conn = connectionThreadLocal.get();
            if (null != conn) {
                conn.commit();
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    // 设置回滚方法，这个方法是在如果提交不成功，而你又设置成不自动提交，那么表就会被锁住，所以需要进行回滚
    public static void rollback() {
        try {
            Connection conn = connectionThreadLocal.get();
            if (conn != null) {
                conn.rollback();
                connectionThreadLocal.remove();
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    // 加载数据库连接配置文件
    private static Properties loadPropertiesFile(String file) {
        if (StringUtils.isEmpty(file)) {
            throw new IllegalArgumentException("Properties file path can not be null" + file);
        }
        Properties prop = new Properties();
        try {
            prop.load(DBUtil.class.getClassLoader().getResourceAsStream(file));
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        return prop;
    }

    // 更新操作
    public static void saveOrUpdate(String sql, Object... params) {
        QueryRunner runner = new QueryRunner(druidDataSource, true);
        try {
            runner.update(sql, params);
        } catch (Exception e) {
            logger.error("更新失败sql:" + sql, e);
        }
    }

    // 查询操作
    public static Object query(String sql, ResultSetHandler resultHandler) {
        QueryRunner queryRunner = new QueryRunner();
        Connection connection = getConnection();
        Object object = null;
        try {
            object = queryRunner.query(connection, sql, resultHandler);
        } catch (Exception e) {
            logger.error("query failed...", e);
        } finally {
            DBUtil.closeConnection();
        }
        return object;
    }
}

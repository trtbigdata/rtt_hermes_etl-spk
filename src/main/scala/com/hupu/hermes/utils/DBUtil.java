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

    public static Connection getConnection() {
        Connection connection = connectionThreadLocal.get();
        try {
            if (null == connection) {
                connection = druidDataSource.getConnection();
                connectionThreadLocal.set(connection);
            }
        } catch (SQLException e) {
            logger.error(e.getMessage());
        }
        return connection;
    }

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

    public static void saveOrUpdate(String sql, Object... params) {
        QueryRunner runner = new QueryRunner(druidDataSource, true);
        try {
            runner.update(sql, params);
        } catch (Exception e) {
            logger.error("更新失败sql:" + sql, e);
        }
    }

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

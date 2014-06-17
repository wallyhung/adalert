package com.alert.config;

import com.mysql.jdbc.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.Properties;

/**
 * @author wally
 * @version 1.0
 * @email hongwei200612@gmail.com
 * @created 2014/6/16 10:53
 * @description
 */
public class Config {
    private final static Logger     logger     = LoggerFactory.getLogger(Config.class);
    private static       Config     config     = null;
    private static       Properties properties = new Properties();

    public synchronized static Config getInstance() {

        if (config == null) {
            config = new Config();
        }
        return config;
    }

    protected Connection getConnection() {
        Connection connection = null;
        try {
            properties.load(Config.class.getResourceAsStream("/application.properties"));
            Class.forName(properties.getProperty("spring.datasource.driverClassName"));
            connection = DriverManager.getConnection(properties.getProperty("spring.datasource.url"), properties.getProperty("spring.datasource.username"), properties.getProperty("spring.datasource.password"));
        } catch (IOException e) {
            logger.error("数据库配置文件找不到...");
        } catch (ClassNotFoundException e) {
            logger.error("类找不到：{}", e.getMessage());
        } catch (SQLException e) {
        	logger.error("数据库连接失败：{}",e.getMessage());
        }
        return connection;
    }

    //关闭连接
    public void close(Connection connection) {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            logger.error("database close fail:{}", e.getMessage());
        }
    }

    //关闭连接
    public void closeAll(ResultSet rs, PreparedStatement st, Connection connection) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
            }
        }
        if (st != null) {
            try {
                st.close();
            } catch (SQLException e) {
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
            }
        }
    }

    public String[] getMailAdds() {
        try {
            properties.load(Config.class.getResourceAsStream("/application.properties"));
            String adds = properties.getProperty("mail.adds");
            if(StringUtils.isNullOrEmpty(adds)) return null;
            else return adds.split(",");
        } catch (IOException e) {
            logger.error("加载邮件接受人失败...");
        }
        return null;
    }


}

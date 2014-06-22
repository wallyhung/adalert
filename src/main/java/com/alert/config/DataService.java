package com.alert.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

/**
 * @author wally
 * @version 1.0
 * @email hongwei200612@gmail.com
 * @created 2014/6/16 13:32
 * @description
 */
public class DataService {
    private static final Logger logger = LoggerFactory.getLogger(DataService.class);
    private static final Config config = Config.getInstance();
    private PreparedStatement preparedStatement;
    private ResultSet         resultSet;

    public boolean hasdata() {
        boolean flag = false;
        String hour = TimeUtil.getDayLastHour(new Date());
        Connection connection = config.getConnection();
        try {
            preparedStatement = connection.prepareStatement("SELECT COUNT(*) FROM ad_hour_report_new t WHERE t.`hour` = '" + hour + "'");
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                if (resultSet.getInt(1) > 0) flag = true;
            }
        } catch (SQLException e) {
            logger.error("数据库查询失败...");
        } finally {
            config.closeAll(resultSet, preparedStatement, connection);
        }
        return flag;
    }

    public Integer[] validateHour() {
        Integer[] res = new Integer[6];
        String hour = TimeUtil.getDayLastHour(new Date());
        Connection connection = config.getConnection();
        try {
            preparedStatement = connection.prepareStatement("SELECT SUM(t.`request`) AS request,SUM(t.`push`) AS push,SUM(t.`view`) AS 'view',SUM(t.`cpc`+t.`c_oth`+t.`c_wall`) AS click,SUM(t.`d_oth`+t.`d_wall`) AS download,SUM(t.`i_oth`+t.`i_wall`) AS 'install' FROM app_hour_report_new t WHERE t.`hour` = '" + hour + "'");
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                res[0] = resultSet.getInt("request");
                res[1] = resultSet.getInt("push");
                res[2] = resultSet.getInt("view");
                res[3] = resultSet.getInt("click");
                res[4] = resultSet.getInt("download");
                res[5] = resultSet.getInt("install");
            }
            preparedStatement = connection.prepareStatement("SELECT SUM(t.`push`) AS push ,SUM(t.`view`) AS 'view' ,SUM(t.`click`) AS click,SUM(t.`d_oth`+t.`d_wall`) AS download,SUM(t.`i_oth`+t.`i_wall`) AS 'install' FROM ad_hour_report_new t WHERE t.`hour` = '" + hour + "'");
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                res[1] = res[1] == resultSet.getInt("push") ? res[1] : -1;
                res[2] = res[2] == resultSet.getInt("view") ? res[2] : -1;
                res[3] = res[3] == resultSet.getInt("click") ? res[3] : -1;
                res[4] = res[4] == resultSet.getInt("download") ? res[4] : -1;
                res[5] = res[5] == resultSet.getInt("install") ? res[5] : -1;
            }

        } catch (SQLException e) {
            logger.error("数据库查询失败...");
        } finally {
            config.closeAll(resultSet, preparedStatement, connection);
        }
        return res;
    }
    
    public Integer[] validate() {
        Integer[] res = new Integer[6];
        String day = TimeUtil.getDayLastHour(new Date()).substring(0,10);
        Connection connection = config.getConnection();
        try {
            preparedStatement = connection.prepareStatement("SELECT SUM(t.`request`) AS request,SUM(t.`push`) AS push,SUM(t.`view`) AS 'view',SUM(t.`cpc`+t.`c_oth`+t.`c_wall`) AS click,SUM(t.`d_oth`+t.`d_wall`) AS download,SUM(t.`i_oth`+t.`i_wall`) AS 'install' FROM app_hour_report_new t WHERE t.`hour` LIKE '" + day + "%'");
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                res[0] = resultSet.getInt("request");
                res[1] = resultSet.getInt("push");
                res[2] = resultSet.getInt("view");
                res[3] = resultSet.getInt("click");
                res[4] = resultSet.getInt("download");
                res[5] = resultSet.getInt("install");
            }
        } catch (SQLException e) {
            logger.error("数据库查询失败...");
        } finally {
            config.closeAll(resultSet, preparedStatement, connection);
        }
        return res;
    }

    public void sendMail() {
        String[] adds = config.getMailAdds();
        String subject = "广告统计";

        DataService dataService = new DataService();
        Integer[] size = dataService.validateHour();
        StringBuilder sb = new StringBuilder();
        sb.append(TimeUtil.getDayLastHour(new Date())).append("小时统计结果：").append(dataService.hasdata()).append("\n");
        sb.append("请求数：" + size[0] + ";推送数：" + size[1] + ";展示数：" + size[2] + ";点击数：" + size[3] + ";下载数：" + size[4] + ";安装数：" + size[5]).append("\n");

        size = dataService.validate();
        sb.append("截止目前，总统计数据：").append("\n");
        sb.append("请求数：" + size[0] + ";推送数：" + size[1] + ";展示数：" + size[2] + ";点击数：" + size[3] + ";下载数：" + size[4] + ";安装数：" + size[5]);
        try {
            Mail.sendMail(adds, subject, sb.toString());
        } catch (Exception e) {
            logger.error("发送邮件失败...");
        }
    }
}

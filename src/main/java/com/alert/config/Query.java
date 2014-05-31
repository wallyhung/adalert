package com.alert.config;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Query {
	 private static final Logger logger = LoggerFactory.getLogger(Query.class);
	    private static final Config      config = Config.getInstance();
	    private static PreparedStatement preparedStatement;
	    private static ResultSet         resultSet;
	    
	    private static Integer[] executeHql(String sql,int res_size) {
	        Connection connection = config.getConnection();
	        Integer[] res = new Integer[res_size];
	        try {
	            preparedStatement = connection.prepareStatement(sql);
	            resultSet = preparedStatement.executeQuery();
	            while (resultSet.next()) {
 	            	for (int i = 0; i < res.length; i++) {
 						res[i] = resultSet.getInt(i+1);
 					}
 	            }
	           
	        } catch (SQLException e) {
	            logger.error("数据库查询失败...");
	        } finally {
	            config.closeAll(resultSet, preparedStatement, connection);
	        }
	        return res;
	    }
	    
	    private static List<Integer[]> executeMultHql(String sql,int res_size) {
	        Connection connection = config.getConnection();
	        List<Integer[]> list = new ArrayList<Integer[]>();
	        try {
	            preparedStatement = connection.prepareStatement(sql);
	            resultSet = preparedStatement.executeQuery();
	            while (resultSet.next()) {
	            	Integer[] res = new Integer[res_size];
 	            	for (int i = 0; i < res.length; i++) {
 						res[i] = resultSet.getInt(i+1);
 					}
 	            	list.add(res);
 	            }
	           
	        } catch (SQLException e) {
	            logger.error("数据库查询失败...");
	        } finally {
	            config.closeAll(resultSet, preparedStatement, connection);
	        }
	        return list;
	    }
	    
	    private static String getMailContent()
	    {
	    	String hour = TimeUtil.getDayLastHour(new Date());
	    	StringBuilder sb = new StringBuilder();
	    	StringBuilder hql = new StringBuilder();
	    	
	    	sb.append("<style type='text/css'>");
	    	sb.append("table.gridtable {font-family: verdana,arial,sans-serif;	font-size:11px;	color:#333333;	border-width: 1px;	border-color: #666666;	border-collapse: collapse;}");
	    	sb.append("table.gridtable th {border-width: 1px;	padding: 8px;	border-style: solid; border-color: #666666;	background-color: #dedede;}");
	    	sb.append("table.gridtable td {border-width: 1px;	padding: 8px;	border-style: solid;	border-color: #666666;	background-color: #ffffff;text-align: center;}");
	    	sb.append("</style>");
	    	
	    	sb.append(TimeUtil.getDayLastHourStr(new Date())).append("-").append(TimeUtil.getHour(new Date())).append("小时统计结果：");
	    	hql.append("SELECT COUNT(*) FROM ad_hour_report_new t WHERE t.`hour` = '").append(hour).append("'");
	    	Integer[] res = executeHql(hql.toString(), 1);
	    	if(res[0] > 0) sb.append(true);
	    	else sb.append(false);
	    	sb.append("。<span style='color:red;'>注意：值为-1表明该小时两个维度统计的结果不一致</span>。");
	    	sb.append("\n");
	    	
	    	hql = new StringBuilder();
	    	hql.append("SELECT SUM(t.`request`) AS request,SUM(t.`push`) AS push,SUM(t.`view`) AS 'view',SUM(t.`cpc`+ t.`c_wall`) AS click,");
	    	hql.append("SUM(t.`c_wall`) AS cpa,SUM(t.`cpc`) AS cpc,");
	    	hql.append("SUM(t.`d_wall`+t.`d_oth`) AS download, SUM(t.`d_wall`) AS d_wall,SUM(t.`d_oth`) AS d_oth,");
	    	hql.append("SUM(t.`i_wall`+t.`i_oth`) AS 'install',SUM(t.`i_wall`) AS i_wall,SUM(t.`i_oth`) AS i_oth,");
	    	hql.append("SUM(t.`new_u`) AS new_u,SUM(t.`remain`) AS remain,MAX(t.`all_req`) AS al_req, MAX(t.`all_remain`) AS a_remian ");
	    	hql.append("FROM app_hour_report_new t WHERE t.`hour`='");
	    	hql.append(hour);
	    	hql.append("'");
	    	res = executeHql(hql.toString(), 16);
	    	
	    	hql = new StringBuilder();
	    	hql.append("SELECT 0,SUM(t.`push`) AS push,SUM(t.`view`) AS 'view',SUM(t.`click`) AS click,0,0,");
	    	hql.append("SUM(t.`d_wall`+t.`d_oth`) AS download, SUM(t.`d_wall`) AS d_wall,SUM(t.`d_oth`) AS d_oth,");
	    	hql.append("SUM(t.`i_wall`+t.`i_oth`) AS 'install',SUM(t.`i_wall`) AS i_wall,SUM(t.`i_oth`) AS i_oth,0,SUM(t.`remain`) AS remain,0,0 ");
	    	hql.append("FROM ad_hour_report_new t WHERE t.`hour`='");
	    	hql.append(hour);
	    	hql.append("' ");
	    	Integer[] adres = executeHql(hql.toString(), 16);
	    	
	    	res[1] = res[1].intValue() == adres[1].intValue() ? res[1] : -1;
	    	res[2] = res[2].intValue() == adres[2].intValue() ? res[2] : -1;
	    	res[3] = res[3].intValue() == adres[3].intValue() ? res[3] : -1;
	    	res[6] = res[6].intValue() == adres[6].intValue() ? res[6] : -1;
	    	res[9] = res[9].intValue() == adres[9].intValue() ? res[9] : -1;
	    	sb.append("<table class='gridtable'>");
	    	sb.append("<tr><th rowspan='2'>请求数</th><th rowspan='2'>推送数</th><th rowspan='2'>展示数</th><th colspan='2'>点击数</th><th colspan='2'>下载数</th><th colspan='2'>安装数</th><th rowspan='2'>新增用户</th><th rowspan='2'>留存用户</th></tr>");
	    	sb.append("<tr><th>CPA</th><th>CPC</th><th>墙下载</th><th>非墙下载</th><th>墙安装</th><th>非墙安装</th></tr>");
	    	sb.append("<tr><td>");
	    	sb.append(res[0]).append("</td><td>");
	    	sb.append(res[1]).append("</td><td>");
	    	sb.append(res[2]).append("</td><td>");
	    	sb.append(res[4]).append("</td><td>");
	    	sb.append(res[5]).append("</td><td>");
	    	sb.append(res[7]).append("</td><td>");
	    	sb.append(res[8]).append("</td><td>");
	    	sb.append(res[10]).append("</td><td>");
	    	sb.append(res[11]).append("</td><td>");
	    	sb.append(res[12]).append("</td><td>");
	    	sb.append(res[13]);
	    	sb.append("</td></tr></table>");
            sb.append("<br><br>");
            sb.append("截止到").append(TimeUtil.getHour(new Date())).append("，总统计结果：\n");
            
            
            hql = new StringBuilder();
	    	hql.append("SELECT SUM(t.`request`) AS request,SUM(t.`push`) AS push,SUM(t.`view`) AS 'view',SUM(t.`cpc`+ t.`c_wall`) AS click,");
	    	hql.append("SUM(t.`c_wall`) AS cpa,SUM(t.`cpc`) AS cpc,");
	    	hql.append("SUM(t.`d_wall`+t.`d_oth`) AS download, SUM(t.`d_wall`) AS d_wall,SUM(t.`d_oth`) AS d_oth,");
	    	hql.append("SUM(t.`i_wall`+t.`i_oth`) AS 'install',SUM(t.`i_wall`) AS i_wall,SUM(t.`i_oth`) AS i_oth ");
	    	hql.append("FROM app_hour_report_new t WHERE t.`hour` LIKE '");
	    	hql.append(hour.substring(0,10));
	    	hql.append("%'");
	    	
	    	res = executeHql(hql.toString(), 12);
            sb.append("\n");
            sb.append("<table class='gridtable'>");
	    	sb.append("<tr><th rowspan='2'>请求数</th><th rowspan='2'>推送数</th><th rowspan='2'>展示数</th><th colspan='2'>点击数</th><th colspan='2'>下载数</th><th colspan='2'>安装数</th>");
	    	sb.append("<tr><th>CPA</th><th>CPC</th><th>墙下载</th><th>非墙下载</th><th>墙安装</th><th>非墙安装</th></tr>");
	    	sb.append("<tr><td>");
	    	sb.append(res[0]).append("</td><td>");
	    	sb.append(res[1]).append("</td><td>");
	    	sb.append(res[2]).append("</td><td>");
	    	sb.append(res[4]).append("</td><td>");
	    	sb.append(res[5]).append("</td><td>");
	    	sb.append(res[7]).append("</td><td>");
	    	sb.append(res[8]).append("</td><td>");
	    	sb.append(res[10]).append("</td><td>");
	    	sb.append(res[11]);
	    	sb.append("</td></tr></table>");
            sb.append("<br><br>");
            
            String day = TimeUtil.getDay(TimeUtil.getLastDay(new Date()));
	    	hql = new StringBuilder();
	    	hql.append("SELECT t.`new_a`,t.`push`,t.`view`,t.`click`,SUM(t.`d_wall`+t.`d_wall`),SUM(t.`i_wall`+t.`i_oth`),t.`alive` ");
	    	hql.append("FROM day_sum t WHERE t.`day` = '");
	    	hql.append(day);
	    	hql.append("'");
	    	
	    	res = executeHql(hql.toString(), 7);
	    	sb.append("昨日统计结果：");
	    	sb.append("\n");
            sb.append("<table class='gridtable'>");
	    	sb.append("<tr><th>推送数</th><th>展示数</th><th>点击数</th><th>下载数</th><th>安装数</th><th>终端数</th>");
	    	sb.append("<tr><td>");
	    	sb.append(res[1]).append("</td><td>");
	    	sb.append(res[2]).append("</td><td>");
	    	sb.append(res[3]).append("</td><td>");
	    	sb.append(res[4]).append("</td><td>");
	    	sb.append(res[5]).append("</td><td>");
	    	sb.append(res[6]);
	    	sb.append("</td></tr></table>");
            sb.append("<br><br>");

            hql = new StringBuilder();
 	    	hql.append("SELECT t.`adid`+0,t.`push`,SUM(t.`d_oth`+t.`d_wall`),SUM(t.`i_oth`+t.`i_wall`),t.`re_view`,t.`re_click`,SUM(t.`re_d_oth`+t.`re_d_wall`),SUM(t.`re_i_oth`+t.`re_i_wall`),t.`remain` FROM ad_hour_report_new t ");
 	    	hql.append("WHERE t.`hour` = '");
 	    	hql.append(hour);
 	    	hql.append("' AND t.`re_view` <> '0' GROUP BY t.`adid` ORDER BY t.`adid`");
 	    	System.out.println(hql.toString());
 	    	List<Integer[]> list = executeMultHql(hql.toString(), 9);
	    	if(list.size() > 0)
	    	{
	    		sb.append(TimeUtil.getDayLastHourStr(new Date())).append("-").append(TimeUtil.getHour(new Date())).append("小时补推的广告效果：\n");
	            sb.append("<table class='gridtable'>");
		    	sb.append("<tr><th>广告ID</th><th>推送数</th><th>推送安装</th><th>补推展示</th><th>补推点击</th><th>补推下载</th><th>补推安装</th><th>终端数</th>");
		    	for (Integer[] integers : list) {
		    		sb.append("<tr><td>");
			    	sb.append(integers[0]).append("</td><td>");
			    	sb.append(integers[1]).append("</td><td>");
			    	sb.append(integers[3]).append("</td><td>");
			    	sb.append(integers[4]).append("</td><td>");
			    	sb.append(integers[5]).append("</td><td>");
			    	sb.append(integers[6]).append("</td><td>");
			    	sb.append(integers[7]).append("</td><td>");
			    	sb.append(integers[8]);
			    	sb.append("</td></tr>");
				}
		    	sb.append("</table>");
	    	}
            
	    	return sb.toString();
	    }
	    
	    public static void sendMail() {
	        String[] adds = config.getMailAdds();
	        String subject = "广告统计";
	        try {
	            Mail.sendHtmlMail(adds, subject, getMailContent());
	        } catch (Exception e) {
	            logger.error("发送邮件失败...");
	        }
	    }
	    
	    public static void main(String[] args) {
//	    	getMailContent();
			sendMail();
		}

}

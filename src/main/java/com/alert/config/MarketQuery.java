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

public class MarketQuery {
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
	    	sb.append(".wr{font-family:verdana,arial,sans-serif;font-size:11px;color:#333333;font-weight: bold;}");
	    	sb.append("table.gridtable {font-family: verdana,arial,sans-serif;	font-size:11px;	color:#333333;	border-width: 1px;	border-color: #666666;	border-collapse: collapse;}");
	    	sb.append("table.gridtable th {border-width: 1px;	padding: 8px;	border-style: solid; border-color: #666666;	background-color: #dedede;}");
	    	sb.append("table.gridtable td {border-width: 1px;	padding: 8px;	border-style: solid;	border-color: #666666;	background-color: #ffffff;text-align: center;}");
	    	sb.append("</style>");
	    	
	    	//判断该小时是否进行了小时统计
	    	sb.append(TimeUtil.getDayLastHourStr(new Date())).append("-").append(TimeUtil.getHour(new Date())).append("小时统计结果：");
	    	hql.append("SELECT COUNT(*) FROM ad_hour_report_new t WHERE t.`hour` = '").append(hour).append("'");
	    	Integer[] res = executeHql(hql.toString(), 1);
	    	if(res[0] > 0) sb.append(true);
	    	else sb.append(false);
	    	sb.append("。\n");
	    	
	    	//获取该小时的应用统计概况
            hql = new StringBuilder();
	    	hql.append("SELECT SUM(t.`request`) AS request,SUM(t.`push`) AS push,SUM(t.`view`) AS 'view',SUM(t.`cpc`+ t.`c_wall`) AS click,");
	    	hql.append("SUM(t.`c_wall`) AS cpa,SUM(t.`cpc`) AS cpc,");
	    	hql.append("SUM(t.`d_wall`+t.`d_oth`) AS download, SUM(t.`d_wall`) AS d_wall,SUM(t.`d_oth`) AS d_oth,");
	    	hql.append("SUM(t.`i_wall`+t.`i_oth`) AS 'install',SUM(t.`i_wall`) AS i_wall,SUM(t.`i_oth`) AS i_oth ");
	    	hql.append("FROM app_hour_report_new t WHERE t.`hour` = '");
	    	hql.append(hour);
	    	hql.append("'");
	    	
	    	res = executeHql(hql.toString(), 12);
            sb.append("\n");
            sb.append("<table class='gridtable'>");
	    	sb.append("<tr><th rowspan='2'>总请求<br />(含补推)</th><th rowspan='2'>总推送<br />(含补推)</th><th rowspan='2'>常规展示</th><th colspan='2'>点击累计</th><th colspan='2'>下载累计</th><th colspan='2'>安装累计</th><th rowspan='2' style='color:red;'>下载总计</th><th rowspan='2' style='color:red;'>安装总计</th>");
	    	sb.append("<tr><th>CPA</th><th>CPC</th><th>墙</th><th>非墙</th><th>墙</th><th>非墙</th></tr>");
	    	sb.append("<tr><td>");
	    	sb.append(res[0]).append("</td><td>");
	    	sb.append(res[1]).append("</td><td>");
	    	sb.append(res[2]).append("</td><td>");
	    	sb.append(res[4]).append("</td><td>");
	    	sb.append(res[5]).append("</td><td>");
	    	sb.append(res[7]).append("</td><td>");
	    	sb.append(res[8]).append("</td><td>");
	    	sb.append(res[10]).append("</td><td>");
	    	sb.append(res[11]).append("</td><td style='color:red;'>");
	    	sb.append(res[6]).append("</td><td style='color:red;'>");
	    	sb.append(res[9]);
	    	sb.append("</td></tr>");
	    	
            //获取补推的累计效果 
	    	String h = hour.substring(hour.length()-2,hour.length()); //2014-07-18-01
	    	int hint = Integer.parseInt(h);
	    	int push = 0;
	    	if(hint > 18)
	    	{
	    		hql = new StringBuilder();
	            hql.append("SELECT SUM(t.`push`) AS push FROM  ad_hour_report_new t WHERE t.`hour` = '");
	            hql.append(hour);
	            hql.append("' AND t.`re_view` <> 0");
	            res = executeHql(hql.toString(), 1);
	            push = res[0];
	    	}
            
            
            hql = new StringBuilder();
	    	hql.append("SELECT SUM(t.`re_view`) AS 'view',SUM(t.`re_click`) AS click,");
	    	hql.append("SUM(t.`re_d_wall`),SUM(t.`re_d_oth`),SUM(t.`re_d_oth`+t.`re_d_wall`),");
	    	hql.append("SUM(t.`re_i_wall`),SUM(t.`re_i_oth`),SUM(t.`re_i_oth`+t.`re_i_wall`)");
	    	hql.append("FROM ad_hour_report_new t WHERE t.`hour` = '");
	    	hql.append(hour);
	    	hql.append("'");
	    	
	    	res = executeHql(hql.toString(), 8);
	    	sb.append("<tr><th rowspan='2'></th><th rowspan='2'>补推数</th><th rowspan='2'>补推展示</th><th colspan='2'>补推点击</th><th colspan='2'>补推下载</th><th colspan='2'>补推安装</th><th rowspan='2' style='color:red;'>补推<br />下载总计</th><th rowspan='2' style='color:red;'>补推<br />安装总计</th>");
	    	sb.append("<tr><th>CPA</th><th>CPC</th><th>墙</th><th>非墙</th><th>墙</th><th>非墙</th></tr>");
	    	sb.append("<tr><td></td><td>");
	    	sb.append(push).append("</td><td>");
	    	sb.append(res[0]).append("</td><td>");
	    	sb.append(res[1]).append("</td><td>");
	    	sb.append("-").append("</td><td>");
	    	sb.append(res[2]).append("</td><td>");
	    	sb.append(res[3]).append("</td><td>");
	    	sb.append(res[5]).append("</td><td>");
	    	sb.append(res[6]).append("</td><td style='color:red;'>");
	    	sb.append(res[4]).append("</td><td style='color:red;'>");
	    	sb.append(res[7]);
	    	sb.append("</td></tr></table>");
            
            //获取补推广告情况
//            hql = new StringBuilder();
// 	    	hql.append("SELECT t.`adid`+0,t.`push`,SUM(t.`d_oth`+t.`d_wall`),SUM(t.`i_oth`+t.`i_wall`),t.`re_view`,t.`re_click`,t.`re_d_wall`,t.`re_d_oth`,SUM(t.`re_d_oth`+t.`re_d_wall`),t.`re_i_wall`,t.`re_i_oth`,SUM(t.`re_i_oth`+t.`re_i_wall`) FROM ad_hour_report_new t ");
// 	    	hql.append("WHERE t.`hour` = '");
// 	    	hql.append(hour);
// 	    	hql.append("' AND t.`re_view` <> '0' GROUP BY t.`adid` ORDER BY t.`adid`");
// 	    	List<Integer[]> list = executeMultHql(hql.toString(), 12);
//	    	if(list.size() > 0)
//	    	{
//	    		sb.append("补推广告效果：\n");
//	            sb.append("<table class='gridtable'>");
//		    	sb.append("<tr><th rowspan='2'>补推ADID</th><th rowspan='2'>补推数</th><th rowspan='2'>补推展示</th><th colspan='2'>补推点击</th><th colspan='2'>补推下载数</th><th colspan='2'>补推安装数</th>");
//		    	sb.append("<tr><th>CPA</th><th>CPC</th><th>墙</th><th>非墙</th><th>墙</th><th>非墙</th></tr>");
//		    	for (Integer[] integers : list) {
//		    		sb.append("<tr><td>");
//			    	sb.append(integers[0]).append("</td><td>");
//			    	sb.append(integers[1]).append("</td><td>");
//			    	sb.append(integers[4]).append("</td><td>");
//			    	sb.append(integers[5]).append("</td><td>");
//			    	sb.append("-").append("</td><td>");
//			    	sb.append(integers[6]).append("</td><td>");
//			    	sb.append(integers[7]).append("</td><td>");
//			    	sb.append(integers[9]).append("</td><td>");
//			    	sb.append(integers[10]);
//			    	sb.append("</td></tr>");
//				}
//		    	sb.append("</table>");
//	    	}
	    	
	    	//获取今日累计的推送效果
            sb.append("今日截止到").append(TimeUtil.getHour(new Date())).append("，累计：\n");
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
	    	sb.append("<tr><th rowspan='2'>总请求<br />(含补推)</th><th rowspan='2'>总推送<br />(含补推)</th><th rowspan='2'>常规展示</th><th colspan='2'>点击累计</th><th colspan='2'>下载累计</th><th colspan='2'>安装累计</th><th rowspan='2' style='color:red;'>下载总计</th><th rowspan='2' style='color:red;'>安装总计</th>");
	    	sb.append("<tr><th>CPA</th><th>CPC</th><th>墙</th><th>非墙</th><th>墙</th><th>非墙</th></tr>");
	    	sb.append("<tr><td>");
	    	sb.append(res[0]).append("</td><td>");
	    	sb.append(res[1]).append("</td><td>");
	    	sb.append(res[2]).append("</td><td>");
	    	sb.append(res[4]).append("</td><td>");
	    	sb.append(res[5]).append("</td><td>");
	    	sb.append(res[7]).append("</td><td>");
	    	sb.append(res[8]).append("</td><td>");
	    	sb.append(res[10]).append("</td><td>");
	    	sb.append(res[11]).append("</td><td style='color:red;'>");
	    	sb.append(res[6]).append("</td><td style='color:red;'>");
	    	sb.append(res[9]);
	    	sb.append("</td></tr>");
	    	
            //获取补推的累计效果
	    	if(hint > 18)
	    	{
	    		hql = new StringBuilder();
	            hql.append("SELECT SUM(t.`push`) AS push FROM  ad_hour_report_new t WHERE t.`hour` LIKE '");
	            hql.append(hour.substring(0,10));
	            hql.append("%' AND t.`re_view` <> 0");
	            res = executeHql(hql.toString(), 1);
	            push = res[0];
	    	}
            
            hql = new StringBuilder();
	    	hql.append("SELECT SUM(t.`re_view`) AS 'view',SUM(t.`re_click`) AS click,");
	    	hql.append("SUM(t.`re_d_wall`),SUM(t.`re_d_oth`),SUM(t.`re_d_oth`+t.`re_d_wall`),");
	    	hql.append("SUM(t.`re_i_wall`),SUM(t.`re_i_oth`),SUM(t.`re_i_oth`+t.`re_i_wall`)");
	    	hql.append("FROM ad_hour_report_new t WHERE t.`hour` LIKE '");
	    	hql.append(hour.substring(0,10));
	    	hql.append("%'");
	    	
	    	res = executeHql(hql.toString(), 8);
	    	sb.append("<tr><th rowspan='2'></th><th rowspan='2'>补推数</th><th rowspan='2'>补推展示</th><th colspan='2'>补推点击</th><th colspan='2'>补推下载</th><th colspan='2'>补推安装</th><th rowspan='2' style='color:red;'>补推<br />下载总计</th><th rowspan='2' style='color:red;'>补推<br />安装总计</th>");
	    	sb.append("<tr><th>CPA</th><th>CPC</th><th>墙</th><th>非墙</th><th>墙</th><th>非墙</th></tr>");
	    	sb.append("<tr><td></td><td>");
	    	sb.append(push).append("</td><td>");
	    	sb.append(res[0]).append("</td><td>");
	    	sb.append(res[1]).append("</td><td>");
	    	sb.append("-").append("</td><td>");
	    	sb.append(res[2]).append("</td><td>");
	    	sb.append(res[3]).append("</td><td>");
	    	sb.append(res[5]).append("</td><td>");
	    	sb.append(res[6]).append("</td><td style='color:red;'>");
	    	sb.append(res[4]).append("</td><td style='color:red;'>");
	    	sb.append(res[7]);
	    	sb.append("</td></tr></table>");
            
            //获取昨日统计累计
            String day = TimeUtil.getDay(TimeUtil.getLastDay(new Date()));
	    	hql = new StringBuilder();
	    	hql.append("SELECT SUM(t.`push`),SUM(t.`remain`) AS push FROM  ad_day_report_new t WHERE t.`day` = '");
	    	hql.append(day);
            hql.append("' AND t.`re_view` <> 0");
	    	res = executeHql(hql.toString(), 2);
	    	
	    	hql = new StringBuilder();
	    	hql.append("SELECT t.`push`,t.`view`,t.`click`,SUM(t.`d_oth`+t.`d_wall`),SUM(t.`i_oth`+t.`i_wall`),t.`alive` FROM day_sum t WHERE t.`day` = '");
	    	hql.append(day).append("' UNION ALL ");
	    	hql.append("SELECT 0,SUM(t.`re_view`),SUM(t.`re_click`),SUM(t.`re_d_oth`+t.`re_d_wall`),SUM(t.`re_i_oth`+t.`re_i_wall`),0 ");
	    	hql.append("FROM ad_day_report_new t WHERE t.day = '");
	    	hql.append(day).append("' UNION ALL ");
	    	hql.append("SELECT SUM(t.`push`),SUM(t.`view`+t.`re_view`),SUM(t.`click`+t.`re_click`),SUM(t.`d_oth`+t.`d_wall`+t.`re_d_oth`+t.`re_d_wall`),SUM(t.`i_oth`+t.`i_wall`+t.`re_i_oth`+t.`re_i_wall`),0 ");
	    	hql.append("FROM ad_day_report_new t WHERE t.day = '");
	    	hql.append(day);
	    	hql.append("'");
	    	List<Integer[]> list = executeMultHql(hql.toString(), 6);
	    	
            if(list.size() > 0)
	    	{
            	sb.append("昨日统计累计：\n");
	            sb.append("<table class='gridtable'>");
	            sb.append("<tr><th>推送类别</th><th>推送数</th><th>展示数</th><th>点击数</th><th>下载数</th><th>安装数</th><th>终端数</th>");
	            int sum = list.get(0)[0];
	            int rsum = list.get(0)[5];
	            for (int i = 0; i < list.size(); i++) {
	            	Integer[] integers = list.get(i);
	            	sb.append("<tr><td class='wr'>");
			    	sb.append(getType(i)).append("</td><td>");
			    	sb.append(getPush(i, sum, res[0])).append("</td><td>");
			    	sb.append(integers[1]).append("</td><td>");
			    	sb.append(integers[2]).append("</td><td>");
			    	sb.append(integers[3]).append("</td><td style='color:red;'>");
			    	sb.append(integers[4]).append("</td><td>");
			    	sb.append(getRemain(i, rsum, res[1]));
			    	sb.append("</td></tr>");
				}
		    	sb.append("</table>");
	    	}
	    	return sb.toString();
	    }
	    
	    private static int getPush(int i,int sum,int re)
	    {
	    	switch (i) {
			case 0:
				return sum-re;
			case 1:
				return re;
			case 2:
				return sum;
			default:
				return 0;
			}
	    }
	    
	    private static String getRemain(int i,int sum,int re)
	    {
	    	switch (i) {
			case 0:
				return String.valueOf(sum);
			case 1:
				return "-";
			case 2:
				return String.valueOf(sum);
			default:
				return "-";
			}
	    }
	    
	    private static String getType(int i)
	    {
	    	switch (i) {
			case 0:
				return "推送";
			case 1:
				return "补推";
			case 2:
				return "合计";
			default:
				return "";
			}
	    }
	    
	    public static void sendMail() {
	        String[] adds = config.getMarketMailAdds();
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

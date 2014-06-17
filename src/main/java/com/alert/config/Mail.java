package com.alert.config;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.io.UnsupportedEncodingException;
import java.util.Properties;

/**
 * @author wally
 * @version 1.0
 * @email hongwei200612@gmail.com
 * @created 2014/6/16 15:55
 * @description
 */
public class Mail {

    public static void sendMail(String[] adds, String subject, String msgBody) throws UnsupportedEncodingException, MessagingException {
        final String username = "jukuadsupport";
        final String password = "cnmmdcdts";

        Properties props = new Properties();
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.host", "114.255.157.20");
        props.put("mail.smtp.port", "25");
        props.put("mail.transport.protocol", "smtp");


        Session session = Session.getInstance(props,
                                              new javax.mail.Authenticator() {
                                                  @Override
                                                  protected javax.mail.PasswordAuthentication getPasswordAuthentication() {
                                                      return new javax.mail.PasswordAuthentication(username, password);
                                                  }
                                              }
        );
        session.setDebug(true);
        Message msg = new MimeMessage(session);
        msg.setFrom(new InternetAddress("jukuadsupport@jukuad.com", "飓酷广告统计平台"));
        Address[] as = null;
        if(adds != null && adds.length > 0)
        {
        	as = new InternetAddress[adds.length];
            for (int i = 0; i < adds.length ; i++) {
                as[i] = new InternetAddress(adds[i],"监控者");
            }
        }
        else{
        	as = new InternetAddress[1];
        	as[0] = new InternetAddress("363306725@qq.com","开发者");
        }
        msg.addRecipients(Message.RecipientType.TO,as);
        msg.setSubject(subject);
        msg.setText(msgBody);
        Transport.send(msg);
    }

}

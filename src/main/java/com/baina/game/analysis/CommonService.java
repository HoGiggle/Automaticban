package com.baina.game.analysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.log4j.Logger;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by jjhu on 2015/3/16.
 */
public class CommonService {
    private static final Logger LOGGER = Logger.getLogger(CommonService.class);

    public static Configuration getConf(){
        SimpleDateFormat sdf = new SimpleDateFormat("8.0 yyyy-MM-dd HH:mm:ss");
        sdf.setTimeZone(TimeZone.getTimeZone("GMT+8"));
        Date nowD = new Date();
        int secLimit = 1800;  //30分钟

        String now = sdf.format(nowD);
        String before = sdf.format(nowD.getTime() - secLimit*2*1000l);
        String mid = sdf.format(nowD.getTime() - secLimit*1000l);

        Configuration conf = new Configuration();
        conf.set("date", now);
        conf.set("beforeTime", before);
        conf.set("bmidTime", mid);
        conf.set("hbase.master", "10.168.184.97:60000");
        conf.set("hbase.zookeeper.quorum", "10.168.179.98:2181,10.168.182.202:2181,10.168.184.7:2181");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set(TableOutputFormat.OUTPUT_TABLE, "automatic_ban");
        return conf;
    }

    public static Connection getConnection(){
        // JDBC driver name and database URL
        final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
        final String DB_URL = "jdbc:mysql://10.135.49.45/accountdb";

        //  Database credentials
        final String USER = "globalserver";
        final String PASS = "Fmr9v2U";

        Connection conn = null;
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }


    /**
     * get create date, convert to days which are away from 1970-01-01 at GMT+8 TimeZone.
     * @param pst
     * @param userId
     * @return days between cr_date and 1970-01-01
     */
    public static int getCrDateStamp(PreparedStatement pst, int userId){
        ResultSet rs = null;
        int stamp = 0;
        try {
            pst.setInt(1, userId);
            rs = pst.executeQuery();
            if (rs.next()){
                stamp = (rs.getInt(1) + 8*60*60) / (60*60*24);
            }
        } catch (SQLException e) {
            LOGGER.info("CommonService--->getCrDateStamp() get rs failed");
            e.printStackTrace();
        } finally {
            if (rs != null){
                try {
                    rs.close();
                } catch (SQLException e) {
                    LOGGER.info("CommonService--->getCrDateStamp() close rs failed");
                    e.printStackTrace();
                }
            }
        }
        return stamp;
    }

    public static int getDateStamp(int userId){
        Connection conn = CommonService.getConnection();
        PreparedStatement pst = null;
        ResultSet rs = null;
        int table = userId % 16;
        int stamp = 0;
        try {
            pst = conn.prepareStatement("select t.cr_date from a_acc_data_"+ table +" t where t.acc_id=?");
            pst.setInt(1, userId);
            rs = pst.executeQuery();
            if (rs.next()){
                stamp = (rs.getInt(1) + 8*60*60) / (60*60*24);   //转化为东八区天数，代表日期
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (rs != null){
                try {
                    rs.close();
                } catch (SQLException e) {
                    LOGGER.info("CommonService---->getDateStamp():resultSet close failed");
                    e.printStackTrace();
                }
            }
            if (pst != null){
                try {
                    pst.close();
                } catch (SQLException e) {
                    LOGGER.info("CommonService---->getDateStamp:preparedStatement close failed");
                    e.printStackTrace();
                }
            }
            if (conn != null){
                try {
                    conn.close();
                } catch (SQLException e) {
                    LOGGER.info("CommonService---->getDateStamp:connection close failed");
                    e.printStackTrace();
                }
            }

        }
        return stamp;
    }

    public static String getPassword(int userId){
        Connection conn = CommonService.getConnection();
        PreparedStatement pst = null;
        ResultSet rs = null;
        int tables = 16;   //分表数
        String pass = null;

        try {
            for (int i = 0; i < tables; i++){
                pst = conn.prepareStatement("select t.user_str from a_acc_account_account_"+ i +" t where t.acc_id=?");
                pst.setInt(1, userId);
                rs = pst.executeQuery();
                if (rs.next()){
                    pass = rs.getString(1);
                    return pass;
                }
            }
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (rs != null){
                try {
                    rs.close();
                } catch (SQLException e) {
                    LOGGER.info("CommonService---->getDateStamp():resultSet close failed");
                    e.printStackTrace();
                }
            }
            if (pst != null){
                try {
                    pst.close();
                } catch (SQLException e) {
                    LOGGER.info("CommonService---->getDateStamp:preparedStatement close failed");
                    e.printStackTrace();
                }
            }
            if (conn != null){
                try {
                    conn.close();
                } catch (SQLException e) {
                    LOGGER.info("CommonService---->getDateStamp:connection close failed");
                    e.printStackTrace();
                }
            }
        }
        return pass;
    }

    public static boolean playedCaishenOrWanren(Connection conn, int userId){
        LOGGER.info("played---> I'm coming");

        PreparedStatement pst = null;
        ResultSet rs = null;
        int table = userId % 16;
        try {
            pst = conn.prepareStatement("select * from c_property_"+ table +" t where t.acc_id=? and (t.prop_id=1005 or t.prop_id=1002)");
            pst.setInt(1, userId);
            rs = pst.executeQuery();
            if (rs.next()){
                return true;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                rs.close();
            } catch (SQLException e) {
                LOGGER.info("CommonService---->playedCaishenOrWanren():resultSet close failed");
                e.printStackTrace();
            }

            try {
                pst.close();
            } catch (SQLException e) {
                LOGGER.info("CommonService---->playedCaishenOrWanren():preparedStatement  close failed");
                e.printStackTrace();
            }
        }
        return false;
    }

    public static int getWanrenTimes(PreparedStatement pst, int uid){
        ResultSet rs = null;
        int wanrenTimes = 0;
        try {
            pst.setInt(1, uid);
            rs = pst.executeQuery();
            if (rs.next()){
                wanrenTimes = rs.getInt(1);
            }
        } catch (SQLException e) {
            LOGGER.info("CommonService--->getWanrenTimes() get rs failed");
            e.printStackTrace();
        } finally {
            if (rs != null){
                try {
                    rs.close();
                } catch (SQLException e) {
                    LOGGER.info("CommonService--->getWanrenTimes() close rs failed");
                    e.printStackTrace();
                }
            }
        }
        return wanrenTimes;
    }

    public static int getMMBRecharge(PreparedStatement pst, int uid){
        ResultSet rs = null;
        int recharge = 0;
        try {
            pst.setInt(1, uid);
            rs = pst.executeQuery();
            if (rs.next()){
                recharge = rs.getInt(1);
            }
        } catch (SQLException e) {
            LOGGER.info("CommonService--->getMMBRecharge() get rs failed");
            e.printStackTrace();
        } finally {
            if (rs != null){
                try {
                    rs.close();
                } catch (SQLException e) {
                    LOGGER.info("CommonService--->getWanrenTimes() close rs failed");
                    e.printStackTrace();
                }
            }
        }
        return recharge;
    }

    public static boolean isJoinedAnyActivity(PreparedStatement pst, int uid){
        ResultSet rs = null;
        boolean result = false;
        try {
            pst.setInt(1, uid);
            rs = pst.executeQuery();
            if (rs.next()){
                result = true;
            }
        } catch (SQLException e) {
            LOGGER.info("CommonService--->isJoinedAnyActivity() get rs failed");
            e.printStackTrace();
        } finally {
            if (rs != null){
                try {
                    rs.close();
                } catch (SQLException e) {
                    LOGGER.info("CommonService--->isJoinedAnyActivity() close rs failed");
                    e.printStackTrace();
                }
            }
        }
        return result;
    }


    public static boolean isMobileCard(String imsi){

        if (imsi == null || imsi.length() == 0)
            return false;

        /*Logger logger = Logger.getLogger(CommonService.class);
        try {
            String MNC = imsi.substring(3, 5);
            if (MNC.equals("00") || MNC.equals("02")) //移动运营商标识MNC标识00、02
                return true;
            else
                return false;
        }catch (Exception e){
            logger.info("jjhu---->" + imsi);
            e.printStackTrace();
        }
        return false;*/

        String MNC = imsi.substring(3, 5);
        if (MNC.equals("00") || MNC.equals("02")) //移动运营商标识MNC标识00、02
            return true;
        else
            return false;
    }

    public static boolean isTrueImsi(String imsi){
        if (imsi == null || imsi.length() == 0)
            return false;

        if (imsi.length() == 15)
            return true;
        else
            return false;
    }
}

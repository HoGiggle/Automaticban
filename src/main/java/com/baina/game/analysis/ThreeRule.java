package com.baina.game.analysis;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by jjhu on 2015/3/27.
 */
public class ThreeRule {

    private static final Logger LOGGER = Logger.getLogger(ThreeRule.class);

    public static class ThreeRuleMapper extends Mapper<Object, Text, Text, Text>{
        private int logLength;
        private int timeLocal;
        private int uidLocal;
        private int outIpLocal;
        private int playedLocal;
        private int caishenLocal;
        private int rechargeLocal;

        private int playedLimit;
        private int caishenLimit;
        private int wanrenLimit;
        private String startTime;
        private String endTime;

        private Connection conn;
        private Map<Integer, PreparedStatement> rechargePst_map;
        private Map<Integer, PreparedStatement> wanrenPst_map;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            this.logLength = conf.getInt("logLength", 24);
            this.timeLocal = conf.getInt("timeLocal", 0);
            this.uidLocal = conf.getInt("uidLocal", 12);
            this.outIpLocal = conf.getInt("outIpLocal", 11);
            this.playedLocal = conf.getInt("playedLocal", 13);
            this.caishenLocal = conf.getInt("caishenLocal", 23);
            this.rechargeLocal = conf.getInt("rechargeLocal", 17);

            this.playedLimit = conf.getInt("playedLimit", 25);
            this.caishenLimit = conf.getInt("caishenLimit", 0);
            this.wanrenLimit = conf.getInt("wanrenLimit", 0);
            this.startTime = conf.get("startTime");
            this.endTime = conf.get("endTime");

            this.conn = CommonService.getConnection();
            this.rechargePst_map = new HashMap<>();
            this.wanrenPst_map = new HashMap<>();
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String []items = value.toString().split("\\|");

            //日志长度、牌局次数、财神次数筛选
            if (items.length != this.logLength)
                return;

            if (items[this.timeLocal].compareTo(this.startTime) < 0 || items[this.timeLocal].compareTo(this.endTime) > 0)
                return;

            if (Integer.parseInt(items[this.playedLocal]) > this.playedLimit)
                return;

            if (Integer.parseInt(items[this.caishenLocal]) > this.caishenLimit)
                return;


            //财神、万人场次数筛选
            int uid = Integer.parseInt(items[this.uidLocal]);
            int table = uid % 16;

            if (wanrenPst_map.containsKey(table)){
                if (CommonService.isPlayedCaiOrWanren(wanrenPst_map.get(table), uid, this.caishenLimit, this.wanrenLimit))
                    return;
            }else {
                PreparedStatement pst = null;
                try {
                    pst = this.conn.prepareStatement("select * from c_property_"
                            + table +" t where t.acc_id=? and ((t.prop_id=1005 and t.amount>?) or (t.prop_id=1002 and t.amount>?))");
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                if (pst != null){
                    wanrenPst_map.put(table, pst);
                    if (CommonService.isPlayedCaiOrWanren(wanrenPst_map.get(table), uid, this.caishenLimit, this.wanrenLimit))
                        return;
                }
            }

            LOGGER.info("map-->   uid:" + uid + "  Ip:" + items[this.outIpLocal]);
            //ip校验，且不能为0.0.0.0
            Pattern p = Pattern.compile("^(25[0-5]|2[0-4]\\d|[0-1]?\\d?\\d)(\\.(25[0-5]|2[0-4]\\d|[0-1]?\\d?\\d)){3}$");
            Matcher m = p.matcher(items[this.outIpLocal]);
            if (!m.find() || items[this.outIpLocal].equals("0.0.0.0"))
                return;

            //是否全部为话费充值筛选
            if (rechargePst_map.containsKey(table)){
                if (CommonService.getMMBRecharge(rechargePst_map.get(table), uid) != Integer.parseInt(items[this.rechargeLocal]))
                    return;
            }else {
                PreparedStatement pst = null;
                try {
                    pst = this.conn.prepareStatement("select t.amount from c_property_"
                            + table +" t where t.acc_id=? and t.prop_id=1003");
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                if (pst != null){
                    rechargePst_map.put(table, pst);
                    if (CommonService.getMMBRecharge(rechargePst_map.get(table), uid) != Integer.parseInt(items[this.rechargeLocal]))
                        return;
                }
            }

            //write the data we need.
            StringBuilder sb = new StringBuilder();
            sb.append(items[this.outIpLocal]);
            sb.append("|");
            sb.append(items[this.rechargeLocal]);
            context.write(new Text(sb.toString()), new Text(items[this.uidLocal]));
            LOGGER.info("map--->     key:" + sb.toString() + "value:" + uid);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (PreparedStatement pst : wanrenPst_map.values()){
                if (pst != null){
                    try {
                        pst.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }

            for (PreparedStatement pst : rechargePst_map.values()){
                if (pst != null){
                    try {
                        pst.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }

            if (this.conn != null){
                try {
                    this.conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static class ThreeRuleReducer extends TableReducer<Text, Text, NullWritable>{
        private Configuration conf;
        private String game;
        private String date;
        private int uidLimit;

        private Connection conn;
        private Map<Integer, PreparedStatement> crDate_map;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.conf = context.getConfiguration();
            this.game = this.conf.get("game", "CARD");
            this.date = this.conf.get("date");
            this.uidLimit = this.conf.getInt("uidLimit", 10);

            this.conn = CommonService.getConnection();
            this.crDate_map = new HashMap<>();
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<Integer> uidSet = new HashSet<>();
            for (Text value :values){                      //去除重复user id
                uidSet.add(Integer.parseInt(value.toString()));
            }

            LOGGER.info("reduce->size:" + uidSet.size());

            if (uidSet.size() < this.uidLimit)            //同一ip和充值金额下账号数量 < 账号数量限制, return
                return;

            Map<Integer, List<Integer>> crdateAndUid_map = new HashMap<>();

            // 遍历同一ip和充值金额下的所有uid, 查询uid的创建时间,
            // 并将创建时间（距离1970-01-01的天数）做为key, 相同创建时间下的所有uid作为value, 维护crdateAndUid_map.
            for (int uid : uidSet){
                int table = uid % 16;
                int crDateStamp = -1;       //区分CommonService.getCrDateStamp()返回的默认值0
                if (this.crDate_map.containsKey(table)){
                    crDateStamp = CommonService.getCrDateStamp(this.crDate_map.get(table), uid);
                }else {
                    PreparedStatement pst = null;
                    try {
                        pst = this.conn.prepareStatement("select t.cr_date from a_acc_data_"+ table +" t where t.acc_id=?");
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                    if (pst != null){
                        this.crDate_map.put(table, pst);
                        crDateStamp = CommonService.getCrDateStamp(pst, uid);
                    }
                }

                if (crDateStamp > 0){          //维护crdateAndUid_map map, 同一创建日期下的所有uid
                    if (crdateAndUid_map.containsKey(crDateStamp)){
                        crdateAndUid_map.get(crDateStamp).add(uid);
                    }else {
                        List<Integer> uidList = new ArrayList<>();
                        uidList.add(uid);
                        crdateAndUid_map.put(crDateStamp, uidList);
                    }
                }
            }


            //遍历crdateAndUid_map, 如果同一创建时间下的uid size >= uidLimit, 则将此类所有的uid标记为满足规则3, 存入hbase
            Set<Map.Entry<Integer, List<Integer>>> entries = crdateAndUid_map.entrySet();
            for (Map.Entry<Integer, List<Integer>> entry : entries){
                List<Integer> uids = entry.getValue();

                LOGGER.info("reduce-->size:" + uids.size());

                if (uids.size() >= this.uidLimit){
                    for (int uid : uids){
                        /** get from 'automatic_ban' hbase table
                         */
                        String rowkey = this.game + "|" + uid;
                        HTable hTable = new HTable(this.conf, "automatic_ban");
                        Get get = new Get(Bytes.toBytes(rowkey));
                        get.addColumn(Bytes.toBytes("details"), Bytes.toBytes("reason"));
                        Result result = hTable.get(get);

                        /** parse jsonArray to object list
                         */
                        List<RuleResultCell> list = new ArrayList<>();
                        if (!result.isEmpty()){
                            //list = JSON.parseArray(new String(result.value()), RuleResultCell.class);
                            list = JSON.parseObject(new String(result.value()), new TypeReference<List<RuleResultCell>>() {
                            });
                        }

                        /** parse object list to jsonArray string, and put to hbase.
                         */
                        RuleResultCell cell = new RuleResultCell();
                        cell.setDate(this.date);
                        cell.setBanType(3);
                        cell.setBannedKey(key.toString() + "|" + entry.getKey());  //保存规则3下ip、充值金额、创建日期
                        list.add(cell);
                        String jsonResult = JSON.toJSONString(list);
                        Put put = new Put(Bytes.toBytes(rowkey));
                        put.add(Bytes.toBytes("details"), Bytes.toBytes("reason"), Bytes.toBytes(jsonResult));
                        context.write(NullWritable.get(), put);
                    }
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (PreparedStatement pst : this.crDate_map.values()){
                if (pst != null){
                    try {
                        pst.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }

            try {
                this.conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat sdf1 = new SimpleDateFormat("8.0 yyyy-MM-dd HH:mm:ss");
        sdf1.setTimeZone(TimeZone.getTimeZone("GMT+8"));
        sdf.setTimeZone(TimeZone.getTimeZone("GMT+8"));
        String today = sdf.format(new Date());

        Date now = new Date();
        String endTime = sdf1.format(now);
        String startTime = sdf1.format(now.getTime() - 12*60*60*1000l);

        Configuration conf = CommonService.getConf();
        conf.set("startTime", startTime);
        conf.set("endTime", endTime);
        Job job = new Job(conf, "ThreeRuleJob");
        job.setJarByClass(ThreeRule.class);
        job.setMapperClass(ThreeRuleMapper.class);
        job.setReducerClass(ThreeRuleReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputFormatClass(TableOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("/data/rawlog/card/login/" + today));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}

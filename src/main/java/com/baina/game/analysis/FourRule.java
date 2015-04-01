package com.baina.game.analysis;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
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
 * Created by jjhu on 2015/3/18.
 */
public class FourRule {

    private static final Logger LOGGER = Logger.getLogger(FourRule.class);

    public static class FourRuleMapper extends Mapper<Object, Text, Text, IntWritable>{

        private int logLength;
        private int uidLocal;
        private int outIpLocal;
        private int timeLocal;
        private int caishenLocal;

        private String startTime;
        private String endTime;
        private int caishenLimit;


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            this.logLength = conf.getInt("logLength", 24);
            this.uidLocal = conf.getInt("uidLocal", 12);
            this.outIpLocal = conf.getInt("outIpLocal", 11);
            this.timeLocal = conf.getInt("timeLocal", 0);
            this.caishenLocal = conf.getInt("caishenLocal", 23);

            this.startTime = conf.get("startTime");
            this.endTime = conf.get("endTime");
            this.caishenLimit = conf.getInt("caishenLimit", 0);
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String []items = value.toString().split("\\|", -1);

            if (items.length != this.logLength)
                return;
            if (items[this.timeLocal].compareTo(this.startTime) < 0 || items[this.timeLocal].compareTo(this.endTime) > 0)
                return;
            if (Integer.parseInt(items[this.caishenLocal]) > this.caishenLimit)
                return;

            LOGGER.info("map->   id:" + key.toString() + "   cai:" + items[this.caishenLocal]);
            LOGGER.info("map-->   id:" + key.toString() + "   ip:" + items[this.outIpLocal]);

            //匹配ip地址，外网ip地址为空抛弃，0.0.0.0本地地址抛弃
            Pattern p = Pattern.compile("^(25[0-5]|2[0-4]\\d|[0-1]?\\d?\\d)(\\.(25[0-5]|2[0-4]\\d|[0-1]?\\d?\\d)){3}$");
            Matcher m = p.matcher(items[this.outIpLocal]);
            if (!m.find() || items[this.outIpLocal].equals("0.0.0.0"))
                return;

            LOGGER.info("map--->  key" + items[this.outIpLocal] + "  value:" + items[this.uidLocal]);
            context.write(new Text(items[this.outIpLocal]), new IntWritable(Integer.parseInt(items[this.uidLocal])));
        }
    }

    public static class FourRuleReducer extends TableReducer<Text, IntWritable, NullWritable>{
        private Configuration conf;
        private String game;
        private String date;
        private int uidLimit;
//        private int similarity;
        private int caishenLimit;
        private int wanrenLimit;

        private Connection conn;
        private Map<Integer, PreparedStatement> wanrenPst_map;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.conf = context.getConfiguration();
            this.game = this.conf.get("game", "CARD");
            this.date = this.conf.get("date");
            this.uidLimit = this.conf.getInt("uidLimit", 10);
//            this.similarity = this.conf.getInt("similarity", 2);
            this.caishenLimit = conf.getInt("caishenLimit", 0);
            this.wanrenLimit = conf.getInt("wanrenLimit", 0);

            this.conn = CommonService.getConnection();
            this.wanrenPst_map = new HashMap<>();
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            Map<String, Set<Integer>> datePwd_uid_map = new HashMap<>();
            Set<Integer> allIdSet = new HashSet<>();
            Set<Integer> trueIdSet = new HashSet<>();

            for (IntWritable value : values){
                allIdSet.add(value.get());
            }
            LOGGER.info("reduce->size:" + allIdSet.size());
            if (allIdSet.size() < this.uidLimit)                     //reduce端筛选
                return;

            for (int uid : allIdSet){
                //财神、万人场次数筛选
                int table = uid % 16;

                if (wanrenPst_map.containsKey(table)){
                    if (!CommonService.isPlayedCaiOrWanren(wanrenPst_map.get(table), uid, this.caishenLimit, this.wanrenLimit))
                        trueIdSet.add(uid);
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
                        if (!CommonService.isPlayedCaiOrWanren(wanrenPst_map.get(table), uid, this.caishenLimit, this.wanrenLimit))
                            trueIdSet.add(uid);
                    }
                }
            }
            LOGGER.info("reduce->trueSize:" + trueIdSet.size());
            if (trueIdSet.size() < this.uidLimit)                         //再一次reduce筛选
                return;

            for (int uid : allIdSet){
                String pass = CommonService.getPassword(uid);
                LOGGER.info("reduce-->id:" + uid + "  pass:" + pass);
                if (pass != null){
                    int crStamp = CommonService.getDateStamp(uid);

                    LOGGER.info("reduce--->id:" + uid + "  stamp:" + crStamp);

                    StringBuilder sb = new StringBuilder();
                    sb.append(crStamp);
                    sb.append("|");
                    sb.append(pass);

                    String passAndDate = sb.toString();
                    if (datePwd_uid_map.containsKey(passAndDate)){
                        datePwd_uid_map.get(passAndDate).add(uid);
                    }else {
                        Set<Integer> uidSet = new HashSet<>();
                        uidSet.add(uid);
                        datePwd_uid_map.put(passAndDate, uidSet);
                    }
                }
            }

            Set<Map.Entry<String, Set<Integer>>> entries = datePwd_uid_map.entrySet();
            for (Map.Entry<String, Set<Integer>> entry : entries){
                Set<Integer> uids = entry.getValue();

                LOGGER.info("reduce---->same crdateAndPass size:" + uids.size());

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
                        cell.setBanType(4);
                        cell.setBannedKey(key.toString() + "|" + entry.getKey());  //保存规则4下ip、创建日期、密码
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
            for (PreparedStatement pst : wanrenPst_map.values()){
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
        Job job = new Job(conf, "FourRuleJob");
        job.setJarByClass(FourRule.class);
        job.setMapperClass(FourRuleMapper.class);
        job.setReducerClass(FourRuleReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TableOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path("/data/rawlog/card/login/" + today));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

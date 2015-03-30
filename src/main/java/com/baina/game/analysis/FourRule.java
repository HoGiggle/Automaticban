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
        private Connection conn;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            this.logLength = conf.getInt("logLength", 24);
            this.uidLocal = conf.getInt("uidLocal", 12);
            this.outIpLocal = conf.getInt("outIpLocal", 11);
            this.conn = CommonService.getConnection();
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String []items = value.toString().split("\\|", -1);
            if (items.length != this.logLength)
                return;

            //匹配ip地址，外网ip地址为空抛弃，0.0.0.0本地地址抛弃
            Pattern p = Pattern.compile("^(25[0-5]|2[0-4]\\d|[0-1]?\\d?\\d)(\\.(25[0-5]|2[0-4]\\d|[0-1]?\\d?\\d)){3}$");
            Matcher m = p.matcher(items[this.outIpLocal]);
            LOGGER.info("map--->" + items[this.outIpLocal]);
            if (!m.find() || items[this.outIpLocal].equals("0.0.0.0"))
                return;
            LOGGER.info("map--->ip is right " + items[this.outIpLocal]);

            /*if (items[this.outIpLocal].length() == 0 || items[this.outIpLocal].equals("0.0.0.0"))
                return;*/

            //玩过万人场或者财神抛弃
            if (CommonService.playedCaishenOrWanren(this.conn, Integer.parseInt(items[this.uidLocal])))
                return;

            LOGGER.info("map---->write " + items[this.uidLocal]);
            context.write(new Text(items[this.outIpLocal]), new IntWritable(Integer.parseInt(items[this.uidLocal])));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            try {
                this.conn.close();
            } catch (SQLException e) {
                LOGGER.info("map----> connection close failed");
                e.printStackTrace();
            }
        }
    }

    public static class FourRuleReducer extends TableReducer<Text, IntWritable, NullWritable>{
        private Configuration conf;
        private String game;
        private String date;
        private int uidLimit;
        private int similarity;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.conf = context.getConfiguration();
            this.game = this.conf.get("game", "CARD");
            this.date = this.conf.get("date");
            this.uidLimit = this.conf.getInt("uidLimit", 10);
            this.similarity = this.conf.getInt("similarity", 2);
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            Map<String, Set<Integer>> datePwd_uid_map = new HashMap<>();
            for (IntWritable value : values){
                int uid = value.get();
                String pass = CommonService.getPassword(uid);
                LOGGER.info("reduce--->" + uid + "-----" + pass);
                if (pass != null){
                    int crStamp = CommonService.getDateStamp(uid);
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
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = CommonService.getConf();
        Job job = new Job(conf, "FourruleJob");
        job.setJarByClass(FourRule.class);
        job.setMapperClass(FourRuleMapper.class);
        job.setReducerClass(FourRuleReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TableOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path("/data/rawlog/card/login/2015-03-24"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

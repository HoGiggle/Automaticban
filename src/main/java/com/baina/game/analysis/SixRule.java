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
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by jjhu on 2015/3/17.
 */
public class SixRule {
    public static class SixRuleMapper extends Mapper<Object, Text, Text, Text>{
        private int logLength;
        private int uidLocal;
        private int imsiLocal;
        private int moneyLocal;
        private int payTypeLocal;
        private String payType;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            this.logLength = conf.getInt("logLength", 27);
            this.uidLocal = conf.getInt("uidLocal", 11);
            this.imsiLocal = conf.getInt("imsiLocal", 9);
            this.moneyLocal = conf.getInt("moneyLocal", 22);
            this.payTypeLocal = conf.getInt("payTypeLocal", 18);
            this.payType = conf.get("payType", "billing");
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String []items = value.toString().split("\\|");
            if (items.length != this.logLength && items.length != 28)       //日志有点问题，前期日志长度为28
                return;

            try {
                Long.parseLong(items[4]);
                if (items[this.payTypeLocal + 1].toLowerCase().endsWith(this.payType)){
                    StringBuilder sb = new StringBuilder();
                    sb.append(items[0]);   //date
                    sb.append("|");
                    sb.append(items[this.imsiLocal + 1]);
                    sb.append("|");
                    sb.append(items[this.moneyLocal + 1]);
                    context.write(new Text(items[this.uidLocal + 1]), new Text(sb.toString()));
                }
                return;
            }catch (Exception e){
                e.printStackTrace();
            }

            if (items[this.payTypeLocal].toLowerCase().endsWith(this.payType)){
                StringBuilder sb = new StringBuilder();
                sb.append(items[0]);   //date
                sb.append("|");
                sb.append(items[this.imsiLocal]);
                sb.append("|");
                sb.append(items[this.moneyLocal]);
                context.write(new Text(items[this.uidLocal]), new Text(sb.toString()));
            }
        }
    }

    public static class SixRulePartitioner extends Partitioner<Text, Text>{
        @Override
        public int getPartition(Text text, Text text2, int i) {
            String uid = text.toString().split("\\|")[1]; //we need keys which have same uid go to same reducer.
            return (new HashPartitioner()).getPartition(new Text(uid), text2, i);
        }
    }

    public static class SixRuleReducer extends TableReducer<Text, Text, NullWritable>{

        private int secLimit;
        private float moneyLimit;
        private int imsiLimit;
        private int timesLimit;
        private int totalDays;
        private String game;
        private String date;
        private Configuration conf;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.conf = context.getConfiguration();
            this.secLimit = conf.getInt("secLimit", 5) * 60;
            this.moneyLimit = conf.getFloat("moneyLimit", 30.0f);
            this.imsiLimit = conf.getInt("imsiLimit", 2);
            this.timesLimit = conf.getInt("timesLimit", 2);
            this.totalDays = conf.getInt("totalDays", 3);
            this.game = conf.get("game", "CARD");
            this.date = conf.get("date");
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            List<String> valueList = new ArrayList<>();        //按时间排序
            for (Text item : values){
                valueList.add(item.toString());
            }
            Collections.sort(valueList);

            /**
             * 获得uid所属更换且不再用的imsi数量
             */
            Set<String> uselessImsi = new HashSet<>();          //ex:AABBA, store A
            Set<String> usefulImsi = new HashSet<>();           //ex:AABBAa, store B
            String beforeImsi = "";                             //ex:AABBA, store A
            for (String value : valueList){
                String []items = value.split("\\|");
                if (CommonService.isTrueImsi(items[1]) && !uselessImsi.contains(items[1])){
                    if (!usefulImsi.contains(items[1])){
                        usefulImsi.add(items[1]);
                        beforeImsi = items[1];
                    }else if (!items[1].equals(beforeImsi)){
                        usefulImsi.remove(items[1]);
                        uselessImsi.add(items[1]);
                        beforeImsi = items[1];
                    }
                }
            }

            /**
             * 1、if useful.size >= this.imsiLimit
             *     时间段里>=10天里有5分钟内完成>=2次30元充值订单的行为
             */
            if (usefulImsi.size() < this.imsiLimit)
                return;

            //oneday_details key:2015-02-02 value:[15:48:23, 15:49:23]
            Map<String, List<String>> oneday_details = new HashMap<>();
            for (String value : valueList){
                String []items = value.split("\\|");
                if (Float.parseFloat(items[2]) == this.moneyLimit){
                    String []date = items[0].split(" ");
                    if (oneday_details.containsKey(date[1])){
                        oneday_details.get(date[1]).add(date[2]);
                    }else {
                        List<String> dlist = new ArrayList<>();
                        dlist.add(date[2]);
                        oneday_details.put(date[1], dlist);
                    }
                }
            }

            SimpleDateFormat df = new SimpleDateFormat("HH:mm:ss");
            int totalDays = 0;                      //一个月内总有多少天满足条件
            for (List<String> oneday : oneday_details.values()){
                String before = oneday.get(0);
                int i = 0;                          //5分钟充值次数，>=2结束break;
                for (String onedate : oneday){
                    try {
                        long secInterval = (df.parse(onedate).getTime() - df.parse(before).getTime()) / (1000);
                        if (secInterval <= this.secLimit && (++i) >= this.timesLimit){    //这样写应该没问题吧
                            totalDays++;
                            break;
                        }
                        before = onedate;
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                }
            }

            if (totalDays >= this.totalDays){
                /** get from 'automatic_ban' hbase table
                 */
                String rowkey = this.game + "|" + key.toString();
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
                cell.setBanType(6);
                cell.setBannedKey("1");
                list.add(cell);
                String jsonResult = JSON.toJSONString(list);
                Put put = new Put(Bytes.toBytes(rowkey));
                put.add(Bytes.toBytes("details"), Bytes.toBytes("reason"), Bytes.toBytes(jsonResult));
                context.write(NullWritable.get(), put);
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = CommonService.getConf();
        Job job = new Job(conf, "sixRuleJob");
        job.setJarByClass(SixRule.class);
        job.setMapperClass(SixRuleMapper.class);
        job.setReducerClass(SixRuleReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputFormatClass(TableOutputFormat.class);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        sdf.setTimeZone(TimeZone.getTimeZone("GMT+8"));
        Calendar before = Calendar.getInstance();
        Calendar now = Calendar.getInstance();
        now.setTime(new Date());
        now.set(now.get(Calendar.YEAR), now.get(Calendar.MONTH), 14);
        before.set(now.get(Calendar.YEAR), now.get(Calendar.MONTH) - 1, 15);
        while (before.compareTo(now) <= 0){
            FileInputFormat.addInputPath(job, new Path("/data/rawlog/card/pay_for_order/" + sdf.format(before.getTime())));
            before.add(Calendar.DAY_OF_MONTH, 1);
        }
        System.exit(job.waitForCompletion(true)?0:1);
    }
}

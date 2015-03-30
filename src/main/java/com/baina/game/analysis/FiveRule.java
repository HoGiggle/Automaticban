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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by jjhu on 2015/3/16.
 */
public class FiveRule {

    private final static Logger logger = Logger.getLogger(FiveRule.class);

    public static class FiveRuleMapper extends Mapper<Object, Text, Text, Text>{
        private int logLength;
        private String payType;
        private int uidLocal;
        private int imsiLocal;
        private int payTypeLocal;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            this.logLength = conf.getInt("logLength", 27);
            this.uidLocal = conf.getInt("uidLocal", 11);
            this.payType = conf.get("payType", "billing");
            this.imsiLocal = conf.getInt("imsiLocal", 9);
            this.payTypeLocal = conf.getInt("payTypeLocal", 18);
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] items = value.toString().split("\\|", -1);
            if (items.length != this.logLength && items.length != 28)       //日志有点问题，前期日志长度为28
                return;

            int payTypeKey;  //The type of we need is 0, otherwise 1.

            if (items[this.payTypeLocal].toLowerCase().endsWith(this.payType)){   //移动话费充值充值类型以billing结束
                payTypeKey = 0;
            }else {
                payTypeKey = 1;
            }
            StringBuilder sb = new StringBuilder();
            sb.append(items[0]);   //date
            sb.append("|");
            sb.append(items[this.imsiLocal]);
            sb.append("|");
            sb.append(payTypeKey);
            context.write(new Text(items[this.uidLocal]), new Text(sb.toString()));
        }
    }

    public static class FiveRuleCombiner extends Reducer<Text, Text, Text, Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text item : values){
                if (item.toString().endsWith("1")){
                    context.write(key, item);
                    return;
                }
            }
            for (Text item : values){
                context.write(key, item);
            }
        }
    }

    public static class FiveRuleReducer extends TableReducer<Text, Text, NullWritable>{
        private int imsiLimit;
        private String game;
        private String date;
        private Configuration conf;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.conf = context.getConfiguration();
            this.imsiLimit = this.conf.getInt("imsiLimit", 3);
            this.game = this.conf.get("game", "CARD");
            this.date = this.conf.get("date");
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> valueList = new ArrayList<>();        //按时间排序
            for (Text item : values){                          //当所有充值方式都为移动话费充值才有可能满足规则
                valueList.add(item.toString());
                if (item.toString().endsWith("1"))
                    return;
            }

            Collections.sort(valueList);
            Set<String> uselessImsi = new HashSet<>();          //ex:AABBA, store A
            Set<String> usefulImsi = new HashSet<>();           //ex:AABBAa, store B
            String beforeImsi = "";                             //ex:AABBA, store A
            for (String value : valueList){
                String []items = value.split("\\|");
                if (!uselessImsi.contains(items[1])){
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
             * conditionImsis is the sum of imsi which meet all conditions.
             * conditionImsis >= this.imsiLimit ? write it : nothing to do.
             */
            int conditionImsis = usefulImsi.size();
            if (conditionImsis >= this.imsiLimit){
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
                    logger.info("Reducer:" + new String(result.value().toString()));
                    list = JSON.parseObject(new String(result.value()), new TypeReference<List<RuleResultCell>>(){});
                }

                /** parse object list to jsonArray string, and put to hbase.
                 */
                RuleResultCell cell = new RuleResultCell();
                cell.setDate(this.date);
                cell.setBanType(5);
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
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        sdf.setTimeZone(TimeZone.getTimeZone("GMT+8"));
        Calendar before = Calendar.getInstance();
        before.setTime(new Date());
        before.set(before.get(Calendar.YEAR), before.get(Calendar.MONTH) - 1, 1);

        Configuration conf = CommonService.getConf();
        Job job = new Job(conf, "fiveRuleJob");
        job.setJarByClass(FiveRule.class);
        job.setMapperClass(FiveRuleMapper.class);
//        job.setCombinerClass(FiveRuleCombiner.class);
        job.setReducerClass(FiveRuleReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputFormatClass(TableOutputFormat.class);

        int monthDays = before.getActualMaximum(Calendar.DAY_OF_MONTH);
        for (int i = 0; i < monthDays; i++){
            FileInputFormat.addInputPath(job, new Path("/data/rawlog/card/pay_for_order/" + sdf.format(before.getTime())));
            before.add(Calendar.DAY_OF_MONTH, 1);
        }
        System.exit(job.waitForCompletion(true)?0:1);
    }
}

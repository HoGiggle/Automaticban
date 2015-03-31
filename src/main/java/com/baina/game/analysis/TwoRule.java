package com.baina.game.analysis;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by jjhu on 2015/3/26.
 */
public class TwoRule {

    private static final Logger LOGGER = Logger.getLogger(TwoRule.class);

    public static class TwoRuleMapper extends Mapper<Object, Text, Text, Text>{

        private int logLength;
        private int dateLocal;
        private int uidLocal;
        private int propertyIdLocal;
        private int propAmountLocal;
        private int chanAmountLocal;
        private int rechargeLocal;
        private int playedTimesLocal;

        private int propertyId;
        private int rechargeLimit;
        private int playedTimesLimit;
        private String beforeTime;
        private String date;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            this.logLength = conf.getInt("logLength", 23);
            this.dateLocal = conf.getInt("dateLocal", 0);
            this.uidLocal = conf.getInt("uidLocal", 12);
            this.playedTimesLocal = conf.getInt("playedTimesLocal", 13);
            this.rechargeLocal = conf.getInt("rechargeLocal", 17);
            this.propertyIdLocal = conf.getInt("propertyIdLocal", 19);
            this.propAmountLocal = conf.getInt("propAmountLocal", 21);
            this.chanAmountLocal = conf.getInt("chanAmountLocal", 22);

            this.propertyId = conf.getInt("propertyId", 1);
            this.rechargeLimit = conf.getInt("rechargeLimit", 300);
            this.playedTimesLimit = conf.getInt("playedTimesLimit", 100);
            this.beforeTime = conf.get("beforeTime");
            this.date = conf.get("date");
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String []items = value.toString().split("\\|");

            //map端筛选条件，全部满足才写出
            if (items.length != this.logLength)
                return;

            if (items[this.dateLocal].compareTo(this.beforeTime) < 0 || items[this.dateLocal].compareTo(this.date) > 0 )
                return;

            if (Integer.parseInt(items[this.propertyIdLocal]) != this.propertyId)
                return;

            if (Integer.parseInt(items[this.playedTimesLocal]) > this.playedTimesLimit ||
                    Integer.parseInt(items[this.rechargeLocal]) > this.rechargeLimit)
                return;

            StringBuilder sb = new StringBuilder();
            sb.append(items[this.dateLocal]);
            sb.append("|");
            sb.append(items[this.propAmountLocal]);
            sb.append("|");
            sb.append(items[this.chanAmountLocal]);
            context.write(new Text(items[this.uidLocal]), new Text(sb.toString()));
        }
    }

    public static class TwoRuleReducer extends TableReducer<Object, Text, NullWritable>{

        private Configuration conf;
        private String game;
        private String date;
        private String beforeTime;
        private String midTime;
        private int secLimit;
        private int unusualLimit;
        private static SimpleDateFormat sdf;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.conf = context.getConfiguration();
            this.date = this.conf.get("date");
            this.game = this.conf.get("game", "CARD");
            this.beforeTime = this.conf.get("beforeTime");
            this.midTime = this.conf.get("midTime");
            this.secLimit = this.conf.getInt("secLimit", 1800);
            this.unusualLimit = this.conf.getInt("unusualLimit", 10000);  //异常金币量
            this.sdf = new SimpleDateFormat("8.0 yyyy-MM-dd HH:mm:ss");
            this.sdf.setTimeZone(TimeZone.getTimeZone("GMT+8"));
        }

        @Override
        protected void reduce(Object key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            /** get from 'automatic_ban' hbase table
             */
            String rowkey = this.game + "|" + key.toString();
            HTable hTable = new HTable(this.conf, "automatic_ban");
            Get get = new Get(Bytes.toBytes(rowkey));
            get.addColumn(Bytes.toBytes("details"), Bytes.toBytes("reason"));
            Result result = hTable.get(get);

            /** parse jsonArray to object list, and set trueBeforeTime by compare this.beforeTime to hisBeforeTime.
             */
            List<RuleResultCell> list = new ArrayList<>();
            String trueBeforeTime = this.beforeTime;
            if (!result.isEmpty()){
                //list = JSON.parseArray(new String(result.value()), RuleResultCell.class);
                list = JSON.parseObject(new String(result.value()), new TypeReference<List<RuleResultCell>>() {
                });
                for (int i = list.size() - 1; i >= 0; i--){
                    if (list.get(i).getBanType() == 2 && list.get(i).getBannedKey().equals("1")){
                        String hisBeforeTime = list.get(i).getBeforeTime();
                        trueBeforeTime = (hisBeforeTime.compareTo(this.beforeTime) > 0 ? hisBeforeTime : this.beforeTime);
                        break;
                    }
                }
            }

            LOGGER.info("reduce--->" + key.toString() + "........." + trueBeforeTime);
            /** Get true data, and add unusual coins
             *  if unusualSum >= this.unusualLimit, write it to hbase.
             */
            List<String> userActions = new ArrayList<>();
            for (Text value : values){
                String valueStr = value.toString();
                if ((valueStr.compareTo(trueBeforeTime) >= 0)){
                    userActions.add(valueStr);
                }
            }
            Collections.sort(userActions);


            for (int i = 0; i < userActions.size() - 1; i++){
                String []items = userActions.get(i).split("\\|");
                int unusualSum = 0;               //一段时间内异常金币总量
                int beforePropAmount = Integer.parseInt(items[1]) - Integer.parseInt(items[2]);

                Date d = new Date(0l);    //初始化d 1900-01-01 00:00:00
                try {
                    d = this.sdf.parse(items[0]);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                String timeLimit = this.sdf.format(d.getTime() + this.secLimit * 1000l);

                int j;
                for ( j= i; j < userActions.size(); j++){
                    String []itemsj = userActions.get(j).split("\\|");
                    if (itemsj[0].compareTo(timeLimit) > 0){ //时间间隔超过30分钟，返回
                        break;
                    }

                    int nowPropAmount = Integer.parseInt(itemsj[1]) - Integer.parseInt(itemsj[2]);
                    if ((nowPropAmount - beforePropAmount) != 0){
                        LOGGER.info("except--->i:"+ key.toString()+ "...." + userActions.get(i));
                        LOGGER.info("except--->" + beforePropAmount);
                        LOGGER.info("except--->j: " +key.toString()+ "...." + userActions.get(j));
                    }

                    unusualSum += (nowPropAmount - beforePropAmount);
                    beforePropAmount = Integer.parseInt(itemsj[1]);
                    if (Math.abs(unusualSum) >= this.unusualLimit){   //如果异常金币大于100w，写入hbase
                        /** parse object list to jsonArray string, and put to hbase.
                         */
                        RuleResultCell cell = new RuleResultCell();
                        cell.setDate(this.date);
                        cell.setBanType(2);
                        cell.setBannedKey("1");

                        LOGGER.info("jjhu---->start: "+ key.toString()+ "...." + userActions.get(i));
                        LOGGER.info("jjhu---->Sum: " +key.toString()+ "...." + unusualSum);
                        LOGGER.info("jjhu---->end: " +key.toString()+ "...." + userActions.get(j));

                        String tmp = itemsj[0].compareTo(this.midTime) > 0 ? items[0] : this.midTime;
                        cell.setBeforeTime(tmp);
                        list.add(cell);
                        String jsonResult = JSON.toJSONString(list);
                        Put put = new Put(Bytes.toBytes(rowkey));
                        put.add(Bytes.toBytes("details"), Bytes.toBytes("reason"), Bytes.toBytes(jsonResult));
                        context.write(NullWritable.get(), put);

                        i = j;
                        break;
                    }
                }

                if (j == userActions.size())
                    break;    //整个用户行为都遍历完成，跳出循环
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        df.setTimeZone(TimeZone.getTimeZone("GMT+8"));
        String today = df.format(new Date());

        Configuration conf = CommonService.getConf();
        Job job = new Job(conf, "TwoRuleJob");
        job.setJarByClass(TwoRule.class);
        job.setMapperClass(TwoRuleMapper.class);
        job.setReducerClass(TwoRuleReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputFormatClass(TableOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("/data/rawlog/card/property_changed/" + today));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}

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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by jjhu on 2015/3/27.
 */
public class OneRule {
    public static class OneRuleMapper extends Mapper<Object, Text, Text, Text>{
        private int logLength;
        private int timeLocal;
        private int uidLocal;
        private int playedLocal;
        private int caishenLocal;

        private int playedLimit;
        private int caishenLimit;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            this.logLength = conf.getInt("logLength", 24);
            this.timeLocal = conf.getInt("timeLocal", 0);
            this.uidLocal = conf.getInt("uidLocal", 12);
            this.playedLocal = conf.getInt("playedLocal", 13);
            this.caishenLocal = conf.getInt("caishenLocal", 23);

            this.playedLimit = conf.getInt("playedLimit", 0);
            this.caishenLimit = conf.getInt("caishenLimit", 0);
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String []items = value.toString().split("\\|");
            if (items.length != this.logLength)
                return;

            if ((Integer.parseInt(items[this.caishenLocal]) > this.caishenLimit))  //玩过财神(玩财神超过caishenLimit)抛弃
                return;

            int tmp = 0;                                                          //玩过牌(玩牌超过playedLimit)记录为1, reducer端使用
            if (Integer.parseInt(items[this.playedLocal]) > this.playedLimit)
                tmp = 1;
            StringBuilder out = new StringBuilder();
            out.append(items[0]);       //date
            out.append("|");
            out.append(tmp);            //有木有玩牌局>=this.playedLimit标志位
            context.write(new Text(items[this.uidLocal]), new Text(out.toString()));
        }
    }

    public static class OneRuleReducer extends TableReducer<Text, Text, NullWritable>{

        private Configuration conf;
        private String game;
        private String date;

        private String beforeTime;
        private int crDaysLimit;
        private int loginDaysLimit;
        private int wanrenLimit;
        private int lotteryLimit;

        private Connection conn;
        private Map<Integer, PreparedStatement> crDatePst_map;
        private Map<Integer, PreparedStatement> allActPst_map;
        private Date now;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.conf = context.getConfiguration();
            this.game = this.conf.get("game", "CARD");
            this.date = this.conf.get("date");

            this.beforeTime = this.conf.get("beforeTime");
            this.crDaysLimit = this.conf.getInt("crDaysLimit", 7);
            this.loginDaysLimit = this.conf.getInt("loginDaysLimit", 3);
            this.wanrenLimit = this.conf.getInt("wanrenLimit", 0);
            this.lotteryLimit = this.conf.getInt("lotteryLimit", 0);

            this.conn = CommonService.getConnection();
            this.crDatePst_map = new HashMap<>();
            this.allActPst_map = new HashMap<>();
            this.now = new Date();
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<String> valueSet = new HashSet<>();
            Set<String> dateSet = new HashSet<>();

            for (Text value : values){
                valueSet.add(value.toString());
            }

            String latestTime = Collections.max(valueSet);   //只处理1小时内登陆账号
            if (latestTime.compareTo(this.beforeTime) < 0)
                return;

            for (String value : valueSet){
                String []items = value.split("\\|");
                if (Integer.parseInt(items[1]) > 0){     //用户30天内玩过牌, 抛弃
                    return;
                }
                String date = items[0].split(" ")[1];
                dateSet.add(date);
            }

            //reduce 端筛选
            if (dateSet.size() < this.loginDaysLimit)    //登陆天数 >= loginDaysLimit, 否则抛弃
                return;

            int uid = Integer.parseInt(key.toString());  //没有参与任何活动, 否则抛弃
            int table = uid % 16;
            if (allActPst_map.containsKey(table)){
                if (CommonService.isJoinedAnyActivity(allActPst_map.get(table), uid))
                    return;
            }else {
                PreparedStatement pst = null;
                try {
                    pst = this.conn.prepareStatement("select * from c_property_"+ table +" t where t.acc_id=? and t.prop_id in (1001,1002,1005)");
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                if (pst != null){
                    allActPst_map.put(table, pst);
                    if (CommonService.isJoinedAnyActivity(pst, uid))
                        return;
                }
            }

            int nowIntervalDays = (int) ((this.now.getTime() + 8*60*60*1000l) / (1000l*60*60*24));
            if (crDatePst_map.containsKey(table)){
                int createDays = CommonService.getCrDateStamp(crDatePst_map.get(table), uid);
                if ((nowIntervalDays - createDays) < this.loginDaysLimit)
                    return;
            }else {
                PreparedStatement pst = null;
                try {
                    pst = this.conn.prepareStatement("select t.cr_date from a_acc_data_"+ table +" t where t.acc_id=?");
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                if (pst != null){
                    crDatePst_map.put(table, pst);
                    int createDays = CommonService.getCrDateStamp(pst, uid);
                    if ((nowIntervalDays - createDays) < this.loginDaysLimit)
                        return;
                }
            }

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
            cell.setBanType(1);
            list.add(cell);
            String jsonResult = JSON.toJSONString(list);
            Put put = new Put(Bytes.toBytes(rowkey));
            put.add(Bytes.toBytes("details"), Bytes.toBytes("reason"), Bytes.toBytes(jsonResult));
            context.write(NullWritable.get(), put);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = CommonService.getConf();
        Job job = new Job(conf, "OneRuleJob");
        job.setJarByClass(OneRule.class);
        job.setMapperClass(OneRuleMapper.class);
        job.setReducerClass(OneRuleReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputFormatClass(TableOutputFormat.class);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        sdf.setTimeZone(TimeZone.getTimeZone("GMT+8"));
        Calendar today = Calendar.getInstance();
        today.setTime(new Date());

        for (int i = 0; i < 30; i++){
            FileInputFormat.addInputPath(job, new Path("/data/rawlog/card/login/" + sdf.format(today.getTime())));
            today.add(Calendar.DAY_OF_MONTH, -1);
        }

        System.exit(job.waitForCompletion(true)?0:1);
    }
}

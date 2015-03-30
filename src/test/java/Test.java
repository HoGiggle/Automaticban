import junit.framework.TestCase;
import org.apache.hadoop.hdfs.tools.DFSAdmin;

import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by jjhu on 2015/3/16.
 */
public class Test extends TestCase {
    public void testSplit() throws ParseException {
        SimpleDateFormat df = new SimpleDateFormat("8.0 yyyy-MM-dd HH:mm:ss");
        df.setTimeZone(TimeZone.getTimeZone("GMT+8"));
        System.out.println(new Date());
        System.out.println(df.format((new Date()).getTime() - 12*60*60*1000l));
    }

    public void testDate() throws ParseException {
        /*SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
        sdf.setTimeZone(TimeZone.getTimeZone("GMT+8"));
        Date d1 = sdf.parse("15:48:23");
        Date d2 = sdf.parse("15:58:23");

        long min = (d1.getTime() - d2.getTime())/(1000*60);
        System.out.println(min);*/

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat s = new SimpleDateFormat("yyyy-MM-dd");
        sdf.setTimeZone(TimeZone.getTimeZone("GMT+8"));
        Calendar now = Calendar.getInstance();
        Date d = new Date();
        System.out.println(sdf.format(now.getTime()));
        now.add(Calendar.DAY_OF_MONTH, -1);
        System.out.println(sdf.format(now.getTime()));
    }

    public void testJson() throws UnsupportedEncodingException {
        /*String s = "[{\"banType\":\"5\",\"date\":\"2015-03-16 20:30:35\"}]";
        byte []bytes = "[{\"banType\":\"5\",\"date\":\"2015-03-16 20:30:35\"}]".getBytes("UTF-8");
//        List<RuleResultCell> cells = JSON.parseArray(s, RuleResultCell.class);
        List<RuleResultCell> cells = JSON.parseObject(s, new TypeReference<List<RuleResultCell>>(){});
        for (RuleResultCell cell : cells){
            System.out.println(cell.getBanType());
            System.out.println(cell.getDate());
        }*/

        byte bytes[] = new byte[] { 50, 0, -1, 28, -24 };
        String s = new String(bytes, "ISO-8859-1");
        byte[] ret = s.getBytes("ISO-8859-1");
        System.out.println(s);
        System.out.println(bytes.toString());
        for (byte b : ret){
            System.out.println(b);
        }
    }

    public void testSplit1(){
        String s = "|a|||";
        System.out.println(s.split("\\|", -1).length);
    }

    public void testEndwith(){
        final List<String> list = new ArrayList<>();
        list.add("1");
        list.add("2");
        list.add("3");
        list.add("5");
        Iterable<String> iterable = new Iterable<String>() {
            @Override
            public Iterator<String> iterator() {
                return list.iterator();
            }
        };
        System.out.println("---------");


        System.out.println("???????????????????");
        Iterable<String> iter = new Iterable<String>() {
            public Iterator<String> iterator() {
                List<String> l = new ArrayList<String>();
                l.add("aa");
                l.add("bb");
                l.add("cc");
                return l.iterator();
            }
        };

        for(int count : new int[] {1, 2}){
            for (String item : iter) {
                System.out.println(item);
            }
            System.out.println("---------->> " + count + " END.");
        }
    }

    public void testMap(){
        List<String> list = new ArrayList<>();
        list.add("lijinlong107@163.com");
        list.add("lijinlong107@163.com.cn");
        list.add("lijinlong107@163com");
        list.add("lijinlong107@163.cn");
        list.add("lijinlong107@163.com.cn");
        list.add("lijinlong107@163comcn");
//        Pattern p = Pattern.compile("^(\\w+[-|\\.]?)+[a-z0-9A-Z]@([a-z0-9A-Z]+(-[a-z0-9A-Z]+)?\\.)+[a-zA-Z]{2,}$");
//        Pattern p = Pattern.compile("([\\w|//.|-]+[/w])@([/w|-]+)(\\.com|\\.com\\.cn)$");
//        Pattern p = Pattern.compile("^((\\w+[-|\\.]?)+\\w)@(\\w+)\\.(com|com\\.cn)$");
//        Pattern p = Pattern.compile("^((\\w+[-|\\.]?)+[a-z0-9A-Z])@((\\w+(-[a-z0-9A-Z]+)?)\\.)((\\w+(-[a-z0-9A-Z]+)?)\\.)*[a-zA-Z]{2,}$");
        Pattern p = Pattern.compile("^((\\w+[-|\\.]?)+[a-z0-9A-Z])@((\\w+(-[a-z0-9A-Z]+)?)\\.)+[a-zA-Z]{2,}$");
        for (String s : list){
            Matcher m = p.matcher(s);
            if (m.find()){
                System.out.println(m.group());
                System.out.println(m.group(1));
                System.out.println(m.group(4));
            }
        }
    }

    public void testException(){
        List<String> list = new ArrayList<>();
        list.add("1");
        list.add("2");
        list.add("3");
        list.add("4");
        String s = "";
        String s1 = s;
        System.out.println(s == s1);
        for (String l : list){
            System.out.println("s: " + s);
            System.out.println("l: " + l);
            s = l;
        }
        list.clear();
        System.out.println(list.size());
        System.out.println(s);
        System.out.println(s1);
        System.out.println(s1.equals(s));
        System.out.println("988676efe06e94e57bace568fbde4ffc49718001".length());
    }

}

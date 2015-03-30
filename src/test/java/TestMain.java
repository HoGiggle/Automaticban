
/**
 * Created by jjhu on 2015/3/25.
 */
public class TestMain {
    public static void main(String[] args) {
        System.out.println("return value of getValue(): " + getValue());
    }

    public static int getValue() {
        int i = 1;
        try {
            int s = i / 0;
        } catch (Exception e){
            i = 2;
            System.out.println("error");
            return i;
        }finally {
            i++;
            System.out.println(i);
        }
        return 4;
    }
}

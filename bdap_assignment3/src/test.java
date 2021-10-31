import java.text.ParseException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;

public class test {
    public static void main(String[] args) throws ParseException {
//        long time = Calculations.convertTime("'2010-02-01 19:18:29'");
//        long t1 = Calculations.parseTimeMonthly(time);
//        Integer s = Calculations.parseMonth(1267398000000L);
//        System.out.println(time);
//        System.out.println(t1);
//        System.out.println(s);
        TreeSet<Integer> a = new TreeSet<Integer>(new myComparator());
        a.add(20);
        a.add(12);
        a.add(42);
        a.add(32);
        a.add(15);
//        for (Integer integer : a) {
//            System.out.println(integer);
//        }
//        int[] b = a.toArray(new int[a.size()]);
//        System.out.println(b);
    }

}
class myComparator implements Comparator<Integer> {

    @Override
    public int compare(Integer o1, Integer o2) {
        if (o1-o2 >0) {
            return 1;
        } else if (o1-o2<0) {
            return -1;
        } else return 0;
    }
}

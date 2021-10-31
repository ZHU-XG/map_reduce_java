//import java.text.ParseException;
//import java.text.SimpleDateFormat;
//import java.util.Date;
//
//public class Calculations {
//    private static final double pi = Math.PI;
//    private static final double rad = pi / 180;
//    private static final double r = 6371.009;  // km
//    private static final double r_square = Math.pow(r, 2);
//
//    public static double sphericalEarthFlatDistance(String splat, String splong, String eplat, String eplong) {
//        double start_pos_lat = Double.parseDouble(splat) * rad;
//        double start_pos_long = Double.parseDouble(splong) * rad;
//        double end_pos_lat = Double.parseDouble(eplat) * rad;
//        double end_pos_long = Double.parseDouble(eplong) * rad;
//        double delta_fai = end_pos_lat - start_pos_lat;
//        double fai_m = (end_pos_lat + start_pos_lat) / 2;
//        double delta_lambda = end_pos_long - start_pos_long;
//        return r * Math.sqrt(Math.pow(delta_fai, 2) + Math.pow(Math.cos(fai_m)*delta_lambda, 2));
//    }
//
//    public static double getDuration(String st, String et) throws ParseException {
//        SimpleDateFormat simple_format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
//        Date start_time = simple_format.parse(st.substring(1, st.length()-1));
//        Date end_time = simple_format.parse(et.substring(1, et.length()-1));
//        long start = start_time.getTime();
//        long end = end_time.getTime();
//        return (double) ((end - start) / 1000);
//    }
//
//    public static double getSpeed(Double dist, Double duration) {
//        return (3600*dist) / duration;
//    }
//
//    public static double getRevenue(Double distance) {
////        return 3.5 + 1.71 * (Math.ceil(distance) - 1);
//        return 3.5 + 1.71 * distance;
//    }
//
//    public static boolean inCircle(Double latitude, Double longitude) {
//        if (latitude > 37.6123 && latitude < 37.63032 && longitude > -122.39033 && longitude < -122.36759) {
//            double loc_lat = latitude * rad;
//            double loc_long = longitude * rad;
//            double airport_lat = 37.62131 * rad;
//            double airport_long = (-122.37896) * rad;
//            double delta_fai = loc_lat - airport_lat;
//            double fai_m = (loc_lat + airport_lat) / 2;
//            double delta_lambda = loc_long - airport_long;
//            double square_distance =  r_square * (Math.pow(delta_fai, 2) + Math.pow(Math.cos(fai_m)*delta_lambda, 2));
//            return square_distance < 1;
//        } else {
//            return false;
//        }
//    }
//
//}

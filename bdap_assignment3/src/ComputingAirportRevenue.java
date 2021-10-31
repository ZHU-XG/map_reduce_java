//import java.io.*;
//import java.text.ParseException;
//import java.util.ArrayList;
//import java.util.List;
//
//public class ComputingAirportRevenue {
//    public static void main(String[] args) throws IOException, ParseException {
//        long start_time = System.currentTimeMillis();
//        BufferedReader br = new BufferedReader(new FileReader("E:\\Study\\bdap_assignment3\\src\\data\\taxi_706.segments"));
//        String line;
//        boolean in_circle = false;
//        String start_lat = null;
//        String start_long = null;
//        String end_lat;
//        String end_long;
//        List<Double> distances = new ArrayList<>();
//
//        while((line = br.readLine())!=null) {
//            String[] trip_info = line.split(",");
//
//            if (trip_info[4].equals("'E'") && trip_info[8].equals("'M'")){
//                start_lat = trip_info[6];
//                start_long = trip_info[7];
//                if (inCircle(Double.parseDouble(start_lat), Double.parseDouble(start_long))) {
//                    in_circle = true;
//                }
//            }
//
//            if (trip_info[4].equals("'M'") && trip_info[8].equals("'E'") && in_circle && start_lat != null){
//                end_lat = trip_info[2];
//                end_long = trip_info[3];
//                double distance = Calculations.sphericalEarthFlatDistance(start_lat, start_long, end_lat, end_long);
//                distances.add(distance);
//                in_circle = false;
//            }
//
//            if (!in_circle){
//
//                if (trip_info[4].equals("'M'") && trip_info[8].equals("'M'")){
//                    double distance = Calculations.sphericalEarthFlatDistance(trip_info[2], trip_info[3], trip_info[6], trip_info[7]);
//                    double seconds = Calculations.getDuration(trip_info[1], trip_info[5]);
//                    double speed = Calculations.getSpeed(distance, seconds);
//                    if (speed <= 200){
//                        double mid_lat = (Double.parseDouble(trip_info[2]) + Double.parseDouble(trip_info[6])) / 2;
//                        double mid_long = (Double.parseDouble(trip_info[3]) + Double.parseDouble(trip_info[7])) / 2;
//                        if (Calculations.inCircle(mid_lat, mid_long)){
//                            in_circle = true;
//                        }
//                    }
//                }
//            }
//        }
//
//        br.close();
//
//        long end_time = System.currentTimeMillis();
//        System.out.println("Total time is: " + (end_time-start_time) + "ms.");
//        double revenue = 0;
//        double trip_distances = 0;
//        for (Double distance : distances) {
//            revenue += Calculations.getRevenue(distance);
//            trip_distances += distance;
//        }
//        System.out.println("Total revenue obtained from airport trips is " + revenue + "$.");
//        System.out.println("Total distance of airport trips is " + trip_distances + "km.");
//    }
//
//    private static boolean inCircle(double parseDouble, double parseDouble1) {
//    }
//
//}

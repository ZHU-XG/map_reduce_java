import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.log4j.BasicConfigurator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.lang.Double;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Exercise2 {

    /**
     * Serialization of (time, latitude, longitude, state).
     */
    public static class StateData implements Writable {
        private Long time;
        private Double latitude;
        private Double longitude;
        private Boolean full;

        public StateData() {
        }

        public StateData(Long time, Double latitude, Double longitude, Boolean full) {
            this.time = time;
            this.latitude = latitude;
            this.longitude = longitude;
            this.full = full;
        }

        public Long getTime() {
            return time;
        }

        public void setTime(Long time) {
            this.time = time;
        }

        public Double getLatitude() {
            return latitude;
        }

        public void setLatitude(Double latitude) {
            this.latitude = latitude;
        }

        public Double getLongitude() {
            return longitude;
        }

        public void setLongitude(Double longitude) {
            this.longitude = longitude;
        }

        public Boolean getFull() {
            return full;
        }

        public void setFull(Boolean full) {
            this.full = full;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeLong(time);
            dataOutput.writeDouble(latitude);
            dataOutput.writeDouble(longitude);
            dataOutput.writeBoolean(full);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.time = dataInput.readLong();
            this.latitude = dataInput.readDouble();
            this.longitude = dataInput.readDouble();
            this.full = dataInput.readBoolean();
        }

        @Override
        public String toString() {
            return time + "," + latitude + "," + longitude + "," + full;
        }
    }

    /**
     * Serialization of (year-month, revenue).
     */
    public static class MonthlyRevenue implements Writable {

        private Long year_month;
        private Double revenue;

        public MonthlyRevenue() {

        }

        public MonthlyRevenue(Long year_month, Double revenue){
            this.year_month = year_month;
            this.revenue = revenue;
        }

        public Long getYear_month() {
            return year_month;
        }

        public void setYear_month(Long year_month) {
            this.year_month = year_month;
        }

        public Double getRevenue() {
            return revenue;
        }

        public void setRevenue(Double revenue) {
            this.revenue = revenue;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeLong(year_month);
            dataOutput.writeDouble(revenue);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.year_month = dataInput.readLong();
            this.revenue = dataInput.readDouble();
        }

        @Override
        public String toString() {
            return year_month + "\t" + revenue;
        }
    }

    /**
     * Serialization of (time, latitude, longitude, state).
     */
    public static class YearMonth implements WritableComparable<YearMonth> {

        private Integer year;
        private Integer month;

        public YearMonth(){

        };

        public YearMonth(Integer year, Integer month) {
            this.year = year;
            this.month = month;
        }

        public Integer getYear() {
            return year;
        }

        public void setYear(Integer year) {
            this.year = year;
        }

        public Integer getMonth() {
            return month;
        }

        public void setMonth(Integer month) {
            this.month = month;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeInt(year);
            dataOutput.writeInt(month);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.year = dataInput.readInt();
            this.month = dataInput.readInt();
        }

        @Override
        public String toString() {
            return year + "-" + month;
        }


        @Override
        public int compareTo(YearMonth o) {
            if (year > o.getYear()) {
                return 1;
            } else if (year < o.getYear()) {
                return -1;
            } else {
                return Integer.compare(month, o.getMonth());
            }
        }
    }

    /**
     * override of the comparator for treeset.
     */
    public static class TimeComparator implements Comparator<StateData> {

        @Override
        public int compare(StateData o1, StateData o2) {
            long t = o1.getTime() - o2.getTime();
            if (t > 0) {
                return 1;
            } else if (t < 0){
                return -1;
            } else {
                return 0;
            }
        }
    }

    /**
     * mapper of job 1.
     * delete NULLs.
     * output (taxi_number, residual_data)
     */
    public static class TripMapper extends Mapper<Object, Text, IntWritable, StateData> {
        private final IntWritable out_key = new IntWritable();
        private final StateData out_value = new StateData();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String[] trip_info = line.split(",");

            // skip if both are NULL
            boolean empty = parseTrip(trip_info[4], trip_info[8]);
            if (empty) {
                return;
            }

            // if the first segment in a line is null
            if (trip_info[4].equals("NULL")){
                int taxi_num = Integer.parseInt(trip_info[0]);
                Long end_time = null;
                try {
                    end_time = Calculations.convertTime(trip_info[5]);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                Double end_latitude = Double.parseDouble(trip_info[6]);
                Double end_longitude = Double.parseDouble(trip_info[7]);
                Boolean end_state = trip_info[8].equals("'M'");

                out_key.set(taxi_num);
                out_value.setTime(end_time);
                out_value.setLatitude(end_latitude);
                out_value.setLongitude(end_longitude);
                out_value.setFull(end_state);

                context.write(out_key, out_value);

            } else if (trip_info[8].equals("NULL")) {  // the second segment in a line is Null
                int taxi_num = Integer.parseInt(trip_info[0]);
                Long start_time = null;
                try {
                    start_time = Calculations.convertTime(trip_info[1]);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                Double start_latitude = Double.parseDouble(trip_info[2]);
                Double start_longitude = Double.parseDouble(trip_info[3]);
                Boolean start_state = trip_info[4].equals("'M'");

                out_key.set(taxi_num);
                out_value.setTime(start_time);
                out_value.setLatitude(start_latitude);
                out_value.setLongitude(start_longitude);
                out_value.setFull(start_state);
                context.write(out_key, out_value);

            } else {  // both segments in a line are not null
                int taxi_num = Integer.parseInt(trip_info[0]);
                Long start_time = null;
                try {
                    start_time = Calculations.convertTime(trip_info[1]);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                Double start_latitude = Double.parseDouble(trip_info[2]);
                Double start_longitude = Double.parseDouble(trip_info[3]);
                Boolean start_state = trip_info[4].equals("'M'");
                Long end_time = null;
                try {
                    end_time = Calculations.convertTime(trip_info[5]);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                Double end_latitude = Double.parseDouble(trip_info[6]);
                Double end_longitude = Double.parseDouble(trip_info[7]);
                Boolean end_state = trip_info[8].equals("'M'");

                // output both segments
                out_key.set(taxi_num);
                out_value.setTime(start_time);
                out_value.setLatitude(start_latitude);
                out_value.setLongitude(start_longitude);
                out_value.setFull(start_state);
                context.write(out_key, out_value);

                out_value.setTime(end_time);
                out_value.setLatitude(end_latitude);
                out_value.setLongitude(end_longitude);
                out_value.setFull(end_state);
                context.write(out_key, out_value);
            }
        }
        // Extract-Transform-Load
        private boolean parseTrip(String ss, String es) {
            return ss.equals("NULL") && es.equals("NULL");
        }
    }

    /**
     * reducer of job 1.
     * find trips passing through the airport circle.
     * output (taxi_number, month and revenue)
     */
    public static class TripReducer extends Reducer<IntWritable, StateData, IntWritable, MonthlyRevenue> {

        private final Set<StateData> lines = new TreeSet<>(new TimeComparator());

        @Override
        protected void reduce(IntWritable key, Iterable<StateData> values, Context context) throws IOException, InterruptedException {
            boolean in_circle = false;
            boolean previous_state = false;
            double start_trip_lat = 0;
            double start_trip_long = 0;
            double end_trip_lat;
            double end_trip_long;

            // utilize tree set to eliminate duplicates and sort
            lines.clear();
            for (StateData value : values) {
                lines.add(new StateData(value.getTime(), value.getLatitude(), value.getLongitude(), value.getFull()));
            }
            StateData[] a = lines.toArray(new StateData[lines.size()]);

            for (int i = 0; i < lines.size(); i++) {
                boolean current_state = a[i].getFull();
                // start the trip if status: E->M
                if (!previous_state && current_state) {
                    start_trip_lat = a[i].getLatitude();
                    start_trip_long = a[i].getLongitude();
                    // check if in the airport circle
                    if (Calculations.inCircle(start_trip_lat, start_trip_long)) {
                        in_circle = true;
                    }
                }

                // end the trip if status: M->E
                if (previous_state && !current_state && in_circle && start_trip_lat != 0) {
                    end_trip_lat = a[i-1].getLatitude();
                    end_trip_long = a[i-1].getLongitude();
                    double trip_distance = Calculations.sphericalEarthDistance(start_trip_lat, start_trip_long, end_trip_lat, end_trip_long);
                    double trip_revenue = Calculations.getRevenue(trip_distance);
                    try {
                        Long trip_time = Calculations.parseTimeMonthly(a[i-1].getTime());
                        context.write(key, new MonthlyRevenue(trip_time, trip_revenue));
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                    in_circle = false;
                }

                if (!in_circle) {
                    if (previous_state && current_state) {
                        double distance = Calculations.sphericalEarthDistance(a[i-1].getLatitude(), a[i-1].getLongitude(),
                                                                              a[i].getLatitude(), a[i].getLongitude());
                        double seconds = (double) (a[i].getTime() - a[i-1].getTime());
                        double speed = Calculations.getSpeed(distance, seconds);
                        if (speed <= 200) {
                            double mid_lat = (a[i].getLatitude() + a[i-1].getLatitude()) / 2;
                            double mid_long = (a[i].getLongitude() + a[i-1].getLongitude()) / 2;
                            if (Calculations.inCircle(mid_lat, mid_long)) {
                                in_circle = true;
                            }
                        }
                    }
                }
                previous_state = current_state;
            }
        }
    }

    /**
     * mapper of job 2.
     * output (year_month, revenues)
     */
    public static class MonthlyMapper extends Mapper<Object, Text, YearMonth, DoubleWritable> {
        private final YearMonth out_k = new YearMonth();
        private final DoubleWritable out_v = new DoubleWritable();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] month_distance = line.split("\t");

            Long year_month = Long.parseLong(month_distance[1]);
            Integer year = Calculations.parseYear(year_month);
            Integer month = Calculations.parseMonth(year_month);
            Double distance = Double.parseDouble(month_distance[2]);

            out_k.setYear(year);
            out_k.setMonth(month);
            out_v.set(distance);

            context.write(out_k, out_v);
        }
    }

    /**
     * reducer of job 2.
     * summation.
     * output (year_month, revenue)
     */
    public static class MonthlyReducer extends Reducer<YearMonth, DoubleWritable, YearMonth, DoubleWritable>{

        @Override
        protected void reduce(YearMonth key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double monthly_dist = 0;
            for (DoubleWritable value : values) {
                monthly_dist += value.get();
            }

            DoubleWritable total_distance = new DoubleWritable(monthly_dist);
            context.write(key, total_distance);

        }
    }

    /**
     * mathematical calculations.
     */
    public static class Calculations {
        private static final double pi = Math.PI;
        private static final double rad = pi / 180;
        private static final double r = 6371.009;  // km
        private static final double r_square = Math.pow(r, 2);
        private static final TimeZone time_zone = TimeZone.getTimeZone("America/Los Angeles");

        // compute distance with double input
        public static double sphericalEarthDistance(Double splat, Double splong, Double eplat, Double eplong) {
            double start_pos_lat = splat * rad;
            double start_pos_long = splong * rad;
            double end_pos_lat = eplat * rad;
            double end_pos_long = eplong * rad;
            double delta_fai = end_pos_lat - start_pos_lat;
            double fai_m = (end_pos_lat + start_pos_lat) / 2;
            double delta_lambda = end_pos_long - start_pos_long;
            return r * Math.sqrt(Math.pow(delta_fai, 2) + Math.pow(Math.cos(fai_m)*delta_lambda, 2));
        }

        // convert time from string to long
        public static long convertTime(String t) throws ParseException {
            SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
            sf.setTimeZone(time_zone);
            Date time = sf.parse(t.substring(1, t.length()-1));
            return time.getTime() / 1000;
        }

        // only save year and month information
        public static Long parseTimeMonthly(Long t) throws ParseException {
            SimpleDateFormat sf1 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
            sf1.setTimeZone(time_zone);
            Date d1 = new Date(t * 1000);
            String st = sf1.format(d1);
            SimpleDateFormat sf2 = new SimpleDateFormat("yyyy-MM");
            sf2.setTimeZone(time_zone);
            Date time = sf2.parse(st.substring(0, 7));
            return time.getTime();
        }

        // get year
        public static Integer parseYear(Long time) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM");
            sdf.setTimeZone(time_zone);
            Date date = new Date(time);
            return Integer.parseInt(sdf.format(date).substring(0, 4));
        }

        // get month
        public static Integer parseMonth(Long time) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM");
            sdf.setTimeZone(time_zone);
            Date date = new Date(time);
            return Integer.parseInt(sdf.format(date).substring(5, 7));
        }

        // compute speed
        public static double getSpeed(Double dist, Double duration) {
            return (3600*dist) / duration;
        }

        // compute revenue
        public static double getRevenue(Double distance) {
//        return 3.5 + 1.71 * (Math.ceil(distance) - 1);
            return 3.5 + 1.71 * distance;
        }

        // whether pass through the airport circle
        public static boolean inCircle(Double latitude, Double longitude) {
            // roughly filter the trips
            if (latitude > 37.6123 && latitude < 37.63032 && longitude > -122.39033 && longitude < -122.36759) {
                double loc_lat = latitude * rad;
                double loc_long = longitude * rad;
                double airport_lat = 37.62131 * rad;
                double airport_long = (-122.37896) * rad;
                double delta_fai = loc_lat - airport_lat;
                double fai_m = (loc_lat + airport_lat) / 2;
                double delta_lambda = loc_long - airport_long;
                double square_distance =  r_square * (Math.pow(delta_fai, 2) + Math.pow(Math.cos(fai_m)*delta_lambda, 2));
                return square_distance < 1;
            } else {
                return false;
            }
        }
    }

    public static void main(String[] args) throws Exception {

        BasicConfigurator.configure();

        // set job1
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "compute revenues by taxi number");
        job1.setJarByClass(Exercise2.class);
        job1.setMapperClass(Exercise2.TripMapper.class);
        job1.setReducerClass(Exercise2.TripReducer.class);
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(StateData.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(MonthlyRevenue.class);
        job1.setNumReduceTasks(32);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        // set job2
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "compute total revenue by month");
        job2.setJarByClass(Exercise2.class);
        job2.setMapperClass(Exercise2.MonthlyMapper.class);
        job2.setCombinerClass(Exercise2.MonthlyReducer.class);
        job2.setReducerClass(Exercise2.MonthlyReducer.class);
        job2.setMapOutputKeyClass(YearMonth.class);
        job2.setMapOutputValueClass(DoubleWritable.class);
        job2.setOutputKeyClass(YearMonth.class);
        job2.setOutputValueClass(DoubleWritable.class);
        job2.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        FileInputFormat.setMaxInputSplitSize(job1, 134217728);
        FileInputFormat.setMaxInputSplitSize(job2, 134217728);

        // job chain, concatenate job2 to job1
        JobControl job_control = new JobControl("jobs");

        ControlledJob cj1 = new ControlledJob(conf1);
        cj1.setJob(job1);
        job_control.addJob(cj1);

        ControlledJob cj2 = new ControlledJob(conf2);
        cj2.setJob(job2);
        cj2.addDependingJob(cj1);
        job_control.addJob(cj2);

        Thread jc_thread = new Thread(job_control);
        jc_thread.start();
        while (!job_control.allFinished()) {
            Thread.sleep(500);
        }
        job_control.stop();
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}




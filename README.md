# assignment13.3

Assignment 13.3.1

package assign13;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
public class petrol{
public static class Map extends Mapper<LongWritable, Text, Text,
IntWritable> {
private Text name = new Text();
private IntWritable volume = new IntWritable();
public void map(LongWritable key, Text value, Context context )
throws IOException, InterruptedException {
String line = value.toString();
if(line.length()>0){
String str[]=line.split(",");
name.set(str[1]);
if(str[5].matches("\\d+")){
int i=Integer.parseInt(str[5]);
volume.set(i);
}
context.write(name,volume);
}
}
}
public static class Reduce extends
Reducer<Text, IntWritable, Text, IntWritable> {
public void reduce(Text key, Iterable<IntWritable> values,
Context context) throws IOException, InterruptedException {
int sum = 0;
for (IntWritable val : values) {
sum = sum + val.get();
}
context.write(key, new IntWritable(sum));
}
}
public static void main(String[] args) throws Exception {
Configuration conf = new Configuration();
@SuppressWarnings("deprecation")
Job job = new Job(conf, "wordcount");
job.setJarByClass(petrol.class);
job.setMapOutputKeyClass(Text.class);
job.setMapOutputValueClass(IntWritable.class);
setOutputKeyClass(Text.class);
job.setOutputValueClass(IntWritable.class);
job.setMapperClass(Map.class);
job.setReducerClass(Reduce.class);
job.setInputFormatClass(TextInputFormat.class);
job.setOutputFormatClass(TextOutputFormat.class);
FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));
Path out=new Path(args[1]);
out.getFileSystem(conf).delete(out);
job.waitForCompletion(true);
}}

Assignment13.3.2

package assign13;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
public class top5{
public static TreeMap<Integer, Text> ToRecordMap = new
TreeMap<Integer,Text>();
public static class TopTenMapper extends Mapper<LongWritable,
Text, NullWritable, Text> {
// Stores a map of employees salaryâ€™s to the record
private Text name = new Text();
public void map(LongWritable key, Text value, Context
context)throws IOException, InterruptedException {
String line=value.toString();
String[] tokens=line.split(",");
name.set(tokens[0]);
if(tokens[5].matches("\\d+")){
int salary=Integer.parseInt(tokens[5]);
ToRecordMap.put(salary, new Text(value));
}
ToRecordMap.put(salary, new Text(value));
}
if(ToRecordMap.size()>5){
ToRecordMap.remove(ToRecordMap.firstKey());
}
}
protected void cleanup(Context context) throws IOException,
InterruptedException {
for (Text t : ToRecordMap.values()) {
context.write(NullWritable.get(), t);
}
}
}
public static class Reduce extends
Reducer<NullWritable, Text, Text, Text> {
public void reduce(NullWritable key, Iterable<Text>
values,Context context) throws IOException, InterruptedException {
int volume=0;
for (Text value : values) {
String line=value.toString();
if(line.length()>0){
String[] tokens=line.split(",");
if(tokens[5].matches("\\d+")){
volume=Integer.parseInt(tokens[5]);
}
ToRecordMap.put(volume, new Text(value));
}
}
if(ToRecordMap.size()>5){
ToRecordMap.remove(ToRecordMap.firstKey());
}
Text key1=new Text();
Text value1=new Text();
for (Text t : ToRecordMap.descendingMap().values()) {
String line=t.toString();
String[] text=line.split(",");
key1.set(text[0]);
value1.set(text[5]);
// Output our ten records to the file system with a
null key
context.write(key1, value1);
}
}
}
public static void main(String[] args) throws Exception {
Configuration conf = new Configuration();
@SuppressWarnings("deprecation")
Job job = new Job(conf, "wordcount");
job.setJarByClass(top5.class);
job.setMapOutputKeyClass(NullWritable.class);
job.setMapOutputValueClass(Text.class);
//job.setNumReduceTasks(0);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(Text.class);
job.setMapperClass(TopTenMapper.class);
job.setReducerClass(Reduce.class);
job.setInputFormatClass(TextInputFormat.class);
job.setOutputFormatClass(TextOutputFormat.class);
FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));
Path out=new Path(args[1]);
out.getFileSystem(conf).delete(out);
job.waitForCompletion(true);
}
}

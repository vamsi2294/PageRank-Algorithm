/*########################################VKOVURU@UNCC.EDU###################################
 * ####################################Vamsi Krishna Kovuru###############################
 */


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.hdfs.server.balancer.Matcher;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class GraphGenerator extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( GraphGenerator.class);

   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new GraphGenerator(), args);
      System .exit(res);
   }

   public int run( String[] args) throws  Exception {
      Job job  = Job .getInstance(getConf(), " graphgenerator ");
      job.setJarByClass( this .getClass());

      FileInputFormat.addInputPaths(job,  args[0]);
      FileOutputFormat.setOutputPath(job,  new Path(args[ 1]));
      job.setMapperClass( Map .class);
      job.setReducerClass( Reduce .class);
      job.setOutputKeyClass( IntWritable .class);
      job.setOutputValueClass( Text .class);

      return job.waitForCompletion( true)  ? 0 : 1;
   }
   
   public static class Map extends Mapper<LongWritable ,  Text , IntWritable, Text > {
      private final static IntWritable one  = new IntWritable( 1);
      
      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {

         String line  = lineText.toString();
        
         String titlePattern = "(<title>)([\\s\\S]*?)(</title>)";
         String urlPattern = "(\\[\\[)([\\s\\S]*?)(\\]\\])";
         
         String linkText = "";
         Pattern r1 = Pattern.compile(urlPattern);

         // Now create matcher object.
         java.util.regex.Matcher m1 = r1.matcher(line);
         while (m1.find( )) {
        	 linkText = linkText.concat(m1.group(2)).concat(",");  
         }
         
      // Create a Pattern object
         Pattern r = Pattern.compile(titlePattern);

         // Now create matcher object.
         java.util.regex.Matcher m = r.matcher(line);
         while (m.find( )) {
        	 if(linkText.length()>0){
        	 context.write(one, new Text(m.group(2).concat("#").concat(linkText.substring(0,linkText.length()-1))));
        	 }else{
        		 context.write(one, new Text(m.group(2).concat("#").concat(linkText)));
        	 }
         }       
      }
   }
   
   public static class Reduce extends Reducer< IntWritable , Text,  Text ,  DoubleWritable > {
      @Override 
      public void reduce( IntWritable count,  Iterable<Text > words,  Context context)
         throws IOException,  InterruptedException {
    	  ArrayList<String> list = new ArrayList<String>();
    	  HashMap<String,ArrayList<String>> pointTo = new HashMap<String,ArrayList<String>>();
    	   Double x = 0.0;
    	  for ( Text word: words) {
    		  ArrayList<String> urlsTo = new ArrayList<String>();
             	
    		  String[] keyWords = word.toString().split("#");
         	 if(keyWords.length>1){
           	for(String key: keyWords[1].split(",")){
           	
           		urlsTo.add(key);
           		
           	}
         	 }
           	list.add(keyWords[0]);
           	pointTo.put(keyWords[0], urlsTo);
         	
         	 
           	 x = x + 1;
          }
          
          Double pageRank = (double) (1/x);
          
          for(int i=0;i<list.size();i++){
           	String textName = list.get(i);
           	String keyName = textName.concat("#");
           	for(int j=0;j<list.size();j++){
           		if(pointTo.get(list.get(j)).contains(textName)&&(textName!=list.get(j))){
           			keyName = keyName.concat(list.get(j)).concat(",");
           		}
           	}
           	
           	context.write(new Text(keyName.substring(0, keyName.length()-1)), new DoubleWritable(pageRank));
            }   
      }
   }
}
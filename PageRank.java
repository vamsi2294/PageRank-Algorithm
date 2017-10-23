
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class PageRank extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( PageRank.class);

   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new PageRank(), args);
      System .exit(res);
   }

   public int run( String[] args) throws  Exception {
      Job job  = Job .getInstance(getConf(), " pagerank ");
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
         String[] wordNames = line.split("\\t");
         context.write(one, new Text(wordNames[0].concat("#<-").concat(wordNames[1])));
	 
       
      }
   }
   
   public static class Reduce extends Reducer< IntWritable , Text,  Text ,  DoubleWritable > {
      @Override 
      public void reduce( IntWritable count,  Iterable<Text > words,  Context context)
         throws IOException,  InterruptedException {
    	  
    	  HashMap<String,Double> pageRanks = new HashMap<String,Double>();
    	  HashMap<String,ArrayList<String>> urlsFrom = new HashMap<String,ArrayList<String>>();
    	  ArrayList<String> list = new ArrayList<String>();
    	  ArrayList<String> urlsTo = new ArrayList<String>();
    	  
    	  for(Text word: words){
    		  String[] keyWords = word.toString().split("#<-");
    		  String pageName = keyWords[0].split("#,#")[0];
    		  list.add(pageName);
    		  pageRanks.put(pageName, Double.parseDouble(keyWords[1]));
    		  
    		  ArrayList<String> urls = new ArrayList<String>();
    		  for(String x: keyWords[0].split("#<-")[1].split("#,#")){
    			  urls.add(x);
    			  urlsTo.add(x);
    		  }
    		  
    		  urlsFrom.put(pageName, urls);
    	  }
    	  
    	  for(int i=0; i<list.size(); i++){
    		  double newPageRank = 0.0;
    		  String page = list.get(i);
    		  String x = page.concat("#<-");
    		  for(String url: urlsFrom.get(page)){
    			  newPageRank = newPageRank+ (pageRanks.get(url)/Collections.frequency(urlsTo, url));
    			  x = x.concat(url).concat("#,#");
    		  }
    		  
    		  newPageRank = 0.15 + (0.85 * newPageRank);
    		  context.write(new Text(x.substring(0, x.length()-1)), new DoubleWritable(newPageRank));
    	  }
    	  
      }
   }
}
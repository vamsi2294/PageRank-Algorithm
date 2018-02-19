/*########################################VKOVURU@UNCC.EDU###################################
 * ####################################Vamsi Krishna Kovuru###############################
 */

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class Sort extends Configured implements Tool {

	private static final Logger LOG = Logger .getLogger( Sort.class);

	   public static void main( String[] args) throws  Exception {
	      int res  = ToolRunner .run( new Sort(), args);
	      System .exit(res);
	   }

	   public int run( String[] args) throws  Exception {
		   /* job3 Sortdriver functions */
		   Job job3  = Job .getInstance(getConf(), " sort ");
		   job3.setJarByClass( this .getClass());
		   /* setting intermediate output path for the PageRank */
		   FileInputFormat.addInputPaths(job3,  args[1].concat("/pagerank/10"));
		   FileOutputFormat.setOutputPath(job3,  new Path(args[1].concat("/sort")));
		   job3.setMapperClass( Map3 .class);
		   job3.setReducerClass( Reduce3 .class);
		   job3.setOutputKeyClass( DoubleWritable .class);
		   job3.setOutputValueClass( Text .class);
		   /* Wait for the completion of the sort job*/
		   job3.waitForCompletion( true);
		   Configuration conf = getConf();		   
		   FileSystem hdfs = FileSystem.get(conf);
		   /*Deleting the file path for Pagerank final*/
		   Path path = new Path(args[1].concat("/pagerank/"));
		   if(hdfs.exists(path)){
		    	 hdfs.delete(path, true);
		     }
		   return 1;
	   }	   

	   public static class Map3 extends Mapper<LongWritable ,  Text , DoubleWritable, Text > {

		      public void map( LongWritable offset,  Text lineText,  Context context)
		        throws  IOException,  InterruptedException {
		         String line  = lineText.toString();
		         String[] wordNames = line.split("\\t");		         
		         /*Setting key as the negative page rank value and 
		          * the value as the link name and out links
		          */
		         context.write(new DoubleWritable(Double.parseDouble(wordNames[1])*(-1)), new Text(wordNames[0]));
		      }
		   }
		   
		   public static class Reduce3 extends Reducer< DoubleWritable , Text,  Text ,  DoubleWritable > {
		      @Override 
		      public void reduce( DoubleWritable count,  Iterable<Text > words,  Context context)
		         throws IOException,  InterruptedException {		    	  
		    	  
		    	 for(Text word: words){
		    		 String page = word.toString().split("<#")[0];
		    		 Double value = count.get();
		    		 
		    		 //Setting final output key as the link name ,out links and value as the page rank value
		    		 context.write(new Text(page), new DoubleWritable(-value));
		    	 }
		    	  
		      }
		   }
}
/*########################################VKOVURU@UNCC.EDU###################################
 * ####################################Vamsi Krishna Kovuru###############################
 */


import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.regex.Pattern;

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

public class GraphGenerator extends Configured implements Tool {

	private static final Logger LOG = Logger .getLogger( GraphGenerator.class);

	   public static void main( String[] args) throws  Exception {
	      int graphGenRes  = ToolRunner .run( new GraphGenerator(), args);
	      
	      int pageRankRes  = ToolRunner .run( new PageRank(), args);
	      
	      int sortRes  = ToolRunner .run( new Sort(), args);
	   }

	   public int run( String[] args) throws  Exception {
		   /* job1 graph generator driver functions */
		   Job job  = Job .getInstance(getConf(), " graphgenerator ");
		   job.setJarByClass( this .getClass());
		   FileInputFormat.addInputPaths(job,  args[0]);
		   /* setting intermediate output path for the graphgenerator */
		   FileOutputFormat.setOutputPath(job,  new Path(args[1].concat("/graphgen")));
		   job.setMapperClass( Map .class);
		   job.setReducerClass( Reduce .class);
		   job.setOutputKeyClass( IntWritable .class);
		   job.setOutputValueClass( Text .class);
		   return job.waitForCompletion( true)?0:1;
		   
	   }
	   
	   public static class Map extends Mapper<LongWritable ,  Text , IntWritable, Text > {
		      private final static IntWritable one  = new IntWritable( 1);
		      
		      public void map( LongWritable offset,  Text lineText,  Context context)
		        throws  IOException,  InterruptedException {

		         String line  = lineText.toString();
		         /* Regular expression for identifying the links*/
		         String titleExp = "(<title>)([\\s\\S]*?)(</title>)";
			     /* Regular expression for identifying the links*/
		         String linkExp = "(\\[\\[)([\\s\\S]*?)(\\]\\])";
		         
		         String link = "";
		         Pattern matcher = Pattern.compile(linkExp);

		         /* Using Matcher function to obtain the regex pattern*/
		         java.util.regex.Matcher match = matcher.matcher(line);
		         /* Finding the Matched patterns */
		         while (match.find( )) {
		        	 link = link.concat(match.group(2)).concat("&&");  
		         }		         
		         Pattern r = Pattern.compile(titleExp);

		         /* finding the link pattern using matcher function*/
		         java.util.regex.Matcher match2 = r.matcher(line);
		         while (match2.find( )) {
		        	 if(link.length()>0){
		        		 /* Writing the context values for reducer*/
		        	 context.write(one, new Text(match2.group(2).concat("<#").concat(link.substring(0,link.length()-2))));
		        	 }else{
		        		 context.write(one, new Text(match2.group(2).concat("<#").concat(link)));
		        	 }
		         }       
		      }
		   }
		   
		   public static class Reduce extends Reducer< IntWritable , Text,  Text ,  DoubleWritable > {
			   @Override 
			      public void reduce( IntWritable count,  Iterable<Text > words,  Context context)
			         throws IOException,  InterruptedException {
			    	  	
			    	  HashMap<String,String> urlsTo = new HashMap<String,String>();
			    	  Double x = 0.0;
			    	  
			    	  /*finding the total number of pages in the given document*/
			    	    for(Text word: words){
			    	    	/* split withrespect to the title of the page*/
					    	  
			    	    	String[] keyWords = word.toString().split("<#");
			    	    	if(keyWords.length>1){
			    	    		urlsTo.put(keyWords[0], keyWords[1]);
			    	    	}
			    	    	else{
			    	    		urlsTo.put(keyWords[0], null);
			    	    	}			    	    	
			    	    	x += 1;
			    	    }			    	    
			    	    /* calculating the pagerank for the generated graph*/
			    	    Double pageRank = (double) (1/x);
			    	    
			    	    for(Entry<String, String> entry:urlsTo.entrySet()){
			    	    	/*Passing key value pairs of title and links as the outgoing to the next job*/ 
				    	    
			    	    	if(entry.getValue()!=null){
			    	    		context.write(new Text(entry.getKey().concat("<#").concat(entry.getValue())), new DoubleWritable(pageRank));
			    	    	}
			    	    	else{
			    	    		context.write(new Text(entry.getKey()), new DoubleWritable(pageRank));
			    	    	}
			    	    }
			      }
			   }	  
}
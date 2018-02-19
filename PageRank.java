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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class PageRank extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( PageRank.class);

   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new PageRank(), args);
      System .exit(res);
   }

   public int run( String[] args) throws  Exception {
	   /* running 10 pagerank for 10 iterations*/
	   for(int i=1;i<=10;i++){
		   Configuration conf = getConf();
		   Job job2  = Job .getInstance(conf, " pagerank ");
		   /* Deleting intermediate files from the hdfs*/
		   FileSystem hdfs = FileSystem.get(conf);
		   if(i==2){
			   /* setting path for the graphgeneratod*/
			   Path path = new Path(args[1].concat("/graphgen"));
			   if(hdfs.exists(path)){
			    	 hdfs.delete(path, true);
			     }
		   }
		   /* Deleting PageRank intermediate paths*/
		   if(i>2){
			   Path path = new Path(args[1].concat("/pagerank/").concat(Integer.toString(i-2)));
			   if(hdfs.exists(path)){
			    	 hdfs.delete(path, true);
			     }
		   }
		      job2.setJarByClass( this .getClass());
		      if(i==1){
		    	  FileInputFormat.addInputPaths(job2,  args[1].concat("/graphgen"));
		      }
		      else{
		    	  FileInputFormat.addInputPaths(job2,  args[1].concat("/pagerank/").concat(Integer.toString(i-1)));
		      }
		      FileOutputFormat.setOutputPath(job2,  new Path(args[ 1].concat("/pagerank/").concat(Integer.toString(i))));
		      job2.setMapperClass( Map2 .class);
		      job2.setReducerClass( Reduce2 .class);
		      job2.setOutputKeyClass( Text .class);
		      job2.setOutputValueClass( Text .class);
		      job2.waitForCompletion( true);
   
	   }
	   
	   return 1;
   }
   
   public static class Map2 extends Mapper<LongWritable ,  Text , Text, Text > {

	      public void map( LongWritable offset,  Text lineText,  Context context)
	        throws  IOException,  InterruptedException {
	    	  /*Converting to the string*/
	    	  String line  = lineText.toString();
	    	  /*Splitting the line with tab to obtain the pagerank*/
	    	  String[] delimiter = line.split("\\t");
	    	  /*Split to obtain the urlinks*/
	    	  String[] urlLinks = delimiter[0].split("<#");
	    	  
	    	  if(urlLinks.length>1){
	    		  /*writing the key as the url name and  value as out link urls*/
	    		  context.write(new Text(urlLinks[0]), new Text("<urls>".concat(urlLinks[1]))); 
	    		  String[] links = urlLinks[1].split("&&");
		    	  int total_links = links.length;
		    	
		    	  /*Writing key as the url link and value as 
		    	   * the pagerank calculated with the outlinks
		    	   */
				  for(String link: links){
		    		  try{
		    			  context.write(new Text(link), new Text(Double.toString(Double.parseDouble(delimiter[1])/total_links)));  
	    			  } catch(Throwable e ){
	    				  continue;
	    			  }
		    	  }
	    	  }
	    	  /*Writing key to eliminate the unnecessesary pages*/
	    	  context.write(new Text(urlLinks[0]),new Text("SingleNode"));
	      }
	   }
	   
	   public static class Reduce2 extends Reducer< Text , Text,  Text ,  Text > {
	      @Override 
	      public void reduce( Text linkName,  Iterable<Text > words,  Context context)
	         throws IOException,  InterruptedException {
	    	  
	    	String node = linkName.toString();  
	    	String urlsTo = "";  
	    	Double PageRank = 0.0;
	    	Boolean flag = false;
	    	  
	    	for(Text word: words){
	    		String wordName = word.toString();			    		
	    		/*storing the out links for any link*/
	    		if(wordName.startsWith("<urls>")){
	    			urlsTo = wordName.substring(6, wordName.length());
	    		}
	    		
	    		/*detect the unnecessary pages*/
	    		else if(wordName.equals("SingleNode")){
	    			flag = true;
	    		}			    		
	    		/*calculae page rank page rank values*/
	    		else{
	    			PageRank += Double.parseDouble(wordName);
	    		}
	    	}
	    	
	    	/*Calculating  the final page rank value with d=0.85
	    	 */
	    	PageRank = 0.15 + 0.85*(PageRank);			    	
	    	if(flag){
	    		if(urlsTo==""){
		    		context.write(new Text(node), new Text("" + PageRank));
		    	}				    	
		    	else{
		    		context.write(new Text(node.concat("<#").concat(urlsTo)), new Text("" + PageRank));
		    	}
	    	}
	      }
	   }

}
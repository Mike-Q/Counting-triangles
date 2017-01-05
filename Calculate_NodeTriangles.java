package org.apache.hadoop.experiment_04;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class Calculate_NodeTriangles extends Configured implements Tool
{
    // Parses line into (String, Long) pairs.
    public static class TextLongPairsMapper extends Mapper<LongWritable, Text, Text, LongWritable>
    {
        Text mKey = new Text();
        LongWritable mValue = new LongWritable();

        public void map( LongWritable key, Text value, Context context )
            throws IOException, InterruptedException
        {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            if ( tokenizer.hasMoreTokens() ) {
                mKey.set( tokenizer.nextToken() );
                if ( !tokenizer.hasMoreTokens() )
                    throw new RuntimeException( "invalid intermediate line " + line );
                mValue.set( Long.parseLong( tokenizer.nextToken() ) );
                context.write(mKey, mValue);
            }
        }
    }

    // recognise the triangles for every node.
    public static class RecogniseTrianglesReducer extends Reducer<Text, LongWritable, LongWritable, LongWritable>
    {
    	boolean flag = false;
        final static LongWritable one = new LongWritable(1);
        long []vArray = new long[4096];
        int size = 0;

        public void reduce( Text key, Iterable<LongWritable> values, Context context )
            throws IOException, InterruptedException
        {
            Iterator<LongWritable> vs = values.iterator();
            // 2-paths value='node', original edge value=-1.
            flag = false;
            for ( size = 0; vs.hasNext(); ) {               
                if ( vArray.length == size ) {
                        vArray = Arrays.copyOf( vArray, vArray.length*2 );
                }
                long v = vs.next().get();
                if (v == -1) {
                	flag = true;
                	continue;
                }
                vArray[size++] = v;                   
            }
            if ( flag ) 
            	 for ( int i=0; i<size; ++i )
                     context.write( new LongWritable( vArray[i] ) , one );           
        }
    }

    // count the number of triangles for every node.
    public static class CountTrianglesReducer extends Reducer<Text, LongWritable, Text, LongWritable>
    {
        public void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException
        {
            long sum = 0;
            Iterator<LongWritable> vs = values.iterator();
            while ( vs.hasNext() ) {
                sum += vs.next().get();
            }
            context.write( key, new LongWritable(sum) );
        }
    }

    // Takes two arguments, the edges file and output file. 
    // File must be of the form: long <whitespace> long <newline>
    //setNumReduceTasks(2);
    public int run(String[] args) throws Exception
    {
        Job job2 = new Job( getConf() );
        job2.setJobName( "Edges_Matches" );

        job2.setMapOutputKeyClass( Text.class );
        job2.setMapOutputValueClass( LongWritable.class );

        job2.setOutputKeyClass( LongWritable.class );
        job2.setOutputValueClass( LongWritable.class );

        job2.setJarByClass( Calculate_NodeTriangles.class );
        job2.setMapperClass( TextLongPairsMapper.class );
        job2.setReducerClass( RecogniseTrianglesReducer.class );

        job2.setInputFormatClass( TextInputFormat.class );
        job2.setOutputFormatClass( TextOutputFormat.class );

        FileInputFormat.addInputPath( job2, new Path( args[0] ) );
        FileOutputFormat.setOutputPath( job2, new Path( "Edge_Match" ) );


        Job job3 = new Job( getConf() );
        job3.setJobName( "Count_Triangles" );

        job3.setMapOutputKeyClass( Text.class );
        job3.setMapOutputValueClass( LongWritable.class );

        job3.setOutputKeyClass( LongWritable.class );
        job3.setOutputValueClass( LongWritable.class );

        job3.setJarByClass( Calculate_NodeTriangles.class );
        job3.setMapperClass( TextLongPairsMapper.class );
        job3.setReducerClass( CountTrianglesReducer.class );

        job3.setInputFormatClass( TextInputFormat.class );
        job3.setOutputFormatClass( TextOutputFormat.class );

        FileInputFormat.setInputPaths( job3, new Path( "Edge_Match" ) );
        FileOutputFormat.setOutputPath( job3, new Path( args[1] ) );

        int ret = job2.waitForCompletion(true) ? 0 : 1;
        if ( ret==0 ) ret = job3.waitForCompletion(true) ? 0 : 1;
        return ret;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run( new Configuration(), new Calculate_NodeTriangles(), args );
        System.exit( res );
    }
}

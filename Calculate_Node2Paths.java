package org.apache.hadoop.experiment_03;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class Calculate_Node2Paths extends Configured implements Tool
{
    // Maps values to Long,Long pairs. 
    public static class ParseLongLongPairsMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable>
    {
        LongWritable mKey = new LongWritable();
        LongWritable mValue = new LongWritable();

        public void map( LongWritable key, Text value, Context context )
            throws IOException, InterruptedException
        {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer( line );
            long e1,e2;
            if ( tokenizer.hasMoreTokens() ) {
                e1 = Long.parseLong( tokenizer.nextToken() );
                if ( !tokenizer.hasMoreTokens() )
                    throw new RuntimeException( "invalid edge line " + line );
                e2 = Long.parseLong( tokenizer.nextToken() );
                // Input contains reciprocal edges, need two.
                if ( e1 < e2 ) {
                    mKey.set( e1 );
                    mValue.set( e2 );
                    context.write( mKey, mValue );
                    context.write( mValue, mKey );
                }
            }
        }
    }

    // Counts the number of 2paths for every node.
    public static class CountDegreeReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable>
    {  
        public void reduce( LongWritable key, Iterable<LongWritable> values, Context context )
            throws IOException, InterruptedException
        {
            long sum = 0;
            Iterator<LongWritable> vs = values.iterator();
            while ( vs.hasNext() ) {
                sum++;
                vs.next();
            }
            
            context.write( key, new LongWritable ( sum*(sum-1)/2 ) );
        }
    }

    // Takes two arguments, the edges file and output file. 
    // File must be of the form: long <whitespace> long <newline>
    //setNumReduceTasks(2);
    public int run(String[] args) throws Exception
    {
        Job job1 = new Job( getConf() );
        job1.setJobName( "Calculate 2Paths" );

        job1.setMapOutputKeyClass( LongWritable.class );
        job1.setMapOutputValueClass( LongWritable.class );

        job1.setOutputKeyClass( LongWritable.class );
        job1.setOutputValueClass( LongWritable.class );

        job1.setJarByClass( Calculate_Node2Paths.class );
        job1.setMapperClass( ParseLongLongPairsMapper.class );
        job1.setReducerClass( CountDegreeReducer.class );

        job1.setInputFormatClass( TextInputFormat.class );
        job1.setOutputFormatClass( TextOutputFormat.class );

        FileInputFormat.addInputPath( job1, new Path(args[0]) );
        FileOutputFormat.setOutputPath( job1, new Path(args[1]) );

        return job1.waitForCompletion( true ) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run( new Configuration(), new Calculate_Node2Paths(), args );
        System.exit( res );
    }
}
package org.apache.hadoop.experiment_02;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class Calculate_NodeCC extends Configured implements Tool
{
    // Maps values to Long,Long pairs. 
    public static class ParseLongLongPairsMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable>
    {
        LongWritable mKey = new LongWritable();
        LongWritable mValue = new LongWritable();

        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
        {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            long e1,e2;
            if (tokenizer.hasMoreTokens()) {
                e1 = Long.parseLong(tokenizer.nextToken());
                if (!tokenizer.hasMoreTokens())
                    throw new RuntimeException("invalid edge line " + line);
                e2 = Long.parseLong(tokenizer.nextToken());
                    mKey.set(e1);
                    mValue.set(e2);
                    context.write(mKey,mValue);              
            }
        }
    }

    // Calculate the Clustering Coefficient for every node.
    public static class CountClusteringCoefficient extends Reducer<LongWritable, LongWritable, LongWritable, FloatWritable>
    {  
        public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException
        {
            long fact_1, fact_2;
            Iterator<LongWritable> vs = values.iterator();
            if ( vs.hasNext() )
            	fact_1 = vs.next().get();
            else  throw new RuntimeException("invalid degree or triangles for the node " + key.get());
            if ( vs.hasNext() )
            	fact_2 = vs.next().get();
            else fact_2 = 0;  
            if (fact_2 == 0	)
            	context.write( key, new FloatWritable( (float) 0 ) );
            else if ( (float)fact_2/fact_1 > 1 ) 
            	context.write( key, new FloatWritable( (float)fact_1/fact_2 ) );
            else 
            	context.write( key, new FloatWritable( (float)fact_2/fact_1 ) );
        }
    }

    // Takes two arguments, the input file and output file. 
    //setNumReduceTasks(2);
    public int run(String[] args) throws Exception
    {
    	Job job1 = new Job( getConf() );
        job1.setJobName( "Calculate Node_CC" );

        job1.setMapOutputKeyClass( LongWritable.class );
        job1.setMapOutputValueClass( LongWritable.class );

        job1.setOutputKeyClass( LongWritable.class );
        job1.setOutputValueClass( FloatWritable.class );

        job1.setJarByClass( Calculate_NodeCC.class );
        job1.setMapperClass( ParseLongLongPairsMapper.class );
        job1.setReducerClass( CountClusteringCoefficient.class );

        job1.setInputFormatClass( TextInputFormat.class );
        job1.setOutputFormatClass( TextOutputFormat.class );

        FileInputFormat.addInputPath( job1, new Path(args[0]) );
        FileInputFormat.addInputPath( job1, new Path(args[1]) );
        FileOutputFormat.setOutputPath( job1, new Path(args[2]) );

        return job1.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run( new Configuration(), new Calculate_NodeCC(), args );
        System.exit( res );
    }
}
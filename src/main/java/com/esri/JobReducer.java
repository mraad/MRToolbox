package com.esri;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 */
public class JobReducer extends Reducer<Text, IntWritable, Text, IntWritable>
{
    @Override
    protected void reduce(
            final Text key,
            final Iterable<IntWritable> values,
            final Context context) throws IOException, InterruptedException
    {
        int sum = 0;
        for (final IntWritable intWritable : values)
        {
            sum += intWritable.get();
        }
        context.write(key, new IntWritable(sum));
    }
}

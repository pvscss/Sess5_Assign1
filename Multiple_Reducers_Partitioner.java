package Task1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class Multiple_Reducers_Partitioner extends Partitioner<Text,IntWritable>
{
	public int getPartition(Text key,IntWritable value,int noReduceTasks)
	{
		Character ch=Character.toUpperCase((char) key.charAt(0));
		
		if(ch.equals('A')||ch.equals('B')||ch.equals('C')||ch.equals('D')||ch.equals('E')||ch.equals('F'))
		{
			return 0;
		}
		else if(ch.equals('G')||ch.equals('H')||ch.equals('I')||ch.equals('J')||ch.equals('K')||ch.equals('L'))
		{
			return 1;
		}
		else if(ch.equals('M')||ch.equals('N')||ch.equals('O')||ch.equals('P')||ch.equals('Q')||ch.equals('R'))
		{
			return 2;
		}
		else
		{
			return 3;
		}
	}

}

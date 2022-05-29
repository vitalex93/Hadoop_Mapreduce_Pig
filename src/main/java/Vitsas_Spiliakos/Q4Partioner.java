package Vitsas_Spiliakos;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class Q4Partioner extends Partitioner<Q4WritableComparable, Text> {

    @Override
    public int getPartition(Q4WritableComparable q4WritableComparable, Text text, int i) {
        return Math.abs(q4WritableComparable.getCategory().hashCode() % i);
    }
}


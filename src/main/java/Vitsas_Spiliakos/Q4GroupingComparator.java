package Vitsas_Spiliakos;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class Q4GroupingComparator extends WritableComparator{
    public Q4GroupingComparator() {
        super(Q4WritableComparable.class, true);
    }

    public int compare(WritableComparable wc1, WritableComparable wc2) {
        Q4WritableComparable q4WritableComparable = (Q4WritableComparable) wc1;
        Q4WritableComparable q4WritableComparable2 = (Q4WritableComparable) wc2;
        return q4WritableComparable.getCategory().compareTo(q4WritableComparable2.getCategory());
    }

}

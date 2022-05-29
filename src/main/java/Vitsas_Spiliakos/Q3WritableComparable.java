package Vitsas_Spiliakos;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Calendar;


public class Q3WritableComparable implements WritableComparable<Q3WritableComparable> {
    @Getter @Setter private IntWritable rank, ID, age;
    @Getter @Setter private FloatWritable income, mntWines;
    @Getter @Setter private Text education, maritalStatus ;
    private IntWritable yearBirth;


    //default constructor for (de)serialization
    public Q3WritableComparable() {
        rank = new IntWritable(0);
        ID = new IntWritable(0);
        yearBirth = new IntWritable(0);
        education = new Text("");
        maritalStatus = new Text("");
        income = new FloatWritable(0);
        mntWines = new FloatWritable(0);
        age = new IntWritable(0);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        rank.write(dataOutput);
        ID.write(dataOutput);
        yearBirth.write(dataOutput);
        education.write(dataOutput);
        maritalStatus.write(dataOutput);
        income.write(dataOutput);
        mntWines.write(dataOutput);
        age.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        rank.readFields(dataInput);
        ID.readFields(dataInput);
        yearBirth.readFields(dataInput);
        education.readFields(dataInput);
        maritalStatus.readFields(dataInput);
        income.readFields(dataInput);
        mntWines.readFields(dataInput);
        age.readFields(dataInput);
    }


    public IntWritable getYearBirth() {
        return this.yearBirth;
    }

    public void setYearBirth(IntWritable yearBirth) {
        this.yearBirth = yearBirth;
        this.age = new IntWritable(Calendar.getInstance().get(Calendar.YEAR) - this.yearBirth.get());
    }


    @Override
    public int compareTo(Q3WritableComparable o) {
        int compareValue = this.mntWines.compareTo(o.mntWines);
        if (compareValue == 0){
            compareValue = this.income.compareTo(o.income);
            if (compareValue == 0) {
                compareValue = this.ID.compareTo(o.ID);
            }
        }
        return compareValue;
    }

    @Override
    public String toString(){
        return rank.toString()+"\t"+ ID.toString()+"\t"+ getAge().toString()+"\t"+ education.toString()+"\t"+ maritalStatus.toString()+ "\t" +income.toString()+"\t"+ mntWines.toString();
    }
}

package Vitsas_Spiliakos;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class Q4WritableComparable implements WritableComparable<Q4WritableComparable> {
    @Getter
    @Setter
    private IntWritable ID, numWebPurchases, numCatalogPurchases, numStorePurchases;
    @Getter
    @Setter
    private FloatWritable income, mntWines, mntFruits, mntMeatProducts, mntFishProducts, mntSweetProducts, mntGoldProds;
    @Getter
    @Setter
    private Text dt_Customer, category;

    //default constructor for (de)serialization
    public Q4WritableComparable() {
        ID = new IntWritable(0);
        dt_Customer = new Text("");
        numWebPurchases = new IntWritable(0);
        numCatalogPurchases = new IntWritable(0);
        numStorePurchases = new IntWritable(0);
        mntWines = new FloatWritable(0);
        mntFruits = new FloatWritable(0);
        mntMeatProducts = new FloatWritable(0);
        mntFishProducts = new FloatWritable(0);
        mntSweetProducts = new FloatWritable(0);
        mntGoldProds = new FloatWritable(0);
        income = new FloatWritable(0);
        category = new Text("");
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        ID.write(dataOutput);
        dt_Customer.write(dataOutput);
        numWebPurchases.write(dataOutput);
        numCatalogPurchases.write(dataOutput);
        numStorePurchases.write(dataOutput);
        mntWines.write(dataOutput);
        mntFruits.write(dataOutput);
        mntMeatProducts.write(dataOutput);
        mntFishProducts.write(dataOutput);
        mntSweetProducts.write(dataOutput);
        mntGoldProds.write(dataOutput);
        income.write(dataOutput);
        category.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        ID.readFields(dataInput);
        dt_Customer.readFields(dataInput);
        numWebPurchases.readFields(dataInput);
        numCatalogPurchases.readFields(dataInput);
        numStorePurchases.readFields(dataInput);
        mntWines.readFields(dataInput);
        mntFruits.readFields(dataInput);
        mntMeatProducts.readFields(dataInput);
        mntFishProducts.readFields(dataInput);
        mntSweetProducts.readFields(dataInput);
        mntGoldProds.readFields(dataInput);
        income.readFields(dataInput);
        category.readFields(dataInput);
    }

    public FloatWritable getMoneyPerPurchase(){
        float totalSpent = mntWines.get() + mntFruits.get() + mntMeatProducts.get() + mntFishProducts.get() + mntSweetProducts.get() + mntGoldProds.get();
        float totalPurchases = numWebPurchases.get()+ numStorePurchases.get() + numCatalogPurchases.get();
        if (totalPurchases > 0){
            return new FloatWritable(Math.round(totalSpent/totalPurchases));
        }else{
            return new FloatWritable(0);
        }
    }

    @Override
    public int compareTo(Q4WritableComparable o) {
        int compareValue = this.category.compareTo(o.category);
        if (compareValue == 0){
            compareValue = this.ID.compareTo(o.ID);
        }
        return compareValue;
    }

    @Override
    public String toString() {
        return ID.toString();
    }
}

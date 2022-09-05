package com.org.mapReduce.reduceJoin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TableBean implements Writable {

    private String id;
    private String pid;
    private int amount;
    private String pName;
    private String flag;

    public TableBean() {
    }

    public TableBean(String id, String pid, int amount, String pName, String flag) {
        this.id = id;
        this.pid = pid;
        this.amount = amount;
        this.pName = pName;
        this.flag = flag;
    }

    public String getId() {
        return id;
    }

    public String getPid() {
        return pid;
    }

    public int getAmount() {
        return amount;
    }

    public String getpName() {
        return pName;
    }

    public String getFlag() {
        return flag;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public void setpName(String pName) {
        this.pName = pName;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    @Override
    public String toString() {
        return id + "\t" + pName + "\t" + amount;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(id);
        out.writeUTF(pid);
        out.writeInt(amount);
        out.writeUTF(pName);
        out.writeUTF(flag);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readUTF();
        this.pid = in.readUTF();
        this.amount = in.readInt();
        this.pName = in.readUTF();
        this.flag = in.readUTF();
    }
}

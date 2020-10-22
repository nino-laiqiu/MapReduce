package com.tongyongtao.BigData.MapReduce.JoinCase;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * User: tongyongtao
 * Date: 2020-10-08
 * Time: 22:56
 */
public class TableBean1 implements Writable {
    private int ID;
    private String Pid;
    private int amount;
    private String Pname;
    private String Flog;

    @Override
    public String toString() {
        return
                "ID=" + ID +
                        " Pid=" + Pid +
                        " amount=" + amount +
                        " Pname=" + Pname +
                        " Flog=" + Flog;

    }

    public int getID() {
        return ID;
    }

    public void setID(int ID) {
        this.ID = ID;
    }

    public String getPid() {
        return Pid;
    }

    public void setPid(String pid) {
        Pid = pid;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getPname() {
        return Pname;
    }

    public void setPname(String pname) {
        Pname = pname;
    }

    public String getFlog() {
        return Flog;
    }

    public void setFlog(String flog) {
        Flog = flog;
    }

    public TableBean1(int ID, String pid, int amount, String pname, String flog) {
        this.ID = ID;
        Pid = pid;
        this.amount = amount;
        Pname = pname;
        Flog = flog;
    }

    public TableBean1() {
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(ID);
        dataOutput.writeUTF(Pid);
        dataOutput.writeInt(amount);
        dataOutput.writeUTF(Pname);
        dataOutput.writeUTF(Flog);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.ID = dataInput.readInt();
        this.Pid = dataInput.readUTF();
        this.amount = dataInput.readInt();
        this.Pname = dataInput.readUTF();
        this.Flog = dataInput.readUTF();
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {

        return new TableBean1(getID(), getPid(), getAmount(), getPname(), getFlog());
    }
}

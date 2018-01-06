package com.epam.bigdata101.module1.homework3.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class StringIntTuple implements WritableComparable<StringIntTuple> {
	private String left;
	private int right; 
	
	public StringIntTuple() {}
	
	public StringIntTuple(String left, int right) {
		this.left = left;
		this.right = right;
	}
	
	public void set(String left, int right) {
		this.left = left;
		this.right = right;
	}
	
	public String getLeft() {
		return this.left;
	}
	
	public int getRight() {
		return this.right;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(left);
		out.writeInt(right);		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		left = in.readUTF();
		right = in.readInt();		
	}
	
	@Override
	public String toString() {
		return left + ":" + right;	
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int hashCode = 1;
		hashCode = left.hashCode();
		hashCode = (int) (prime * hashCode + ((long) right ^ ((long) right >>> 32)));
		return hashCode;
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof StringIntTuple) {
            return this.left.equals(((StringIntTuple) obj).getLeft()) && this.right == ((StringIntTuple) obj).getRight();
        }
        return false;
	}
	
	@Override
	public int compareTo(StringIntTuple obj) {
		int res = this.left.compareTo(obj.getLeft());
		if (res == 0) {return this.right < obj.getRight() ? -1 : (this.right > obj.getRight() ? 1 : 0);}
		else {return res;}
	}

}

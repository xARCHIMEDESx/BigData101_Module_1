package com.epam.bigdata101.module1.homework2.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class FloatIntTuple implements WritableComparable <FloatIntTuple>{
	
	private float left;
	private int right; 
	
	public FloatIntTuple() {}
	
	public FloatIntTuple(float left, int right) {
		this.left = left;
		this.right = right;
	}
	
	public void set(float left, int right) {
		this.left = left;
		this.right = right;
	}
	
	public float getLeft() {
		return this.left;
	}
	
	public int getRight() {
		return this.right;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeFloat(left);
		out.writeInt(right);		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		left = in.readFloat();
		right = in.readInt();		
	}
	
	@Override
	public String toString() {
		return left + "," + right;		
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int hashCode = 1;
		hashCode = (int) (prime * hashCode + right);
		hashCode = (int) (prime * hashCode + ((long) left ^ ((long) left >>> 32)));
		return hashCode;
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof FloatIntTuple) {
            return this.left == ((FloatIntTuple) obj).getLeft() && this.right == ((FloatIntTuple) obj).getRight();
        }
        return false;
	}
	
	@Override
	public int compareTo(FloatIntTuple obj) {
		return(this.left < obj.getLeft() ? -1 : (this.left > obj.getLeft() ? 1 : 
			(this.right < obj.getRight() ? -1 : (this.right > obj.getRight() ? 1 : 0))));	
	}

}

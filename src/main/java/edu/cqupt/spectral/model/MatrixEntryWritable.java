package edu.cqupt.spectral.model;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public   class MatrixEntryWritable implements WritableComparable<MatrixEntryWritable> {
    private int row;
    private int col;
    private double val;

    public int getRow() {
      return row;
    }

    public void setRow(int row) {
      this.row = row;
    }

    public int getCol() {
      return col;
    }

    public void setCol(int col) {
      this.col = col;
    }

    public double getVal() {
      return val;
    }

    public void setVal(double val) {
      this.val = val;
    }

    @Override
    public int compareTo(MatrixEntryWritable o) {
        if (row > o.row) {
            return 1;
        } else if (row < o.row) {
            return -1;
        } else {
            if (col > o.col) {
                return 1;
            } else if (col < o.col) {
                return -1;
            } else {
                return 0;
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof MatrixEntryWritable)) {
            return false;
        }
        MatrixEntryWritable other = (MatrixEntryWritable) o;
        return row == other.row && col == other.col;
    }

    @Override
    public int hashCode() {
        return row + 31 * col;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(row);
        out.writeInt(col);
        out.writeDouble(val);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        row = in.readInt();
        col = in.readInt();
        val = in.readDouble();
    }

    @Override
    public String toString() {
        return "(" + row + ',' + col + "):" + val;
    }
}
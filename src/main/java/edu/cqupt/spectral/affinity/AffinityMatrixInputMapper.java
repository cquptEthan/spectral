/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.cqupt.spectral.affinity;

import edu.cqupt.spectral.model.MatrixEntryWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.regex.Pattern;

public class AffinityMatrixInputMapper
    extends Mapper<LongWritable, Text, IntWritable,MatrixEntryWritable> {
  private static final Pattern COMMA_PATTERN = Pattern.compile(",");
  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


    String[] elements = COMMA_PATTERN.split(value.toString());

    // enforce well-formed textual representation of the graph
    if (elements.length != 3) {
      throw new IOException("Expected input of length 3, received "
                            + elements.length + ". Please make sure you adhere to "
                            + "the structure of (i,j,value) for representing a graph in text. "
                            + "Input line was: '" + value + "'.");
    }
    if (elements[0].isEmpty() || elements[1].isEmpty() || elements[2].isEmpty()) {
      throw new IOException("Found an element of 0 length. Please be sure you adhere to the structure of "
          + "(i,j,value) for  representing a graph in text.");
    }

    // parse the line of text into a DistributedRowMatrix entry,
    // making the row (elements[0]) the key to the Reducer, and
    // setting the column (elements[1]) in the entry itself
    MatrixEntryWritable toAdd = new MatrixEntryWritable();
    IntWritable row = new IntWritable(Integer.valueOf(elements[0]));
    toAdd.setRow(-1); // already set as the Reducer's key
    toAdd.setCol(Integer.valueOf(elements[1]));
    toAdd.setVal(Double.valueOf(elements[2]));
    context.write(row, toAdd);
  }
}

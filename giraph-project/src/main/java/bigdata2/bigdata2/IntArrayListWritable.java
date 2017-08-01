package bigdata2.bigdata2;

import org.apache.giraph.utils.ArrayListWritable;
import org.apache.hadoop.io.IntWritable;

@SuppressWarnings("serial")
public class IntArrayListWritable extends ArrayListWritable<IntWritable> {
/** Default constructor for reflection */
public IntArrayListWritable() {
  super();
}
/** Set storage type for this ArrayListWritable */
@Override
public void setClass() {
  setClass(IntWritable.class);
}
}

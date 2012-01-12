package wordCo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.*;

public class WordCoWritable implements WritableComparable<WordCoWritable> {

	public Text first;
	public Text second;
	
	
	
	public WordCoWritable() {
		
	}

	public WordCoWritable(String left, String right) {
		this.first.set(left);
		this.second.set(right);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(first.toString());
		out.writeUTF(second.toString());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.first = new Text(in.readUTF());
		this.second = new Text(in.readUTF());
	}

	@Override
	public int compareTo(WordCoWritable o) {
		int i = first.compareTo(o.first);
		if (i != 0)
			return i;
		else{
			return second.compareTo(o.second);
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((first == null) ? 0 : first.hashCode());
		result = prime * result + ((second == null) ? 0 : second.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		WordCoWritable other = (WordCoWritable) obj;
		if (first == null) {
			if (other.first != null)
				return false;
		} else if (!first.equals(other.first))
			return false;
		if (second == null) {
			if (other.second != null)
				return false;
		} else if (!second.equals(other.second))
			return false;
		return true;
	}

	
}

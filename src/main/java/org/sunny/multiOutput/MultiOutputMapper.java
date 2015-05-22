package org.sunny.multiOutput;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

//+----------------+-------------+------+-----+---------+-------+
//| Field          | Type        | Null | Key | Default | Extra |
//+----------------+-------------+------+-----+---------+-------+
//| orderNumber    | int(11)     | NO   | PRI | NULL    |       |
//| orderDate      | date        | NO   |     | NULL    |       |
//| requiredDate   | date        | NO   |     | NULL    |       |
//| shippedDate    | date        | YES  |     | NULL    |       |
//| status         | varchar(15) | NO   |     | NULL    |       |
//| comments       | text        | YES  |     | NULL    |       |
//| customerNumber | int(11)     | NO   | MUL | NULL    |       |
//+----------------+-------------+------+-----+---------+-------+


public class MultiOutputMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	
	static final int LINE_WIDTH = 40;
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	
	 private MultipleOutputs mos;
	 public void setup(Context context) {	 
		 mos = new MultipleOutputs(context);
	 }
	
	@Override
	public void map(LongWritable key, Text val, Context context)
	      throws IOException, InterruptedException {
	    String line = val.toString();
	    String[] fields = line.split("\u0001");	    
	    Date orderDate = null;
	    try {
			orderDate = sdf.parse(fields[1]);
		} catch (ParseException e) {
			return;
		}
	    Calendar cal = new GregorianCalendar();
	    cal.setTime(orderDate);
	    int year = cal.get(Calendar.YEAR);
	    int month = cal.get(Calendar.MONTH);
	    
	    mos.write("text",NullWritable.get(), val, generateFileName(year,month));
	}
	
	@Override
	public void cleanup(Context c) throws IOException, InterruptedException {
		 mos.close();
	}
	
	private String generateFileName(int year, int month) {
		return "year=" + year + "/month=" + month + "/";
	}
	
}

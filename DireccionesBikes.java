package bicing.pig;



import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Properties;


import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableInputFormat;
import org.apache.hadoop.hbase.mapred.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.Export;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.client.Scan;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Progressable;

import org.apache.hadoop.fs.Path;




public class DireccionesBikes {

	StringBuilder responseBuilder = new StringBuilder();

	String csvFile = "/home/cloudera/proyecto/nodejs/node-v0.10.17/node_modules/ejemplos/static/bicisdirecciones1.csv";
	public static final String pathTabla = "hdfs:///localhost.localdomain:8020/user/cloudera/direccionbikes.out/part-m-00000";


	private void  direccionesBikes() throws IOException 
	{
		File f;
		Properties props = new Properties();
		props.setProperty("fs.default.name","hdfs://localhost.localdomain:8020");
		props.setProperty("mapred.job.tracker","localhost.localdomain:8021");
		String cabecera = "0";
		String tituloscabecera = null;
		String nuevoFichero = "0";
		BufferedWriter bw = null;
		String data = null;

		String filePathDirectory="/home/cloudera/proyecto/nodejs/node-v0.10.17/node_modules/ejemplos/static/bicisdirecciones1.csv";
		String filePathDirectory1="hdfs://localhost.localdomain:8020/user/cloudera/direccionbikes.out";
		BufferedWriter bufferWritter = null;
		Path path=new Path(filePathDirectory1);


		Configuration conf = new Configuration();
		conf.addResource(new Path("/etc/hadoop/conf.cloudera.hdfs1/core-site.xml"));

		FileSystem hdfs = null;
		try {
			hdfs = FileSystem.get(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


		FileStatus [] dir = hdfs.listStatus(path);

		for (FileStatus fileStatus : dir)  {



			String s  = fileStatus.getPath().getName();

			if (s.contains("part-m-00000")) {

				FSDataInputStream in = hdfs.open(fileStatus.getPath());
				f = new File(filePathDirectory);

				cabecera = "0";
				nuevoFichero = "0";

				if (!f.exists()){

					f.createNewFile();
					cabecera = "1";
					nuevoFichero ="1";
					tituloscabecera = "date"+","+"value";
				}



				byte[] buffer = new byte[1024];

				in.read(buffer);
				in.seek(0);

				List<String> lines = IOUtils.readLines(in);
				try{
					for (String cadena :lines){
						String[] valores = cadena.split(" ");
						int  len = cadena.length();
						String[] temp = null;
						temp = cadena.split(" ");
						int numeroelemento = 1;
						String  fecha="";
						String numerobicis="";
						for(int i=0;i<len;i++) {
							char c1 = cadena.charAt(i);
							int c2 = Character.getNumericValue(c1);
							if (c2==-1) {
								numeroelemento = numeroelemento + 1;
							}    
							if (numeroelemento==1) {
								fecha = fecha+c1;
							}
							if (numeroelemento==2) {
								numerobicis = numerobicis+c1;
								numerobicis = numerobicis.trim();
							}
						}   

						data = fecha+","+numerobicis;


						if (nuevoFichero.equals("1")) {
							bw = new BufferedWriter(new FileWriter(filePathDirectory));
							bw.write(cabecera);
							bw.newLine();
							bw.flush();
						}
						else {

							bw = new BufferedWriter(new FileWriter(filePathDirectory,true));

							bw.write(data);

							bw.newLine();

							bw.flush();


						}



					}


				}catch (IOException ioe) {

					ioe.printStackTrace();
				}  finally {
					if (bw != null) try {
						bw.close();

					} catch (IOException ioe2){

						ioe2.printStackTrace();
					}

				}	
			}
		}
	}


	public static void main(String[] args) {

		DireccionesBikes db= new DireccionesBikes();
		try {
			db.direccionesBikes();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}	                        

}




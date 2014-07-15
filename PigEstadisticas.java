


import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.hadoop.hbase.HBaseStorage;

import org.apache.commons.io.IOUtils;


public class PigEstadisticas {
	
	private static Configuration conf  = null;        
	private static Configuration conf1  = null; 
	private static Configuration conf2  = null; 
	public static final String tableName="TotalBicis";
	public static final String columnFamility1="total";
	public static final String columnFamility2="direcciones";
	public static final String columnFamility3="fechas";
	final static String Name = "export";
	private static HBaseAdmin admin = null;
	public static  HTable tabla = null;

	public   String cargarEstadisticas(long fechamvtoL) {
		try {


			Properties props = new Properties();
			props.setProperty("fs.default.name","hdfs://localhost.localdomain:8020");
			props.setProperty("mapred.job.tracker","localhost.localdomain:8021");

			PigServer pigServer = new PigServer("local"); 
			runMyQuery(pigServer,"hbase://DatosBicing");
			return new String("1");

		}
		catch (IOException e) {

			e.printStackTrace();
			return new String("0");
		}
		catch(Exception e ){


			e.printStackTrace();
			return new String("0");
		}

	}
	public static void runMyQuery(PigServer pigServer, String inputFile) throws IOException {

		String s;



		try {
			Class exampleClass = Class.forName("org.apache.pig.backend.hadoop.hbase.HBaseStorage");


		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


		DateFormat dateformat = new SimpleDateFormat("ddMMyyyyhhmmss");
		Calendar cal = Calendar.getInstance();
		String currentfecha = dateformat.format(cal.getTime());
		Long  fecha = Long.valueOf(currentfecha).longValue();


		Date date1 = new Date(Long.MIN_VALUE);

		SimpleDateFormat dateformatddMMyyyy = new SimpleDateFormat("ddMMyyyyhhmmss");

		StringBuilder nowddMMyyyy = new StringBuilder(dateformatddMMyyyy.format(date1));
		Date date2 = new Date(Long.MAX_VALUE);

		SimpleDateFormat dateformatddMMyyyy1 = new SimpleDateFormat("ddMMyyyyhhmmss");

		StringBuilder nowddMMyyyy1 = new StringBuilder(dateformatddMMyyyy1.format(date2));


		pigServer.registerQuery("source = load '"+ inputFile + "' USING  org.apache.pig.backend.hadoop.hbase.HBaseStorage( " +
				"'datos:bikes , datos:fecha  datos:street', '-loadKey true')"+
				"as (id1:int, bikes:int, fecha:chararray, street:chararray) ;"); 



		Path output = new Path("/home/cloudera/proyecto/consulta1");


		conf = new Configuration(true);


		conf.addResource(new Path("/etc/hadoop/conf.cloudera.hdfs1/core-site.xml"));


		FileSystem hdfs  = FileSystem.get(conf);


		String filePathDirectory="hdfs://localhost.localdomain:8020/user/cloudera/consultaBicing1";

		Path path=new Path(filePathDirectory);


		hdfs.delete(path);



		pigServer.store("source",path.toString());




		FileStatus [] dir = hdfs.listStatus(path);

		for (FileStatus fileStatus : dir)  {



			String s1  = fileStatus.getPath().getName();

			if (s1.contains("part-m-00000")) {


				if (hdfs.exists(new Path("hdfs://localhost.localdomain:8020/user/cloudera/consultaBicing1/numerobicis1"))) {


				}

				else {
					System.out.println("no existe");
				}

				hdfs.rename(fileStatus.getPath(),  new Path("hdfs://localhost.localdomain:8020/user/cloudera/consultaBicing1/numerobicis1"));




			}


		}

		hdfs.close();

		conf = new Configuration(true);

		conf.addResource(new Path("/etc/hadoop/conf.cloudera.hdfs1/core-site.xml"));


		hdfs  = FileSystem.get(conf);


		String filePathString="hdfs://localhost.localdomain:8020/user/cloudera/consultaBicing1/numerobicis1";
		Path path1 = new Path(filePathString);



		conf1 = HBaseConfiguration.create();
		admin = new HBaseAdmin(conf1);


		if (admin.tableExists(tableName)) {
			tabla = new HTable(conf1,tableName);
		}
		else {

			HTableDescriptor desc = new HTableDescriptor(tableName);
			HColumnDescriptor coldef1 = new HColumnDescriptor(columnFamility1);
			HColumnDescriptor coldef2 = new HColumnDescriptor(columnFamility2);
			HColumnDescriptor coldef3 = new HColumnDescriptor(columnFamility3);


			coldef1.setMaxVersions(300);
			desc.addFamily(coldef1);
			desc.addFamily(coldef2);
			desc.addFamily(coldef3);
			admin.createTable(desc);

			tabla = new HTable(conf1,tableName);
		}



		if (hdfs.exists(path1)) {

			boolean anadir = true;
		}
		else {

			boolean anadir = false;
		}
		FileStatus [] dir1 = hdfs.listStatus(path1); 




		int valorkey = 1;

		for (FileStatus fileStatus1 :dir1) {



			FSDataInputStream in = hdfs.open(fileStatus1.getPath());


			byte[] buffer = new byte[1024];
			in.read(buffer);
			in.seek(0);

			List<String> lines = IOUtils.readLines(in);

			for (String cadena :lines) {
				String cadena1 = cadena + "\n";


				int pos = cadena1.indexOf(" ", 1);


				int l = cadena1.length();


				int pos1 = cadena1.indexOf("\b", 1);



				String[] valores = cadena1.toString().split(" ");
				String direccion = null;


				String[] valores1 = new String[5];
				int contadorblancos = 0;
				String valor1 = "";
				String valor2 = "";
				String valor3 = "";
				String valor4 = "";

				for ( int i= 0;i<cadena1.length();i++){

					char  c = cadena1.charAt(i);
					String elemento = c+"";
					if (elemento.replaceAll("\\d+","").length() >0 ) {
						System.out.println("elemento "+elemento+" if se cumple mayor 0");
					}
					else {
						System.out.println("elemento "+elemento+" if no se cumple");
					}
					if (elemento.equals("")){
						System.out.println("elemento blanco "+elemento);
					}
					else {
						System.out.println("elemento no es blanco "+elemento);
					}

					int i1 = elemento.compareTo(" ");

					if (i1<0 ){

						contadorblancos = contadorblancos + 1;


					}
					else {

						if (contadorblancos == 0){
							valor1 = valor1 + elemento;

						}
						if (contadorblancos == 1){
							valor2 = valor2 + elemento;

						}
						if (contadorblancos == 2){
							valor3 = valor3 + elemento;

						}
						if (contadorblancos == 3){
							valor4 = valor4 + elemento;

						}
					}

				}
				Long valorkeyL =new Long(valorkey);
				String valorkeyS = Long.toString(valorkey);

				Put put1 = new Put(Bytes.toBytes(valorkeyS), Long.valueOf(valor3));

				put1.add(Bytes.toBytes("total"), Bytes.toBytes("totalbicis"), Bytes.toBytes(valor2));
				put1.add(Bytes.toBytes("direcciones"), Bytes.toBytes("direccion"), Bytes.toBytes(valor4));
				put1.add(Bytes.toBytes("fechas"), Bytes.toBytes("fecha"), Bytes.toBytes(valor3));
				tabla.put(put1);
				valorkey = valorkey + 1;


			}

			tabla.close();

			in.close();


		}


	}
   
	public static void main(String[] args) {


		//PigEstadisticas pg = new PigEstadisticas();
		//pg.cargarEstadisticas();

	}	                        
			
   
}
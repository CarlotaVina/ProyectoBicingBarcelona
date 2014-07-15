register ConsultaPigFechas1.jar;
register pig.jar;
fs -rm -R /user/cloudera/datebikes.out
row = load 'hbase://DatosBicing' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('datos:bikes', '-minTimestamp $Fecha -maxTimestamp $Fecha1') as (id1:int, bikes:int);
B = FOREACH row GENERATE consultas.pig.ConsultaPigFechas1(bikes);
store row into 'datebikes.out';

	 
register pig.jar;
%declare DESC 'Gran Via Corts Catalanes'
fs -rm -R /user/cloudera/direccionbikes.out
row = load 'hbase://TotalBicis' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('fechas:fecha total:totalbicis direcciones:direccion') as  (fecha:chararray, totalbicis:int, direccion:chararray);
B = FILTER row by direccion matches '$NombreCalle';
store B into 'direccionbikes.out';

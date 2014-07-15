var http = require("http");
var fs = require("fs");
var WebHDFS = require("webhdfs");
var hbase = require('hbase');
var sys = require('sys');
var exec = require('child_process').exec;
var child;
var hdfs = WebHDFS.createClient();
var jsdom = require("jsdom");
var express = require('express');
var app = express();
var d3 = require('d3');
app.engine('.html', require('ejs').__express);
app.set('views',__dirname);
app.set('view engine','html');

//var server = http.createServer(function(req, res) {
app.get('/', function(req, res) {

 
var invoked = false;

if (req.url=='/') {
   console.log('raiz');
    res.write('<a id="totalbicis" href="totalbicis">Bicis direcciones</a><br>');
    res.write('<a id="fechas" href="fechas">Elegir fechas</a><br>');
   res.write('<a id="graficofechas" href="graficofechas">Visualizar datos fechas </a><br>');
   res.write('<a id="graficodirecciones" href="graficodirecciones">Visualizar datos direcciones </a><br>');
  
   res.end();
 
   }
 
  
 app.get('/totalbicis', function(req, res) {
    
      var buf = '<div style=\"color:green;\">' + 'Direccion </div>';
  res.write('<html><body><p>' + buf + '</p></body><html>');
  var body = 
              '<HTML>'+
              '<HEAD>' +
              '<TITLE>Test Input</TITLE>' +
              '<SCRIPT LANGUAGE="JavaScript">' +
              '</SCRIPT>' +
              '</HEAD>' +
              '<BODY>' + 
               '<FORM NAME="fechas" ACTION="/direccionbicis" METHOD="POST">' +
                'Introduce nombre de calle<br>' +
                '<INPUT TYPE="text" NAME="inputcalle" VALUE=""><P>' +
                '<INPUT TYPE="submit" Value="enviar formulario">' +
                '<INPUT TYPE="reset" Value="borrar">' +
                '</FORM>' +
                '</BODY>' +
                 '</HTML>';
     
             res.write(body);
   
         res.end(); 
       
   
 hbase()
 
  .getRow('TotalBicis', '1')
 .get('total:totalbicis',  function(error, valor){
              
               var tmp = '<div style=\"color:red;\">'+JSON.stringify(valor) + '</div>';
               res.write('<html><body><p>' + tmp+ '</p></body><html><br>')
              
              res.end();
             
  });           
  });
  app.get('/fechas', function(req, res) {

    
    
     var body = 
              '<HTML>'+
              '<HEAD>' +
              '<TITLE>Test Input</TITLE>' +
              '<SCRIPT LANGUAGE="JavaScript">' +
              '</SCRIPT>' +
              '</HEAD>' +
              '<BODY>' + 
                
               '<FORM NAME="fechas" ACTION="/mensajeFechas" METHOD="POST">' +
               
                'Elige fechas desde y hasta <br>' +
                '<INPUT TYPE="text" NAME="inputbox" VALUE=""><P>' +
                '<INPUT TYPE="text" NAME="inputbox1" VALUE=""><P>' +
                '<INPUT TYPE="submit" Value="enviar formulario">' +
                '<INPUT TYPE="reset" Value="borrar">' +
               
                '</FORM>' +
                 
                '</BODY>' +
                 '</HTML>';
                 
         res.writeHead(200, {"Content-Type": "text/html"});
       res.write(body);
   
         res.end(); 
       
      
});
app.get('/cargadatos', function(req, res) { 

   
    child = exec ("java -cp nuevo1.jar:PigEstadisticas.jar:hadoop-common-2.0.0-cdh4.4.0.jar:pig.jar:commons-logging-1.1.1.jar:log4j-1.2.16.jar:hadoop-mapreduce-client-core.jar:commons-configuration-1.10.jar:commons-lang-2.5.jar:hadoop-common.jar:hadoop-auth.jar:pig-0.11.0-cdh4.4.0.jar bicing.pig.PigEstadisticas", function (error, stdout, stderr) {
           sys.print('stdout: ' + stdout);
           sys.print('stderr: ' + stderr);
           if (error != null) {
                  console.log('exec error: '+error);
           }
      })   
    
    
    res.end();
  
   
}); 

app.post('/mensajeFechas', function(req, res) { 

 
   if (req.method == 'POST' ) {
         
          
    
         req.on('data', function(chunk) {
               
               var fecha = chunk.toString();
               var len = fecha.length;
               var fecha1 = fecha.substr(9,len);
               
               var posicion = fecha.indexOf('&');
               
               var fecha2 = fecha.substr(0,22);
               var len2 = fecha2.length;
               var fecha3 = fecha.substr(23,len);
               var len3 = fecha3.length;
               
               var posicion1 = fecha2.indexOf('=');
             
               var posicion2 = fecha3.indexOf('=');
               
               var fechadesde = fecha2.substr(posicion1+1,len2);
               var fechahasta = fecha3.substr(posicion2+1,len3);
              
               var fechahasta1 = new String(fechahasta);
              
               fechahasta2 = fechahasta1.valueOf();
               fechahasta21 =parseInt(fechahasta2);
               
               fechahasta22 = fechahasta21 + 1;
               
               fechahasta2 = fechahasta2 + 1;
               
               var fechahasta3 = fechahasta2.toString();
               child = exec ("pig -t ColumnMapKeyPrune -x mapreduce -p Fecha="+fechadesde+" -p Fecha1="+fechahasta22+" PigConsultasFechas.pig" , function (error) {
                     if (error != null) {
                     console.log("exec error: "+error);
                      }
                 });
                  req.on('end', function() {
            
                           
          });
                    
             });  
                          
    } else {
       console.log("else");
       console.log("[405] " +req.method + " to " + req.url);
       res.writeHead(405, "Method not supported ", {'Content-Type': 'text/html'});
       res.end('Method ot supported</title></head><body><h1>Method not supported.</h1></body></html>');
  }
      
       res.writeHead(200, {"Content-Type": "text/html"});
             res.end();
      
      
         
});
app.get('/graficofechas', function(req, res) { 


var localFileStream = fs.createWriteStream('/home/cloudera/proyecto/nodejs/node-v0.10.17/node_modules/ejemplos/static/totalbicis7.csv');
                 var remoteFileStream = hdfs.createReadStream('/user/cloudera/datebikes.out/part-m-00000');
                 
                 localFileStream.write(" \n");
                 remoteFileStream.on('error', function onError (err) {
                           console.log('error');
                  });
                  
                  remoteFileStream.on('data',function onChunk(chunk) {
                        
                         var linea = chunk.toString();
               var len = linea.length;
               var posicion1 = linea.indexOf('\n');
               var posicion2 = posicion1 + 1;
               var linea1 = linea.substr(posicion2,len);
               var elemento = linea.substr(0,posicion1);
              
               while (posicion1!=0) {
                   localFileStream.write(elemento);
                  
                  var posicionanterior = posicion1;
                  posicion1 =linea1.indexOf('\n');
                  posicion2 = posicion1 + 1
                  elemento = linea.substr(posicionanterior,posicion1)
                  linea1 = linea.substr(posicionanterior,len);
                
                  
               }  
               
               });
               remoteFileStream.on('finish', function onFinish() {
                   
               });
               
             
                         app.use('/csv',express.static(__dirname + '/static'));
                         res.render('grafico1');
                         
                        res.end();

          
      
 });
 app.post('/direccionbicis', function(req, res) {
 
           
             req.on('data', function(chunk) {
               
               var calle = chunk.toString();
               var posicion = calle.indexOf('=');
              
               var len = calle.length;
              
               var calle1 = calle.substr(posicion+1,len);
               var temp = new Array();
               temp = calle1.split('+');
                
               var numeroelementos = temp.length;
              
               var parametrocalle='';
               for (i=0; i<numeroelementos ; i++) {
                     if (temp[i] !='' && temp[i] !=' ') {
                     
                     
                     if (i==0) {
                         parametrocalle=temp[i];
                      }
                      else {
                      parametrocalle = parametrocalle +' '+temp[i];
                      }
                    
                    }
                }
               
              child = exec ("pig -t ColumnMapKeyPrune -x mapreduce -p NombreCalle=\"'"+parametrocalle+"'\" PigConsultasDireccion.pig" , function (error) {
                   if (error != null) {
                     console.log("exec error: "+error);
                      }
                  });
              
             child = exec ("java -cp bicingpig.jar:hadoop-common-2.0.0-cdh4.4.0.jar:pig.jar:commons-logging-1.1.1.jar:log4j-1.2.16.jar:hadoop-mapreduce-client-core.jar:commons-configuration-1.10.jar:commons-lang-2.5.jar:hadoop-common.jar:hadoop-auth.jar:pig-0.11.0-cdh4.4.0.jar:hadoop-hdfs.jar:protobuf-java-2.4.0a.jar bicing.pig.DireccionesBikes", function (error, stdout, stderr) {
           sys.print('stdout: ' + stdout);
           sys.print('stderr: ' + stderr);
           if (error != null) {
                  console.log('exec error: '+error);
           }
      })    
             var terminado = 0;
            
                    
                
   
               res.end();      
              });               
                                                     
         req.on('end', function() {
             
             res.end();
              
  });       
           
 }); 
  app.get('/graficodirecciones', function(req, res) { 


                            app.use('/csv',express.static(__dirname + '/static'));
                         res.render('grafico9');
                         
                        res.end();
 
        
 
        
      
 });

 });

app.listen(8000); 

 
 
 
 

     
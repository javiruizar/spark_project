# Ejecutar desde ./docker
## Levantar contenedores
`docker compose up`

Solo se levanta la primera vez, de ahi en adelante siempre se levantaran automaticamente los contenedores al arrancar el pc

## Abrir una terminal del contenedor de Kafka
`docker exec -it kafka bash`

### Desde la terminal del contenedor crear los topics:
    kafka-topics --create --topic streaming-query --bootstrap-server localhost:9092 --partitions 1 --replication-factor "1"
    kafka-topics --create --topic output --bootstrap-server localhost:9092 --partitions 1 --replication-factor "1"

### Abrir productor de Kafka
    kafka-console-producer --topic streaming-query --bootstrap-server localhost:9092

## En otra terminal:
    docker exec -it kafka bash
### Desde la terminal del contenedor abrir consumidor de Kafka:
    kafka-console-consumer --topic output --bootstrap-server localhost:9092 --from-beginning

## Abrir una terminal del contenedor de mongo. 
Desde la shell de mongoDb crear db, coleccion y añadir algunos documentos a Mongo. 
`docker exec -it mongo mongosh`

### Desde la terminal de mongo:
    use elmercado
    var variosArticulos=[{"id_articulo":"0001", "nombre_articulo":"iphone 11 128Gb", "palabras_clave":["iphone","11","128gb","iphone11"],"caracteristicas_venta":{"marca":"Apple","precio":999,"almacenamiento":"128Gb","memoria":"4 Gb", "tamano":"6 pulgadas","conector":"lightning"}},{"id_articulo":"0002", "nombre_articulo":"conga 4690", "palabras_clave":["conga","conga 4690","4690","aspiradora", "robot limpieza", "robot aspirador","robot"],"caracteristicas_venta":{"marca":"Cecotec","precio":200,"potencia succion":"alta","autonomia":"8 horas","peso":"3 kilogramos","conector":"lightning"}}]
    db.articulos
    db.articulos.insert(variosArticulos)

Para borrar los datos de la coleccion de mongo:
    db.articulos.deleteMany({})
## En una terminal desde la raiz del proyecto
`mvn clean package`

`sh scripts/run-streaming.sh`

### Escribir alguna busqueda en el productor de Kafka. El streaming debe devolver en el consumer los articulos que coincidan
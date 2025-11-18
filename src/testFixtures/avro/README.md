
To generate avro 
```shell
java -jar ~/tools/avro-tools-1.12.0.jar idl event.idl > even.avro
```

To generate code 
```shell
java -jar ~/tools/avro-tools-1.12.0.jar compile schema event.avro ../java/
```


# Guía Práctica de HDFS (Hadoop Distributed File System)

## 1. Ejercicios prácticos

### Ejercicio 1: Gestión básica de archivos en HDFS

1. Crea un directorio en HDFS llamado `/curso/ejercicio1`.
2. Sube un archivo de texto local a este directorio.
3. Cambia los permisos del archivo para que sólo el propietario pueda leer y escribir.
4. Mueve el archivo a un subdirectorio dentro de `ejercicio1`.
5. Elimina el archivo y el subdirectorio.

### Ejercicio 2: Replicación y comprobación de bloques

1. Sube un archivo grande (más de 128 MB) a HDFS.
2. Verifica el factor de replicación del archivo.
3. Cambia el factor de replicación a 2.
4. Utiliza `hdfs fsck` para verificar la distribución de bloques del archivo en los DataNodes.

### Ejercicio 3: Monitoreo y diagnóstico de HDFS

1. Accede a la interfaz web del NameNode para revisar el estado general de HDFS.
2. Ejecuta el comando `hdfs dfsadmin -report` y analiza la salida.
3. Utiliza `hdfs fsck` para analizar la salud del sistema de archivos en HDFS.
4. Configura una alerta simple para cuando el uso del espacio en HDFS supere el 80% (no es necesario que configures el cron, solo escribir el scrip).

### Ejercicio 4: Análisis de datos con el dataset Iris

1. Descarga el dataset Iris como se muestra en la sección 9.
2. Usa el comando hdfs dfs -cat para ver las primeras 10 líneas del dataset.
3. Crea una copia del dataset en HDFS con un factor de replicación de 3.
4. Usa hdfs fsck para verificar la distribución de los bloques de la copia.

### Ejercicio 5: Gestión de cuotas y espacio

1. Establece una cuota de 10 MB para el directorio /cuota_test.
2. Intenta subir un archivo que supere esta cuota y observa el resultado.
3. Usa hdfs dfs -count -q para verificar la cuota establecida y el espacio utilizado.
4. Elimina la cuota y vuelve a intentar subir el archivo.


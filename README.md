# Co-Purchase Analysis con Apache Spark

## Descrizione del Progetto

Questo progetto implementa un'analisi di co-acquisto utilizzando Scala ed Apache Spark, valutando la frequenza con cui due prodotti vengono acquistati insieme nello stesso ordine. 

L'obiettivo principale è confrontare le performace in un ambiente distributo sfruttando la comupatazione cloud, quindi viene testato su Google Cloud Platform (GCP) con Dataproc.

## Requisiti

- Apache Spark
- Scala
- Java (JDK 8 o superiore)
- Google Cloud SDK (per esecuzione su GCP)

## Dataset Input

Il dataset è un file CSV in cui ogni riga rappresenta un acquisto:
```
ordine_id,prodotto_id
1,12
1,14
2,8
2,12
...
```
Il programma calcola le coppie di prodotti che appaiono negli stessi ordini e il numero di volte in cui ciò accade.

## Esecuzione 
i.e. I seguenti comandi sono da lanciare dall'interno dela cartella 'CoPurchaseAnalysis'

### Eseguire in locale il progetto
Per eseguire il progetto in locale basta eseguire il comando:
```
sbt "run <path/file/caricare>"
```

Nel caso venisse mostrato un errore java.lang.IllegalAccessError sulla classe sun.nio.ch.DirectBuffer eseguire il comando:
```
sbt -J--add-exports=java.base/sun.nio.ch=ALL-UNNAMED run
```

Queto errore è dovuto al fatto che la classe menzionata non è esportata dal modulo java.base. 
Questo problema è comune con versioni di Java 9+ a causa delle restrizioni sui moduli.

### Eseguire su cluster GCP Dataproc il progetto

1. Compilare il progetto con il comando:
```
sbt package
```

2. Caricare i file su Google Cloud Storage (GCS):
```
gsutil cp order_products.csv gs://<my-bucket>/jars
gsutil cp target/scala-2.12/co-purchase-analysis.jar gs://<my-bucket>/
```

3. Creare un cluster Dataproc:
```
gcloud dataproc clusters create <my-cluster> \
    --region=<region> \
    --master-machine-type=<machine-type> \
    --num-workers=<num-worker> \
    --image-version=2.0-debian10 \
    --project=<project-id>
```

4. Eseguire il job su Dataproc:
```
gcloud dataproc jobs submit spark \
    --project=<project-id> \
    --cluster=<my-cluster> \
    --region=<region> \
    --class=copurchase.analysis.Main \
    --jars=gs://<my-bucket>/jars/copurchaseanalysis_2.12-0.1.jar  \
    -- gs://<my-bucket>/in/order_products.csv
```
L'ultimo argomento corrisponde al path che si assume sia su un bucket GCP

5. Spegnere il cluster 
```
gcloud dataproc clusters delete <my-cluster> --region=<region> --project=<project-id>
```

#### Note: 
- le parole contrassenate da '<>' (es. ```<project-id>```) sono da personalizzare con i propri dati del cluster
- il parametro --project=```<project-id>``` viene inserito solamente se il progetto predefinito nelle configurazioni locali è diverso da quello in cui si sta lavorando, altrimenti non è necessario 

## Dataset Output

L'output viene salvato in formato CSV con righe del tipo:
```
prodotto_1,prodotto_2,frequenza
13176,47209,62341
13176,21137,61628
...
```

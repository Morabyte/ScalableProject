# Co-Purchase Analysis con Apache Spark

## Descrizione del Progetto

Questo progetto implementa un'analisi di co-acquisto utilizzando Apache Spark, valutando la frequenza con cui due prodotti vengono acquistati insieme nello stesso ordine. L'obiettivo principale è confrontare due implementazioni:

Versione Inefficiente: utilizza groupByKey(), causando un alto consumo di memoria e problemi di scalabilità.

Versione Ottimizzata con RDD: utilizza reduceByKey() per un'elaborazione più efficiente.

L'analisi viene eseguita in un ambiente distribuito, testato su Google Cloud Platform (GCP) con Dataproc.

## Dataset

Il dataset è un file CSV in cui ogni riga rappresenta un acquisto:

ordine_id,prodotto_id
1,12
1,14
2,8
2,12
...

Il programma calcola le coppie di prodotti che appaiono negli stessi ordini e il numero di volte in cui ciò accade.

## Confronto tra Versioni

Metrica                     | Versione Inefficiente                     | Versione Ottimizzata (RDD)
--------------------------- | ----------------------------------------- | -----------------------------------------------------

Tempo di esecuzione         | Alto (molta latenza)                      | Inferiore (ridotta latenza)

Utilizzo della memoria      | Alto (groupByKey() genera grandi liste)   | Ottimizzato (reduceByKey() limita lo shuffle)

Efficienza dello shuffle    | Elevato overhead                          | Minimo (uso ottimizzato di RDD)

Scalabilità                 | Scarsa (rallenta con dataset grandi)      | Ottima (distribuisce il carico in modo efficiente)

## Requisiti

- Apache Spark
- Scala
- Java (JDK 8 o superiore)
- Google Cloud SDK (per esecuzione su GCP)

## Esecuzione Locale

```
# Compilare il progetto
sbt package

# Eseguire la versione inefficiente
spark-submit --class CoPurchaseAnalysisSlow \
    --master local[4] \
    target/scala-2.12/co-purchase-analysis.jar \
    input.csv output_slow/

# Eseguire la versione ottimizzata con RDD
spark-submit --class CoPurchaseAnalysisRDD \
    --master local[4] \
    target/scala-2.12/co-purchase-analysis.jar \
    input.csv output_rdd/
```
## Esecuzione su GCP Dataproc

1. Creare un cluster Dataproc:
```
gcloud dataproc clusters create my-cluster \
    --region europe-west1 \
    --num-workers 3 \
    --image-version 2.0-debian10 \
    --scopes cloud-platform
```
2. Caricare i file su Google Cloud Storage (GCS):
```
gsutil cp input.csv gs://my-bucket/input.csv
gsutil cp target/scala-2.12/co-purchase-analysis.jar gs://my-bucket/
```
3. Eseguire il job su Dataproc:
```
gcloud dataproc jobs submit spark \
    --cluster my-cluster \
    --class CoPurchaseAnalysisRDD \
    --jars gs://my-bucket/co-purchase-analysis.jar \
    -- gs://my-bucket/input.csv gs://my-bucket/output_rdd/
```

## Output

L'output viene salvato in formato CSV con righe del tipo:
```
prodotto_1,prodotto_2,frequenza
8,12,2
12,14,3
...
```
## Benchmark

Per valutare le prestazioni:
```
# Contare le righe di output
tail -n +1 output_rdd/* | wc -l

# Controllare i tempi di esecuzione nei log di Spark
```

# Conclusioni

La versione ottimizzata con RDD migliora significativamente il tempo di esecuzione e l'uso della memoria, rendendola adatta a dataset di grandi dimensioni su cluster distribuiti.


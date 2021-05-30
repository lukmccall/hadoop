# MapReduce

## Setup

<details><summary>Pierwszym krokiem jest sklonowanie repozytorium, które pozwoli na uruchomienie Hadoopa przy wykorzystaniu Dockera.</summary>
<p>

```sh
git clone https://github.com/lukmccall/hadoop.git
```

</p>
</details>
<details><summary>Następnie należy uruchomić Hadoopa przy wykorzystaniu pliku docker-compose.yml</summary>
<p>

```sh
docker-compose up -d
```

</p>
</details>
<details><summary>Konieczne jest połącznie się z shellem kontenera namenode.</summary>
<p>

```sh
docker exec -it namenode bash
```

</p>
</details>

## Pierwszy MapReduce

Nasz pierwszy MapReduce będzie bardzo podstawowym przykładem wykorzystania MapReduce, który widoczny jest w domumentacji
Hadoopa - będzie to WordCount, czyli program, którego celem jest zliczenie wystąpień poszczegółnych słów w danym
tekście. Wspomniany program znajduje się w pliku WordCount.java.

### Zadanie

<b>Wykonaj operacje MapReduce przy wykorzystaniu programu WordCount.java na danych znajdujących się w
[pliku](./input/wordcount/lorem.txt).</b>

<details><summary>Zbuduj plik .jar przy wykorzystaniu mavena.</summary>
<p>

```sh
mvn clean package
```

</p>
</details>

<details><summary>Skopiuj otrzymany plik .jar do kontenera.</summary>
<p>

```sh
docker cp ./target/map-reduce.jar namenode:/map-reduce
```

Skrypt copy-jar.sh wykonuje wspomniane polecenie.
</p>
</details>

<details><summary>Skopiuj plik wejściowy (input/wordcount/lorem.txt) do kontenera.</summary>
<p>

```sh
docker cp ./input/wordcount/lorem.txt namenode:/
```

</p>
</details>

<details><summary>Stwórz folder input w HDFS.</summary>
<p>

```sh
hdfs dfs -mkdir -p input
```

</p>
</details>

<details><summary>Skopiuj lokalny plik wejściowy (lorem.txt) do HDFS (do folderu input).</summary>
<p>

```sh
hdfs dfs -put ./lorem.txt input
```

</p>
</details>

<details><summary>Uruchom operacje MapReduce.</summary>
<p>

```sh
hadoop jar map-reduce/map-reduce.jar hadoop.MR_1_WordCount input/lorem.txt output01
```

</p>
</details>

<details><summary>Otwórz plik powstały w wyniku operacji.</summary>
<p>

```sh
hdfs dfs -cat output01/part-r-00000
```

</p>
</details>

<details><summary>Przeglądnij operacje z wykorzystaniem historyserver.</summary>
<p>

Konieczne jest otworzenie [linku](http://localhost:9875/applicationhistory) w przeglądarce, następnie wybranie jobu, co
pozwoli na przeglądanie szczegółów np. logów.

</p>
</details>

### Dodatkowe źródła:

- https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html

## Logowanie w MapReduce

Logowanie w poprzednim przykładzie nie zawierało zbyt wielu informacji, które pozwalałby określić co dzieje się podczas
operacji Map i Reduce. Można jednak to zmienić, ponieważ wszystkie informacje logowane przez aplikacje są zapisywane i
możliwe do zobaczenia w logach danej operacji MapReduce.

### Zadanie

<b>Aby się o tym przekonać, dodaj kilka wywołań System.out oraz System.err do wcześniejszego programu, następnie uruchom
go na danych znajdujących się w [pliku](./input/wordcount/lorem.txt), a następnie sprawdź w jaki sposób można przeglądać
te logi z wykorzystaniem historyserver.</b>

<details><summary>Edytuj plik WordCount.java i dodaj w nim wywołania System.out i System.err.</summary>
<p>

```java
    public static class TokenizerMapper extends Mapper<Object, Text, Text, LongWritable> {

    private final static LongWritable ONE = new LongWritable(1);
    private final Text word = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            System.out.printf("Running mapping on %s%n", word);
            System.err.print("Printing on error level");
            context.write(word, ONE);
        }
    }
}
```

</p>
</details>

<details><summary>Wykonując operacje z poprzedniego punktu uruchom operacje MapReduce.</summary>
<p>

Konieczne jest:

- zbudowanie pliku .jar przy wykorzystaniu Maven
- skopiowanie otrzymanego pliku .jar do kontenera
- uruchomienie operacji MapReduce

```sh
hadoop jar map-reduce/map-reduce.jar hadoop.MR_2_Logging input/lorem.txt output02
```

</p>
</details>

<details><summary>Przeglądnij uzyskane logi z wykorzysaniem historyserver.</summary>
<p>

Konieczne jest otworzenie [linku](http://localhost:9875/applicationhistory) w przeglądarce, następnie wybranie jobu, co
pozwoli na przeglądanie szczegółów np. logów. Kolejnym krokiem jest wybranie <b>Attempt ID</b>, a następnie wybranie
jakiegoś z kontenerów, co pozwoli na zobaczenie logów. <b>W linku trzeba podmienić http://historyserver:8188/ na
http://localhost:9875/ .</b>

</p>
</details>

## Łączenie MapReduce

Wiele złożonych zadań można podzielić na prostsze podzadania, z których każde jest realizowane przez indywidualny
MapReduce. Na przykład znalezienie najczęściej występującego słowa w tekście z wykorzystywanego przez nas pliku
wejściowego. Można to zrobić jako sekwencyjne wykonanie dwóch MapReduce. Pierwszy z nich tworzy zbiór, który mówi, o tym
ile razy dane słowo występuje w tekście, a drugi MapReduce zajmuje się posortowaniem informacji. Chociaż można po prostu
wykonać te dwa zadania ręcznie, jedno po drugim, wygodniej jest to zautomatyzować. Istnieje możliwość połączenia kilku
MapReduce, tak aby były uruchamiane sekwencyjnie, przy czym dane wyjściowe jednego MapReduce są danymi wejściowymi do
następnego. Łączenie zadań MapReduce w łańcuch jest analogiczne do potoków uniksowych. (Czyli wywołania mapreduce-1 |
mapreduce-2 | mapreduce-3 | ...)

### Zadanie

<b>Zmodyfikuj poprzednio używany program WordCount tak, aby po zakończeniu pierwszego jobu MapReduce uruchamiał kolejny,
którego zadaniem będzie posortowanie uzyskanych w poprzednim kroku wartości w kolejności ich występowania.</b>

<details><summary>Skonfiguruj pierwszy job.</summary>
<p>

Oprócz konfiguracji z poprzednich zadań konieczne jest:

- ustawienie OutputFormatClass na SequenceFileOutputFormat (domyślny to TextOutputFormat, który zapisuje pary
  klucz-wartość jako Text, poprzez wykonanie .toString() na kluczu i wartości, są one oddzielone w domyśle tabulatorem,
  ale można to zmienić)
- wywołanie metody job.waitForCompletion(verbose)

```java
        Job job = Job.getInstance(configuration, "Word Count 1.2 - Counting");

        job.setJarByClass(MR_3_JobChaining.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(LongSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
```

SequenceFileOutputFormat - Output Format, który zapisuje dane do pliku sekwencyjnego (Sequence File), który składa się z
par klucz-wartość poddanych procesowi serializacji. W tym formacie przechowywane są dane pomiędzy fazami Map i Reduce,
dlatego idealnie nadają się do naszego przypadku, gdzie po operacji Reduce dane zostaną przekazane do następnego Map.
Deserializacją danych zajmuje się SequenceFileInputFormat.

</p>
</details>

<details><summary>Stwórz KeyValueSwappingMapper, którego zadaniem jest zamiana klucza i wartości w parze.</summary>
<p>

Oprócz konfiguracji z poprzednich zadań konieczne jest:

- ustawienie OutputFormatClass na SequenceFileOutputFormat (domyślny to TextOutputFormat, który zapisuje pary
  klucz-wartość jako Text, poprzez wykonanie .toString() na kluczu i wartości, są one oddzielone w domyśle tabulatorem,
  ale można to zmienić)
- wywołanie metody job.waitForCompletion(verbose)

```java
    public static class KeyValueSwappingMapper extends Mapper<Text, LongWritable, LongWritable, Text> {

    @Override
    public void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
        context.write(value, key);
    }

}
```

</p>
</details>

<details><summary>Skonfiguruj drugi job.</summary>
<p>

Oprócz konfiguracji z poprzednich zadań konieczne jest:

- ustawienie InputFormatClass na SequenceFileInputFormat
- ustawienie MapperClass na stworzonego wcześniej KeyValueSwappingMapper
- ustawienie SortComparatorClass jako LongWritable.DecreasingComparator
- ustawienie ReducerClass na Reducer (IdentityReducer)

```java
        Job job2 = Job.getInstance(configuration, "Word Count 1.2 - Sorting");

        job2.setJarByClass(MR_3_JobChaining.class);

        job2.setInputFormatClass(SequenceFileInputFormat.class);

        job2.setMapperClass(KeyValueSwappingMapper.class);
        job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);
        job2.setReducerClass(Reducer.class);

        job2.setOutputKeyClass(LongWritable.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
```

</p>
</details>

<details><summary>Wykonując operacje z poprzedniego punktu uruchom operacje MapReduce.</summary>
<p>

Konieczne jest:

- zbudowanie pliku .jar przy wykorzystaniu Maven
- skopiowanie otrzymanego pliku .jar do kontenera
- uruchomienie operacji MapReduce (w przypadku naszego programu konieczne podanie jest 3 argumentów, a nie dwóch jak w
  poprzednim przykładzie, ponieważ musimy zdefiniować miejsce pośrednie pomiędzy pierwszym MapReduce a drugim).

```sh
hadoop jar map-reduce/map-reduce.jar hadoop.MR_3_JobChaining input/lorem.txt temp output03
```

</p>
</details>

<details><summary>Otwórz plik powstały w wyniku operacji.</summary>
<p>

```sh
hdfs dfs -cat output03/part-r-00000
```

</p>
</details>

### Dodatkowe źródła:

- https://livebook.manning.com/book/hadoop-in-action/chapter-5/6
- https://towardsdatascience.com/chaining-multiple-mapreduce-jobs-with-hadoop-java-832a326cbfa7
- https://data-flair.training/blogs/hadoop-outputformat/
- https://examples.javacodegeeks.com/enterprise-java/apache-hadoop/hadoop-sequence-file-example/

## Preprocessing i postprocessing w MapReduce

Często przed albo po samym procesie zachodzi potrzeba np. unifikacji danych. Dla naszego przypadku może to być usunięcie
znaków interpunkcyjnych jak kropki i przecinki oraz ignorowanie wielkości liter. Moglibyśmy wykorzystać wiedzę z
poprzedniego punktu i pisać osobny job MapReduce do każdej operacji preprocessingowej przy wykorzystaniu
IdentityReducer, ale nie jest to wydajne rozwiązanie i jest ono niepotrzebnie komplikowane. Dla takich operacji
sensowniejszym jest wywołanie operacji preprocessingu przed główną funkcją Map, a postprocessingu po funkcji Reduce.
Może do tego wykorzystać `ChainMapper` i `ChainReducer`.

### Zadanie

<b>Zmodyfikuj poprzednio używany program WordCount tak, aby przed operacją Map uruchamiały się nowe Mappery, których
zadaniem jest unifikacja wielkości liter oraz usunięcie znaków interpunkcyjnych, a po operacji Reduce następowała
zamiana klucza z wartością. Wykorzystaj do tego klasy `ChainMapper` i `ChainReducer`.</b>

<details><summary>Stwórz mapper odpowiedzialny za unifikacje wielkości liter.</summary>
<p>

```java
   public static class UpperCaseMapper extends Mapper<Text, LongWritable, Text, LongWritable> {

    @Override
    public void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
        context.write(new Text(key.toString().toUpperCase()), value);
    }

}
```

</p>
</details>

<details><summary>Stwórz mapper odpowiedzialny za usuwanie znaków interpunkcyjnych.</summary>
<p>

```java
    public static class InvalidCharactersMapper extends Mapper<Text, LongWritable, Text, LongWritable> {

    @Override
    public void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
        context.write(new Text(key.toString().replaceAll("[.,]", "")), value);
    }

}
```

</p>
</details>

<details><summary>Skonfiguruj ChainMapper.</summary>
<p>

```java
        ChainMapper.addMapper(job, TokenizerMapper.class, Object.class, Text.class, Text.class, LongWritable.class, configuration);
        ChainMapper.addMapper(job, UpperCaseMapper.class, Text.class, LongWritable.class, Text.class, LongWritable.class, configuration);
        ChainMapper.addMapper(job, InvalidCharactersMapper.class, Text.class, LongWritable.class, Text.class, LongWritable.class, configuration);
```

</p>
</details>

<details><summary>Skonfiguruj ChainReducer.</summary>
<p>

```java
        ChainReducer.setReducer(job, LongSumReducer.class, Text.class, LongWritable.class, Text.class, LongWritable.class, configuration);
        ChainReducer.addMapper(job, KeyValueSwappingMapper.class, Text.class, LongWritable.class, LongWritable.class, Text.class, configuration);
```

</p>
</details>

<details><summary>Wykonując operacje z poprzedniego punktu uruchom operacje MapReduce.</summary>
<p>

Konieczne jest:

- zbudowanie pliku .jar przy wykorzystaniu Maven
- skopiowanie otrzymanego pliku .jar do kontenera
- uruchomienie operacji MapReduce

```sh
hadoop jar map-reduce/map-reduce.jar hadoop.MR_4_Preprocessing input/lorem.txt output04
```

</p>
</details>

<details><summary>Otwórz plik powstały w wyniku operacji.</summary>
<p>

```sh
hdfs dfs -cat output04/part-r-00000
```

</p>
</details>

### Dodatkowe źródła:

- https://livebook.manning.com/book/hadoop-in-action/chapter-5/17
- https://www.netjstech.com/2018/07/chaining-mapreduce-job-in-hadoop.html

## Uruchomienie tylko fazy Map

W niektórych przypadkach możemy nie potrzebować wywołania Reduce, ponieważ Map jest wystarczające. Jednym z możliwych
rozwiązań takiego przypadku jest wykorzystanie IdentityReducer, ale trzeba rozumieć że nie zawsze jest to efektywne,
ponieważ faza Reduce składa się z trzech głównych operacji:

- Shuffle - Reducer kopiuje output z każdego Mappera.
- Sort - Input do operacji Reduce jest sortowany względem klucza.
- Reduce

Z tego powodu wykorzystanie IdentityReducer wykonuje dodatkowe operacje, które mogą być zbędne. W takich przypadkach
możliwe jest wykorzystanie funkcji `.setNumReduceTasks(numer)` i ustalenie liczby tasków na 0.

### Zadanie

<b>Stwórz program, którego zadaniem będzie wywołanie fazy Map (bez fazy Reduce), a w wyniku dla każdego słowa w pliku
wejściowym powstanie para słowo-1.</b>

<details><summary>Zmodyfikuj program WordCount poprzez ustawienie liczby Tasków dla fazy Reduce na 0.</summary>
<p>

```java
job.setNumReduceTasks(0);
```

</p>
</details>

<details><summary>Usuń klasę odpowiedzialną za Reduce i jej konfiguracje.</summary>
<p>

```java
    public static class LongSumReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    private final LongWritable result = new LongWritable();

    @Override
    public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (LongWritable val : values) {
            sum += val.get();
        }
        result.set(sum);
        context.write(key, result);
    }

}
```

```java
job.setReducerClass(LongSumReducer.class);
```

</p>
</details>

<details><summary>Wykonując operacje z poprzedniego punktu uruchom operacje MapReduce.</summary>
<p>

Konieczne jest:

- zbudowanie pliku .jar przy wykorzystaniu Maven
- skopiowanie otrzymanego pliku .jar do kontenera
- uruchomienie operacji MapReduce

```sh
hadoop jar map-reduce/map-reduce.jar hadoop.MR_5_MapOnly input/lorem.txt output05
```

</p>
</details>

<details><summary>Otwórz plik powstały w wyniku operacji.</summary>
<p>

```sh
hdfs dfs -cat output05/part-m-00000
```

Tym razem plik powstały w wyniku operacji nazywa się `part-m-00000`, a nie `part-r-00000` jak poprzednio. Pozwala to
ustalić, że powstał on w wykonaniu operacji Map (`m`) bez wykorzystania operacji Reduce (`r`).

</p>
</details>

### Dodatkowe źródła:

- https://hadoop.apache.org/docs/stable/api/org/apache/hadoop/mapreduce/Reducer.html

## Distributed Cache

Distributed Cache jest sposobem na skopiowanie małych plików albo archiwów do node odpowiedzialnego za wykonywanie
jakiejś operacji. Warto zaznaczyć, że wspomniane pliki będą w trybie read-only oraz że są one kopiowane raz przed
rozpoczęciem jakiegoś taska, tak aby po pierwsze ograniczyć przesyłanie danych po sieci, a po drugie aby wspomniane node
mogły wykorzystywać te pliki podczas operacji. W wypadku naszego WordCount możemy wykorzystać Distributed Cache jako
źródło informacji na temat tego jakie słowa powinny być ignorowane.

### Zadanie

<b>Zmodyfikuj program WordCount, tak aby dodawał on plik, w którym będą znajdować się słowa, które należy pominąć w
procesie MapReduce do Distributed Cache. Dodatkowo zaimplementuj w Mapperze ignorowanie tych słów.</b>

<details><summary>Dodaj path do pliku zawierającego informacje o ignorowanych słowach do Distributed Cache.</summary>
<p>

```java
job.addCacheFile(new Path(args[2]).toUri());
```

</p>
</details>

<details><summary>Zmodyfikuj TokenizerMapper tak aby przy tworzeniu zawierał listę słów do ignorowania.</summary>
<p>

```java
        private final Set<String> patternsToSkip = new HashSet<>();

        @Override
        public void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();

            URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
            for (URI patternsURI : patternsURIs) {
                Path patternsPath = new Path(patternsURI.getPath());
                String patternsFileName = patternsPath.getName();
                parseSkipFile(patternsFileName);
            }
        }

        private void parseSkipFile(String fileName) {
            try {
                BufferedReader fis = new BufferedReader(new FileReader(fileName));
                String pattern;
                while ((pattern = fis.readLine()) != null) {
                    patternsToSkip.add(pattern);
                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file '" + StringUtils.stringifyException(ioe));
            }
        }

```

</p>
</details>

<details><summary>Zmodyfikuj TokenizerMapper tak aby ignorował wybrane słowa.</summary>
<p>

```java
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String token = itr.nextToken();
                if (!patternsToSkip.contains(token)) {
                    word.set(token);
                    context.write(word, ONE);
                }
            }
        }
```

</p>
</details>

<details><summary>Skopiuj plik wejściowy (input/ignore/ignore.txt) do kontenera.</summary>
<p>

```sh
docker cp ./input/ignore/ignore.txt namenode:/
```

</p>
</details>

<details><summary>Stwórz folder input w HDFS.</summary>
<p>

```sh
hdfs dfs -mkdir -p ignore
```

</p>
</details>

<details><summary>Skopiuj lokalny plik wejściowy (ignore.txt) do HDFS (do folderu input).</summary>
<p>

```sh
hdfs dfs -put ./ignore.txt input
```

</p>
</details>

<details><summary>Uruchom operacje MapReduce.</summary>
<p>

```sh
hadoop jar map-reduce/map-reduce.jar hadoop.MR_6_DistributedCache input/lorem.txt output06 ignore/ignore.txt
```

</p>
</details>

<details><summary>Otwórz plik powstały w wyniku operacji.</summary>
<p>

```sh
hdfs dfs -cat output06/part-r-00000
```

</p>
</details>

### Dodatkowe źródła:

- https://hadoop.apache.org/docs/r2.6.3/api/org/apache/hadoop/filecache/DistributedCache.html
- https://medium.com/@bharvi.vyas123/distributed-cache-in-hadoop-how-distributed-cache-works-8f7996e179f9
- https://www.geeksforgeeks.org/distributed-cache-in-hadoop-mapreduce/
- https://data-flair.training/blogs/hadoop-distributed-cache/

## Counters

Counters zapewniają sposób mierzenia postępu lub liczby operacji, które odbywają się podczas procesu Map / Reduce.
Liczniki w MapReduce są używane np. do zbierania statystyk dotyczących MapReduce, które później mogą być wykorzystane w
procesie kontroli jakości. Są również przydatne do diagnozowania problemów. Można powiedzieć, że są podobne do
umieszczania System.out, ale bez konieczności sprawdzania w logach ile razy dana wiadomość została wypisana. Zazwyczaj
te liczniki są definiowane w programie i są zwiększane podczas wykonywania, gdy wystąpi określone zdarzenie lub warunek
(specyficzny dla tego licznika). Bardzo dobrym zastosowaniem liczników jest śledzenie prawidłowych i nieprawidłowych
rekordów z wejściowego zestawu danych. W naszym przykładzie wykorzystamy je do zliczania ile razy w przetwarzanym przez
Map słowie znajduje się znak interpunkcyjny.

### Zadanie

<b>Wykorzystując `Counter` zlicz ile razy w pliku wejściowym `lorem.txt` występuje przecinek, kropka oraz ile jest ich
łącznie.</b>

<details><summary>Zdefiniuj Counter wykorzystany do liczenia wystąpienia znaków interpunkcyjnych.</summary>
<p>

```java
    enum Punctuation {
        ANY,
        DOT,
        COMMA
    }
```

</p>
</details>

<details><summary>Zmodyfikuj TokenizerMapper tak aby liczył wystąpienia znaków interpunkcyjnych.</summary>
<p>

```java
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String token = itr.nextToken();
                if (token.endsWith(",")) {
                    context.getCounter(Punctuation.ANY).increment(1);
                    context.getCounter(Punctuation.COMMA).increment(1);
                }
                else if (token.endsWith(".")) {
                    context.getCounter(Punctuation.ANY).increment(1);
                    context.getCounter(Punctuation.DOT).increment(1);
                }

                word.set(token);
                context.write(word, ONE);
            }
        }
```

</p>
</details>

<details><summary>Skonfiguruj job tak aby po zakończeniu procesowania wypisywał do konsoli informacje o counterach.</summary>
<p>

```java
        job.waitForCompletion(true);

        for (CounterGroup group : job.getCounters()) {
            System.out.println("* Counter Group: " + group.getDisplayName() + " (" + group.getName() + ")");
            System.out.println("  number of counters in this group: " + group.size());
            for (Counter counter : group) {
                System.out.println("  - " + counter.getDisplayName() + ": " + counter.getName() + ": "+counter.getValue());
            }
        }

        System.exit(0);
 ```

</p>
</details>

<details><summary>Wykonując operacje z poprzedniego punktu uruchom operacje MapReduce.</summary>
<p>

Konieczne jest:

- zbudowanie pliku .jar przy wykorzystaniu Maven
- skopiowanie otrzymanego pliku .jar do kontenera
- uruchomienie operacji MapReduce

```sh
hadoop jar map-reduce/map-reduce.jar hadoop.MR_7_Counter input/lorem.txt output07
```

</p>
</details>

<details><summary>Otwórz plik powstały w wyniku operacji.</summary>
<p>

```sh
hdfs dfs -cat output07/part-r-00000
```

</p>
</details>

### Dodatkowe źródła:

- https://hadoop.apache.org/docs/current/api/org/apache/hadoop/mapred/Counters.html
- https://data-flair.training/blogs/hadoop-counters/
- https://www.netjstech.com/2018/07/what-are-counters-in-hadoop-mapreduce.html

## Map to class

Czasami zachodzi konieczność stworzenia własnej klasy, która reprezentuje jakieś wczytywane dane. W takich sytuacjach 
konieczne jest wykorzystanie `WritableComparable` który jest połączeniem interfejsów Writable oraz Comparable - tzn. 
obiekt, który może zostać poddany serializacji oraz dodatkowo istnieje możliwość porównywania go z innymi instancjami
tej klasy.

### Zadanie

<b>Wykorzystując `WritableComparable` zdefiniuj obiekt składający się z pól:

- int id
- String firstName
- String lastName

Wykorzystaj ten obiekt do stworzenia operacji MapReduce, którego zadaniem będzie mapowanie linii tekstu z pliku 
`input/class/class.txt` do obiektu, oraz zliczenie liczby wystąpień danego id.</b>

<details><summary>Stwórz klasę implementującą WritableComparable.</summary>
<p>

```java
    public static class One implements WritableComparable<One> {

        private int id;
        private String firstName;
        private String lastName;

        public One(int id, String firstName, String lastName) {
            this.id = id;
            this.firstName = firstName;
            this.lastName = lastName;
        }

        public One() {

        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getFirstName() {
            return firstName;
        }

        public void setFirstName(String firstName) {
            this.firstName = firstName;
        }

        public String getLastName() {
            return lastName;
        }

        public void setLastName(String lastName) {
            this.lastName = lastName;
        }

        @Override
        public void readFields(DataInput dataIp) throws IOException {
            id = dataIp.readInt();
            firstName = dataIp.readUTF();
            lastName = dataIp.readUTF();
        }

        @Override
        public void write(DataOutput dataOp) throws IOException {
            dataOp.writeInt(id);
            dataOp.writeUTF(firstName);
            dataOp.writeUTF(lastName);
        }

        @Override
        public int compareTo(One arg0) {
            return Integer.compare(id, arg0.id);
        }

        @Override
        public String toString() {
            return id + "," + firstName + "," + lastName;
        }

    }
```

</p>
</details>

<details><summary>Stwórz mapper, którego zadaniem jest mapowanie do stworzonej wcześniej klasy.</summary>
<p>

```java
    public static class OneMapper extends Mapper<Object, Text, One, LongWritable> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String token = itr.nextToken();
                String[] split = token.split(",");
                context.write(new One(Integer.parseInt(split[0]), split[1], split[2]), new LongWritable(1));
            }
        }

    }
```

</p>
</details>

<details><summary>Stwórz Reducer, który będzie zliczał wystąpienia danego id.</summary>
<p>

```java
    public static class OneReducer extends Reducer<One, LongWritable, One, LongWritable> {

        private final LongWritable result = new LongWritable();

        @Override
        public void reduce(One key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
```

</p>
</details>

<details><summary>Skonfiguruj job, aby wykorzystywał stworzone klasy.</summary>
<p>

```java
        job.setMapperClass(OneMapper.class);
        job.setReducerClass(OneReducer.class);

        job.setOutputKeyClass(One.class);
        job.setOutputValueClass(LongWritable.class);
```

</p>
</details>

<details><summary>Wykonując operacje z poprzedniego punktu uruchom operacje MapReduce.</summary>
<p>

Konieczne jest:

- zbudowanie pliku .jar przy wykorzystaniu Maven
- skopiowanie otrzymanego pliku .jar do kontenera
- uruchomienie operacji MapReduce

```sh
hadoop jar map-reduce/map-reduce.jar hadoop.MR_8_MapToClass class/class.txt output08
```

</p>
</details>

<details><summary>Otwórz plik powstały w wyniku operacji.</summary>
<p>

```sh
hdfs dfs -cat output08/part-r-00000
```

</p>
</details>

### Dodatkowe źródła

- https://hadoop.apache.org/docs/r2.6.4/api/org/apache/hadoop/io/WritableComparable.html
- https://www.programmersought.com/article/17451475188/
- https://www.edureka.co/community/23667/what-difference-between-writable-writablecomparable-hadoop
- https://timepasstechies.com/mapreduce-custom-writable-example-writablecomparable-example/


## Multiple Inputs

W niektórych przypadkach może pojawić się konieczność wczytania danych z różnych źródeł, a te dane mogą mieć różne
formaty, separatory, albo logika mapowania informacji z każdego ze źródeł może być całkiem inna. Mając takie źródła,
chcemy wykonać na wszystkich danych tę samą operację reduce. W takich przypadkach możliwe jest wykorzystanie
`MultipleInputs` która pozwala na zdefiniowane różnych mapperów dla różnych ścieżek. W naszym przypadku załóżmy, że mamy
dwa osobne pliki:

- `lorem.txt` - wykorzystywany z poprzednich zadaniach
- `mapped.txt` - nowy plik, w którym znajdują się już zmapowane dane w postaci klucz-wartość, gdzie klucz jest słowem, a
  wartość to liczba wystąpień

Chcemy wczytać dane pliki i w wyniku operacji MapReduce otrzymać połączone wyniki, które mówią o tym, ile razy dane
słowo wystąpiło, biorąc pod uwagę zarówno dane w pliku `lorem.txt` jak i `mapped.txt`.

### Zadanie

<b>Wykorzystując `MultipleInputs` stwórz operację MapReduce, która wykorzystując pliki `lorem.txt` oraz `mapped.txt`
odpowie na pytanie, ile razy występuje dane słowo.</b>

<details><summary>Stwórz IdentityMapper, którego zadaniem będzie mapowanie danych z pliku mapped.txt.</summary>
<p>

```java
    public static class IdentityMapper extends Mapper<Object, Text, Text, LongWritable> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            String newKey = itr.nextToken();
            int newValue = Integer.parseInt(itr.nextToken());
            context.write(new Text(newKey), new LongWritable(newValue));
        }
    }

}
```

</p>
</details>

<details><summary>Skonfiguruj wykorzystanie MultipleInputs.</summary>
<p>

```java
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TokenizerMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, IdentityMapper.class);
```

Dodatkowo należy usunąc linijkę mówiącą o konfiguracji `MapperClass`

```java
        job.setMapperClass(TokenizerMapper.class);
```

</p>
</details>

<details><summary>Skopiuj plik wejściowy (input/wordcount/mapped.txt) do kontenera.</summary>
<p>

```sh
docker cp ./input/wordcount/mapped.txt namenode:/
```

</p>
</details>

<details><summary>Skopiuj lokalny plik wejściowy (mapped.txt) do HDFS (do folderu input).</summary>
<p>

```sh
hdfs dfs -put ./mapped.txt input
```

</p>
</details>

<details><summary>Wykonując operacje z poprzedniego punktu uruchom operacje MapReduce.</summary>
<p>

Konieczne jest:

- zbudowanie pliku .jar przy wykorzystaniu Maven
- skopiowanie otrzymanego pliku .jar do kontenera
- uruchomienie operacji MapReduce

```sh
hadoop jar map-reduce/map-reduce.jar hadoop.MR_9_MultipleInputs input/lorem.txt input/mapped.txt output09
```

</p>
</details>

<details><summary>Otwórz plik powstały w wyniku operacji.</summary>
<p>

```sh
hdfs dfs -cat output09/part-r-00000
```

</p>
</details>

### Dodatkowe źródła:

- https://hadoop.apache.org/docs/r2.6.3/api/org/apache/hadoop/mapreduce/lib/input/MultipleInputs.html
- https://www.aegissofttech.com/articles/what-is-the-use-of-multiple-input-files-in-mapreduce-hadoop-development.html

## Join

Podczas przetwarzania danych często występuje potrzeba wykonania operacji Join. Operacja MapReduce pozwala na 
przeprowadzenie operacji Join na różnych danych na podstawie jakiegoś pola. Wykorzystamy technikę znaną jako
Reduce Side Join, w której to Reducer jest odpowiedzialny za przeprowadzenie operacji Join. Cały proces może zostać 
opisany kilkoma krokami:
- Mapper zajmuje się wczytaniem danych, które mają zostać poddane operacji Join na podstawie pola.
- Mapper procesuje input i dodaje informacje pozwalającą później na odróżnienie źródła z którego zostały przeczytane 
  dane. 
- Mapper zwraca dane w postaci klucz-wartość, gdzie klucz to pole, na podstawie którego wykonywana jest operacja Join.
- Po operacjach Shuffling i Sorting klucz oraz lista wartości zostaje przekazana do Reducera.
- Reducer dokonuje operacji Join.

### Zadanie

<b>Wykorzystując dane w plikach `input/join/one.txt` oraz `input/join/two.txt` dokonaj operacji Join, która zwróci
informacje na temat tego, jakie dana osoba wypożyczyła książki.

Schema `input/join/one.txt`: `id,imię,nazwisko,age,kierunek`.

Schema `input/join/two.txt`: `id,data,id_wypożyczającego,nazwa_książki`.</b>

<details><summary>Stwórz Mapper, którego zadaniem będzie mapowanie danych z pliku one.txt.</summary>
<p>

```java
    public static class OneMapper extends Mapper<Object, Text, Text, Text> {

  @Override
  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    StringTokenizer itr = new StringTokenizer(value.toString());
    while (itr.hasMoreTokens()) {
      String token = itr.nextToken();
      String[] parts = token.split(",");
      System.out.println("One|" + String.join(",", parts[1], parts[2], parts[3], parts[4]));
      context.write(new Text(parts[0]),  new Text("One|" + String.join(",", parts[1], parts[2], parts[3], parts[4])));
    }
  }

}
```

</p>
</details>

<details><summary>Stwórz Mapper, którego zadaniem będzie mapowanie danych z pliku two.txt.</summary>
<p>

```java
    public static class TwoMapper extends Mapper<Object, Text, Text, Text> {

  @Override
  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    StringTokenizer itr = new StringTokenizer(value.toString());
    while (itr.hasMoreTokens()) {
      String token = itr.nextToken();
      String[] parts = token.split(",");
      System.out.println("Two|" + String.join(",", parts[0], parts[1], parts[3]));
      context.write(new Text(parts[2]),  new Text("Two|" + String.join(",", parts[0], parts[1], parts[3])));
    }
  }

}
```

</p>
</details>

<details><summary>Stwórz Reducer, którego zadaniem będzie wykonanie operacji Join.</summary>
<p>

```java
    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    String beggining = "";
    List<String> list = new ArrayList<>();
    for (Text val : values) {
      String[] parts = val.toString().split("\\|");
      if (parts[0].equals("One")) {
        beggining = parts[1];
      }
      else if (parts[0].equals("Two")) {
        list.add(parts[1]);
      }
    }

    for (String element: list) {
      context.write(new Text(beggining), new Text(element));
    }
  }

}
```

</p>
</details>

<details><summary>Skonfiguruj Job tak aby wykorzystywał stworzone klasy.</summary>
<p>

```java
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, OneMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TwoMapper.class);
        job.setReducerClass(JoinReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
```

</p>
</details>

<details><summary>Skopiuj pliki wejściowe (input/join/one.txt i input/join/two.txt) do kontenera.</summary>
<p>

```sh
docker cp ./input/join/one.txt namenode:/
```

```sh
docker cp ./input/join/two.txt namenode:/
```

</p>
</details>

<details><summary>Stwórz folder join w HDFS.</summary>
<p>

```sh
hdfs dfs -mkdir -p join
```

</p>
</details>

<details><summary>Skopiuj lokalny pliki wejściowe (one.txt i two.txt) do HDFS (do folderu join).</summary>
<p>

```sh
hdfs dfs -put ./one.txt join
```

```sh
hdfs dfs -put ./two.txt join
```

</p>
</details>

<details><summary>Wykonując operacje z poprzedniego punktu uruchom operacje MapReduce.</summary>
<p>

Konieczne jest:

- zbudowanie pliku .jar przy wykorzystaniu Maven
- skopiowanie otrzymanego pliku .jar do kontenera
- uruchomienie operacji MapReduce

```sh
hadoop jar map-reduce/map-reduce.jar hadoop.MR_10_Join join/one.txt join/two.txt output10
```

</p>
</details>

<details><summary>Otwórz plik powstały w wyniku operacji.</summary>
<p>

```sh
hdfs dfs -cat output10/part-r-00000
```

</p>
</details>

### Dodatkowe źródła:

- https://dzone.com/articles/joins-mapreduce
- https://www.edureka.co/blog/mapreduce-example-reduce-side-join/

## Streaming API
Hadoop Streaming API to narzędzie, które pozwala na tworzenie i uruchamianie operacji MapReduce przy wykorzystaniu
skryptów / plików wykonywalnych jako mapperów, reducerów. Zarówno mapper, jak i reducer:

- czyta informacje ze standardowego strumienia wejścia. 
- zapisuje informacje na standardowy strumień wejścia.

### Zadanie

<b>Wykorzystując `Streaming API` wykonaj operacje MapReduce przy wykorzystaniu Mappera i Reducera zdefiniowanego w 
plikach `python/mapper.py` oraz `python/reducer.py`</b> 

<details><summary>Zainstaluj pythona na wymaganych nodach.</summary>
<p>

```sh
docker exec -it namenode bash -c "apt update && apt install python -y"
docker exec -it datanode1 bash -c "apt update && apt install python -y"
docker exec -it datanode2 bash -c "apt update && apt install python -y"
docker exec -it resourcemanager bash -c "apt update && apt install python -y"
docker exec -it nodemanager bash -c "apt update && apt install python -y"
```

</p>
</details>

<details><summary>Skopiuj pliki wejściowe (python/mapper.py i python/reducer.py) do kontenera.</summary>
<p>

```sh
docker cp ./python/mapper.py namenode:/
```

```sh
docker cp ./python/reducer.py namenode:/
```

</p>
</details>

<details><summary>Uruchom hadoop-streaming na pliku lorem.txt.</summary>
<p>

```sh
hadoop jar ./opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar \
-D mapreduce.job.name='Streaming API' \
-input input/lorem.txt \
-output output11 \
-mapper mapper.py \
-reducer reducer.py \
-file mapper.py \
-file reducer.py
```

</p>
</details>

<details><summary>Otwórz plik powstały w wyniku operacji.</summary>
<p>

```sh
hdfs dfs -cat output11/part-00000
```

</p>
</details>

### Dodatkowe źródła:

- https://hadoop.apache.org/docs/r1.2.1/streaming.html
- http://www.inf.ed.ac.uk/teaching/courses/exc/labs/hadoop_streaming.html
- https://www.r-bloggers.com/2015/02/using-hadoop-streaming-api-to-perform-a-word-count-job-in-r-and-c/
- https://www.michael-noll.com/tutorials/writing-an-hadoop-mapreduce-program-in-python/

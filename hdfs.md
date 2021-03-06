## 膯wiczenia z HDFSa

### 馃彈 Instalacja

- zainstaluje [dockera](https://docs.docker.com/desktop/)
- wykonaj `docker-compose up -d`

### 馃暩 Dost臋pne strony

- [name node](http://localhost:9870/dfshealth.html#tab-overview)
- [resource manager](http://localhost:9871/cluster)
- [history server](http://localhost:9875/applicationhistory)
- [node manager](http://localhost:9874/node)
- [data node 1](http://localhost:9872/datanode.html)
- [data node 2](http://localhost:9873/datanode.html)

### 馃捇 艁膮czenie si臋 z shellem poszczeg贸lnych serwer贸w

```sh
docker exec -it <name> bash
```

Jako <name> nale偶y wybra膰 jeden z kontener贸w:

- namenode
- nodemanager
- resourcemanger
- historyserver
- datanode1
- datanode2

![kontenery](./imgs/containers.png)

### 馃摡 Kopiowanie danych

```sh
docker cp <local path> <name>:<path>
```

### 馃捇 Komendy

- wy艣wietla aktualn膮 wersj臋

  ```sh
  hadoop version
  ```

- pomoc

  ```sh
  hadoop fs -help [command]
  ```

- tworzy katalog

  ```sh
  hadoop fs 鈥搈kdir /path/directory_name
  ```

- listowanie katalog贸w

  ```sh
  hadoop fs -ls /path
  ```

- kopiowanie lokalnych plik贸w

  ```sh
  hadoop fs -copyFromLocal <localsrc> <hdfs destination>
  ```

- pobieranie plik贸w

  ```sh
  hadoop fs -copyToLocal <hdfs source> <localdst>
  ```

- wy艣wietlanie zawarto艣ci pliku

  ```sh
  hadoop fs 鈥揷at /path_to_file_in_hdfs
  ```

- przenoszenie plik贸w

  ```sh
  hadoop fs -mv <src> <dest>
  ```

- kopiowanie plik贸w

  ```sh
  hadoop fs -cp <src> <dest>
  ```

- usuwanie plik贸w
  ```sh
  hadoop fs 鈥搑m <path>
  ```

### 馃摑 膯wiczenia

<br/>

<details><summary>Stw贸rz plik `/data/f4.txt` zawieraj膮cy napis `f4`</summary>
<p>

```sh
echo "f4.txt" | hadoop fs -appendToFile - /data/f4.txt
```

</p>
</details>

<br/>

<details><summary>Stw贸rz plik zbiorczy zawieraj膮cy zawarto艣膰 plik贸w `f1.txt`, `f2.txt`, `f3.txt` i `f4.txt`</summary>
<p>

```sh
hadoop fs -getmerge hdfs:///data/f*.txt ./output.txt
hadoop fs -moveFromLocal /data/output.txt /data/output.txt

lub

hadoop fs -cat /data/f*.txt | hadoop fs -appendToFile - /data/output.txt
```

</p>
</details>

<br/>

<details><summary>Za pomoc膮 hadoop'a stw贸rz lokalny folder</summary>
<p>

```sh
hadoop fs -mkdir file:///data/folder
```

</p>
</details>

<br/>

<details><summary>Wy艣wietl rozmiar plik贸w w folderze `/data`</summary>
<p>

```sh
hadoop fs -du -h -v /data/

lub

hadoop fs -df -h /data
```

</p>
</details>

<br/>

<details><summary>Sprawd藕 czy pliki w folerze `/data` s膮 zdrowe</summary>
<p>

```sh
hdfs fsck /data
```

</p>
</details>

<br/>

<details><summary>Zmie艅 "replication level" na `2` dla pliku `/data/f1.txt`</summary>
<p>

```sh
hadoop fs -setrep -w 2 /data/f1.txt
```

</p>
</details>

<br/>

<details><summary>Wy艣wietl statystyki pliku `/data/f1.txt` korzystaj膮c z podanego formatu: `"type:%F perm:%a %u:%g size:%b mtime:%y atime:%x name:%n"`</summary>
<p>

```sh
hadoop fs -stat "type:%F perm:%a %u:%g size:%b mtime:%y atime:%x name:%n" /data/f1.txt
```

</p>
</details>

<br/>

<details><summary>Sprawd藕 czy plik `/data/f1.txt` nie jest pusty</summary>
<p>

```sh
hadoop fs -test -e /data/f1.txt; echo $
```

</p>
</details>

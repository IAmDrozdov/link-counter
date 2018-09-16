# links-counter

Application that provide abilities to count links via URL

Author
-
[Alexander Drozdov](mailto:aleksandr.drozdov.99@gmail.com)

Languages
-
Python

Dependencies
-

- Python 3.x
- Java 8
- Docker 1.17.x
- Docker-compose 1.22.x
- Mysql 5.7
- Hadoop 3.1.1
- Apache Spark for Hadoop 2.7 and more

In this application using own MySQL configuration with:

- username = linker
- password = linkerpassword
- root name = root
- root password = password
- Database schema locating in db/init.sql.
 
But usernames and passwords you can change in in workflow_exercise/database_uploading_task.py.

Setup [Hadoop](https://www.digitalocean.com/community/tutorials/how-to-install-hadoop-in-stand-alone-mode-on-ubuntu-16-04) 
and [Apache Spark](https://www.tutorialspoint.com/apache_spark/apache_spark_installation.htm).

Running
-

### Docker run

- clones repository
- perform build
- creates a Docker image
- run Hadoop
- launch the application

```bash
git clone https://github.com/IAmDrozdov/link-counter.git
cd link-counter
docker build -t app .
start-dfs.sh
docker run --network host app https://google.com
```
**!You can enter arbitrary number of URLs.**

### External configuration

#### Running with central scheduler

To run application with centralized scheduler first turn on luigid:
```bash
luigid
```
And then:
```bash
docker run --network host app --scheduler centralized https://google.com
```
Now you can go to <https://localhost:8082> and se visualisation of processing.

#### Using docker-compose

```bash
docker-compose up
docker run --network workflow_exercise_default  app --host compose  https://google.com
```
There is you can also switch between local and centralized schedulers.
 

#### Testing
For testing create database :
- database name = test_links
- username = linker
- password = linkerpassword
- table = links
- schema (test_text varchar(255))

Then go to test directory and:
```python3
python3 -m unittest -v saving_task_test.py 
python3 -m unittest -v saving_task_test.py 
python3 -m unittest -v database_uploading_task_test.py 

```

 
#### Issues
 
When you run ```docker-compose up``` and see ```Error starting userland proxy: listen tcp0.0.0.0:3306: bind: address already in use```.
Run: 
 ```bash
sudo service mysql stop
docker-compose down
docker-compose up
 ```
 
API
-

Available only when docker-compose is up.

### Database adminer

Database UI is available at <https://localhost:8080>

### Luigid(task visualisation)

 Available at <https://localhost:8082>

### Hadoop visualization

 Available at <https://localhost:9870>
 
### Hadoop clusters

Available at <https://localhost:8088>
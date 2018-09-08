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
- Docker 1.17.x
- Docker-compose 1.22.x
- Mysql 5.7

In this application using own MySQL user, but you can change in in workflow_exercise/database_uploading_task.py. Database schema locating in db/init.sql.

Running
-

### Docker run

- clones repository
- perform build
- creates a Docker image
- launch the application

```bash
git clone https://github.com/IAmDrozdov/link-counter.git
docker build -t app .
docker run --local-scheduler app https://google.com
```
!You can enter arbitrary number of URLs.

### External configuration

#### Running with central scheduler

By default centralized scheduler is turned of. To turn in ot go to workflow_exercise/__main__.py and set local_scheduler=True.
And then rebuild it and run: 
```bash
docker build -t app .
luigid
docker run app https://google.com
```
Now you can go to <https://localhost:8082> and se visualisation of processing.
#### Using docker-compose

To use docker-compose with centralized scheduler you will need also uncomment 19 row in  workflow_exercise/__main__.py
and also rebuild:
```bash
docker build -t app .
docker-compose up -d
docker run --network=workflow_exercise_default app https://google.com
```
 There is you can also switch between local and centralized schedulers.
 
 API
 -
 
 Available only when docker-compose is up.
 
 ### Database adminer
 
 Database UI is available at <https://localhost:8080>
 
 ### Luigid(task visualisation)
 
  Available at <https://localhost:8082>

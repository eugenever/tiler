# Деплой приложения

Деплой приложения можно осуществлять посредством `Conda`, `pyenv` или скомпоновав в отдельную директорию все необходимые
зависимости и скопировав ее на целевую машину с последующей настройкой переменных окружения для `Python`, `GDAL`.

На ОС Linux (Debian 12) использовался вариант с `miniconda` для исключения проблем с установкой и сборкой оригинального
GDAL и его враппера для Python (https://docs.anaconda.com/miniconda/miniconda-install/):

```
export PATH="/home/username/miniconda3/bin:$PATH" (временно для доступа к conda через терминал)

conda config --add channels conda-forge
conda create -n p3118_gdal383 python=3.11.8 gdal=3.8.3
```

Дополнительно установить пакет `pkg-config`.
Версия компилятора `Rust` не должна превышать `1.79.0` (с версии `1.80.0` ошибки в крейте `time`)

На ОС Windows сохранить путь расположения папки `C:/Aspect/aspect-lib` или при изменении данного пути переустановить
пакет `granian` (при сборке бинарного файла `granian.exe` в него прописывается абсолютный путь расположения пакета):

```
python -m pip uninstall granian
python -m pip install granian==1.5.2
```

Для обфускации большей части кода приложения на `Python`, а также с целью некоторого ускорения кода используется библиотека
Nuitka. Важно отметить, что из компиляции необходимо исключить код, где применяется библиотека `Numba` с помощью ключа
`nofollow-import`.

```
python -m nuitka --module server --include-package=server

python -m nuitka --module raster_tiles --include-package=raster_tiles --nofollow-import-to=raster_tiles.acceleration.acceleration_with_numba

python -m nuitka --module vector_tiles --include-package=vector_tiles
```

Указанные выше команды запускаются из [корня проекта](https://gitlab.isone.com/aspect/aspect-gis/-/tree/develop/Tiler-Rust?ref_type=heads).

На выходе получаем, например, для ОС Windows бинарные библиотеки `raster_tiles.cp311-win_amd64.pyd`, `server.cp311-win_amd64.pyd`, `vector_tiles.cp311-win_amd64.pyd` и справочные файлы `*.pyi` с описанием интерфейсов.
Собранные таким образом бинарные библиотеки заменяют почти весь `Python`-код модулей `raster_tiles`, `vector_tiles` и `server` кроме:

- модуля `raster_tiles.acceleration.acceleration_with_numba`,
- бинарной библиотеки Robyn - `robyn.cp311-win_amd64.pyd` и её файла интерфейсов `robyn.pyi`.

Указанные исключения должны находится в рамках тех же самых путей импорта как и в случае с оригинальным кодом `Python`.

## Определение переменных окружения

В корневой папке проекта создать файл `.env`, в котором указать следующие переменные окружения:

```
# Unix
GDAL_HOME=$HOME/miniconda3/envs/gdal383
PROJ_LIB=$GDAL_HOME/share/proj
PYTHONPATH=$HOME/.pyenv/versions/3.11.5

# Windows
GDAL_HOME=C:/Projects/lib/GDAL-381
PROJ_LIB=C:/Projects/lib/GDAL-381/bin/proj9/share
PROJ_DEBUG=3
PYTHONPATH=C:/Projects/lib/python3118_gdal381

# Connection to PostgreSQL DataBase of Tiler-Server
DBHOST=127.0.0.1
DBPORT=5432
DBNAME=osm_cfo
DBUSER=postgres
DBPASS=123
DBPOOLSIZE=10

ANYIO_TOTAL_TOKENS=500
CHECK_KEYS_AFTER_DAYS=1
```

## Установка PostgreSQL и расширений

Для установки `PostgreSQL 16.x` на Ubuntu 20.x (в том числе и на ряде других ОС Linux) необходимо подключить дополнительный репозиторий. По умолчанию доступна версия 12.

Для установки расширений `PostGIS 3.4.x`, `GEOS 3.12.x`, `PgRouting 3.6.x` необходимо произвести их сборку из исходников на целевых машинах.
Для ОС Windows указанные выше расширения поставляются в составе `PostGIS 3.4.2`.

Расширение `MobilityDB` поставляется в составе `PostGIS` начиная с версии последнего 3.3.3.

Расширение `TimescaleDB` на `Debian` и `Ubuntu` устанавливается из отдельного репозитория (не требует сборки).
На ОС Windows установка происходит с помощью [инсталлятора](https://docs.timescale.com/self-hosted/latest/install/installation-windows/).
На ОС Windows пакет `timescaledb_toolkit` не доступен.

## Инициализация базы данных геосервера

Инициализация может выполняться в несколько этапов.

1. Запуск сервисной команды `tiler-server.exe init` (`./tiler-server init`).
   В итоге будет создана база данных с именем из переменной окружения `DBNAME` и необходимые таблицы:

- datasource
- queue

2. Загрузка датасорсов может осуществляться после выполнения `п.1`. Источниками могут служить:

- директории [datasources/raster](https://gitlab.isone.com/aspect/aspect-gis/-/tree/develop/Tiler-Rust/datasources/raster?ref_type=heads) и [datasources/vector](https://gitlab.isone.com/aspect/aspect-gis/-/tree/develop/Tiler-Rust/datasources/vector?ref_type=heads).
  В указанные папки по мере функционирования приложения можно добавлять датасорсы. Для выполнения загрузки из них
  необходимо выполнить POST запрос следующего плана:

```
curl -X
    POST
    http://localhost:8000/api/datasources/load_files
    -H 'accept: application/json'
    -H 'Content-Type: application/json'
```

Тело запроса при этом пустое. В качестве ответа сервер вернет объект следующей структуры:

```
{
    "load_vector_datasources": <число валидных загруженных векторных датасорсов>,
    "load_raster_datasources": <число валидных загруженных растровых датасорсов>,
    "errors": <список ошибок при наличии>,
}
```

- POST запросы по адресу `/api/datasources` следующего вида:

```
curl -X 'POST' \
  'http://localhost:8000/api/datasources' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  	  "id": "143a2012-ece8-4264-a13e-c742d380204f", // id можно не указывать, сгенерируется автоматически
	  "type": "raster",
	  "mosaics": false,
	  "dataStore": {
	  	"type": "raster",
	  	"store": "internal",
	  	"file": "Telecom_Serbja.TIF"
	  },
	  "pyramidSettings": {
	  	"minzoom": 1,
	  	"maxzoom": 9,
	  	"count_processes": 2,
	  	"resampling": "nearest",
	  	"pixel_selection_method": "LowestMethod"
	  },
	  "minzoom": 0,
	  "maxzoom": 11,
	  "center": [20.45676, 44.798176, 10],
	  "encoding": "f32"
}'

curl -X 'POST' \
  'http://localhost:8000/api/datasources' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
	  "type": "vector",
	  "dataStore": {
	  	"type": "vector",
	  	"store": "tiles",
	  	"tiles": ["https://api.maptiler.com/tiles/v3/{z}/{x}/{y}.pbf?key={k}"],
	    "keys": [
	      "t2mP0OQnprAXkW20R6Wd",
	      "fU3vlMsMn4Jb6dnEIFsx",
	      "qnePkfbGpMsLCi3KFBs3",
	      "Modv7lN1eXX1gmlqW0wY"
	    ]
	  },
	  "minzoom": 1,
	  "maxzoom": 13,
	  "center": [21.5, 45.1, 11],
	  "bounds": {
	  	"lng_w": 19.045474,
	    "lat_s": 41.876317,
	    "lng_e": 180,
	    "lat_n": 77.725596
	  }
}'

curl -X 'POST' \
  'http://localhost:8000/api/datasources' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
	  "type": "vector",
	  "dataStore": {
	  	"type": "vector",
	  	"store": "internal"
	  },
	  "buffer": 64,
	  "extent": 4096,
	  "minzoom": 1,
	  "maxzoom": 10,
	  "center": [21.5, 45.1, 11],
	  "bounds": {
	  	"lng_w": 19.045474,
	    "lat_s": 41.876317,
	    "lng_e": 180,
	    "lat_n": 77.725596
	  },
	  "layers": [
    {
      "id": "building",
      "type": "polygon",
      "storeLayer": "cfo_polygon",
      "geomField": "way",
      "description": "Layer of building",
      "minzoom": 13,
      "maxzoom": 19,
      "simplify": false,
      "filter": [
        "all",
        ["==", ["geometry-type"], "Polygon"],
        [
          "any",
          ["all", ["!=", ["get", "building"], "no"], ["has", "building"]],
          ["in", ["get", "aeroway"], "terminal"]
        ]
      ],
      "fields": [
        {
          "name": "id",
          "name_in_db": "id",
          "encode": true,
          "description": "ID of building geometry in database"
        },
        {
          "name": "osm_id",
          "name_in_db": "osm_id",
          "encode": true,
          "description": "osm_id of building"
        },
        {
          "name": "building",
          "name_in_db": "building",
          "encode": false,
          "description": "Name of building"
        },
        {
          "name": "aeroway",
          "name_in_db": "aeroway",
          "description": "Type building is aeroway"
        }
      ]
    }
  ]
}'
```

При успешно пройденой валидации в качестве ответа вернется объект:

```
{
    "datasource_id": <id сохраненного в БД датасорса>,
    "message": "DataSource successfully created",
}
```

В случае невалидной структуры датасорса будет получен список ошибок валидации:

```
[
    {
        "location": <где произошла ошибка>,
        "message": <текст ошибки>,
        "type": <тип ошибки>,
    },
    ....
]
```

## Запуск экземпляра геосервера

Из корневой директории приложения выполнить одну из команд:

```
./tiler-server serve (запуск воркера на Unix)
./tiler-server serve --address=isone.com:8989 (запуск мастера на Unix)
./tiler-server serve-cache (запуск сервера без окружения Python и без подключения к БД PostgreSQL на Unix)

tiler-server.exe serve (запуск воркера на Windows)
tiler-server.exe serve --address=isone.com:8989 (запуск мастера на Windows)
tiler-server.exe serve-cache (запуск сервера без окружения Python и без подключения к БД PostgreSQL на Windows)
```

В качестве параметра `address` указать сетевой (внешний) адрес машины, на которой запускается экзмепляр геосервера.
Этот параметр необходим мастер-серверу чтобы он знал под каким внешним сетевым адресом он запущен.

После выполнения указанных команд запускаются:

- процесс Балансировщика, реализованный на Rust. Указанный процесс выполняет внешнее централизованное управление над
  дочерними процессами воркеров на Python;
- процессы воркеров на Python, которые выполняют обработку растровых и векторных данных, а также прочие вычисления.

# HTTP сервер приложения на базе Rust

Реализация с применением языка `Rust` носит вынужденный харакрет. В экосистеме `Python` существуют проблемы со стандартными
модулями `multiprocessing` и `asyncio`. Первая проблема касается запуска процессов на `Unix` системах (периодически возникают
блокировки, зависания процессов без явных ошибок). Вторая проблема связана с ошибками в `asyncio` на `Windows` системах, где
происходит преждевременное закрытие сокетов и сервер не обрабатывает запросы (ошибка корнями уходит в интерпретатор).

Серверная часть состоит из двух составляющих:

- прокси-сервер (+ режим мастер), балансировщик запросов, модуль отдачи готовых тайлов, модуль мультипроцессинга:
  [proxy balancer](https://gitlab.isone.com/aspect/aspect-gis/-/tree/develop/Tiler-Rust/server/rust/proxy-balancer?ref_type=heads)
- минималистичный http-сервер `Robyn` для web-приложений на `Python` (фреймворк `Actix`) или сервер `Granian` с полноценной
  поддержкой `ASGI` и `FastAPI` (на основе `hyper`). Используют библиотеку `PyO3` для взаимодествия с `API Python`.
  Код сервера `robyn`:
  [robyn rust worker](https://gitlab.isone.com/aspect/aspect-gis/-/tree/develop/Tiler-Rust/server/rust/robyn-worker?ref_type=heads),
  [robyn python](https://gitlab.isone.com/aspect/aspect-gis/-/tree/develop/Tiler-Rust/server/robyn?ref_type=heads)

  ASGI сервер [Granian](https://github.com/emmett-framework/granian) устанавливается штатно через `pip`.

  `ВАЖНО`: по результатам тестирования и отзывам на `Github` в качестве `http стека` на стороне `Python` выбрана связка
  `Granian + FastAPI`. Указанный выбор позволяет использовать все преимущества фреймворка `FastAPI` (качественная документация,
  стабильность, богатый функционал, простота и большое сообщество) и базовых библиотек `hyper` и `tokio` у сервера `Granian`.

## Прокси-сервер, балансировщик запросов

Код размещается по следующей ссылке - [proxy balancer](https://gitlab.isone.com/aspect/aspect-gis/-/tree/develop/Tiler-Rust/server/rust/proxy-balancer?ref_type=heads)

Указанный сервис обеспечивает приём запросов от клиентов (например, картографическая библиотека `MapLibre GL JS`), отдачу
существующих тайлов (исключая запросы в воркеры `Robyn/Granian - Python`, которые генерируют отсутствующие тайлы), равномерное
распределение запросов за отсутствующими тайлами между воркерами `Robyn/Granian - Python` и запуск/останов процессов воркеров.
Сервис забирает на себя большой объём мелких IO-операций, с которыми плохо справляются асинхронные библиотеки из экосистемы
`Python`.
Также данный функционал обеспечивает единое информационное пространство для всех поступающих запросов, что не позволяет
реализовать связка `FastAPI + uvicorn`.

Файл конфигурации HTTP сервиса - [config_app.json](https://gitlab.isone.com/aspect/aspect-gis/-/blob/develop/Tiler-Rust/config_app.json?ref_type=heads)

Пояснения к отдельным параметрам конфигурации:

- `type` тип базового http-сервера. Допустимые значения `robyn` или `granian`. Сервер `robyn` не поддерживает запуск сторонних
  web-приложений (используется свой Питоновский API над http-сервером Rust). Сервер `granian` поддерживает `ASGI` и позволяет
  запускать сторонние web-приложения, в том числе, на базе `FastAPI`.
- `timeout_worker_response` таймаут в секундах в течение которого ожидается ответ от сервера-воркера. По истечении
  указанного таймаута вернется ошибка с кодом `503`.
- `timeout_pull_job` периодичность проверки отложенных работ из базы данных PostgreSQL, у которых наступил момент исполнения.
- `log_level` в продакшене устанавливать в значение `ERROR` чтобы избежать большого потока сообщений в лог
- `thread_workers` это число потоков внутри отдельного воркера (не всего приложения). Устанавливать в пределах
  (2-2,5)\*число ядер процессора для воркеров `robyn` и 1 для воркеров `granian`.
  Потоки в `robyn` воркерах используются как замена горутинам для IO-операций с растрами.
- `processes_workers` количество воркеров `Robyn/Granian - Python`.
- `blocking_threads` количество дополнительных потоков под блокирующие tasks tokio (spawn_blocking).
- `interface` в нашем случае всегда принимает значение `asgi`.
- `reload_time` время суток для выполнения перезагрузки воркеров Питона, формат "02:45:30" (часы:минуты:секунды).
- `reload_periodicity_days` периодичность перезагрузки воркеров Питона в днях. По умолчанию значение 1.
- `reload_repeat_minutes` периодичность повторной попытки перезагрузить воркеры Питона в минутах. В случае если воркеры
  выполняют вычисления их нельзя перезагружать.
- `reload_repeat_attempts` количество попыток перезагрузить воркеры Питона.
- `max_concurrent_tile_requests` максимальное число тайлов, которые параллельно (конкурентно) обрабатываются воркером Питона.
  Данный параметр необходимо согласовывать со значением `processes_workers`, а именно, их произведение деленное на число ядер
  процессора даст число тайлов обрабатываемых одним ядром в каждый момент времени. Последний параметр зависит от характеристик
  аппаратной части сервера и не должен иметь высоких значений (иначе возможно чрезмерное потребление ресурсов CPU и зависание
  сервера). Например, для виртуальной машины с ОС Windows Server 2022 и 8-мью ядрами, на которой ведется разработка и тестирование,
  оптимальным является значение 5-7 тайлов на одно ядро.
  Важно: `max_concurrent_tile_requests` призван не допускать вредительства в части, например, DDoS атак или безграничного
  числа запросов на генерацию тайлов ("тяжелых" запросов, которые направляются в воркеры Питона). Этот параметр позволяет
  воркерам Питона быть всегда доступными. При этом обычная нагрузка от пользователей геосервера не будет сталкиваться с
  задержками по скорости генерации тайлов.

Логирование в приложении разделено на 2 части:

- `http` составляющая выводится в консоль (терминал).
- операции непосредственно приложения (воркеров) сохраняются в файлы логов в папке `{Tiler_App}/logs`. Параметры логирования
  настраиваются в файле [log_app.ini](https://gitlab.isone.com/aspect/aspect-gis/-/blob/develop/Tiler-Rust/log_app.ini?ref_type=heads).
  По умолчанию лог разбивается по дням, время хранения одного файла лога - 30 дней.

Сборка сервиса осуществляется командой:

```
cargo build --release
```

Собранный бинарный файл помещается в [корневую папку приложения](https://gitlab.isone.com/aspect/aspect-gis/-/tree/develop/Tiler-Rust?ref_type=heads)

Запуск всего приложения выполняется следующими командами:

- `./tiler-server serve` запуск экземпляра сервера в качестве `воркера` на отдельной машине на Unix
- `tiler-server.exe serve` запуск экземпляра сервера в качестве `воркера` на отдельной машине на Windows
- `./tiler-server serve --address isone.com:8989` запуск экземпляра сервера в качестве `мастера` на отдельной машине на Unix
- `tiler-server.exe serve --address isone.com:8989` запуск экземпляра сервера в качестве `мастера` на отдельной машине на Windows
- `./tiler-server serve-cache` запуск экземпляра сервера без воркеров `Python` и без подключения к БД `PostgreSQL` для раздачи
  готовых тайлов в формате `MBTiles` на Unix
- `tiler-server.exe serve-cache` запуск экземпляра сервера без воркеров `Python` и без подключения к БД `PostgreSQL` для раздачи
  готовых тайлов в формате `MBTiles` на Windows

`ВАЖНО`: в рамках одной сети геосерверов можно запускать несколько экземпляров серверов в режиме `МАСТЕР`. Они выполняют
диспетчеризацию запросов тайлов (готовых и требующих генерации), на построении пирамид тайлов и управлению очередью
отложенных работ (например, пирамиды тайлов, потенциально тяжелые расчеты и т.д.). Сетевой адрес (параметр `address`) `МАСТЕР`
сервера выступает входной точкой для всех внешних запросов. В параметрах сущностей можно указывать сетевые адреса как
серверов `ВОРКЕРОВ` так и `МАСТЕР`, которые являются конечными исполнителями работ.

Для запуска сервера на локальной изолированной машине (воркеры `Python` + `PostgreSQL`) необходимо использовать команды:

```
./tiler-server serve (Unix)
tiler-server.exe serve (Windows)
```

При запуске подгружаются файл конфигурации, файл с настройками логирования и в итоге запускаются воркеры `Robyn/Granian - Python`.

## HTTP воркеры (процессы) Robyn/Granian-Python

Код воркеров `Robyn` состоит из двух частей на `Rust` и `Python` соответственно:

- [robyn rust worker](https://gitlab.isone.com/aspect/aspect-gis/-/tree/develop/Tiler-Rust/server/rust/robyn-worker?ref_type=heads)
- [robyn python](https://gitlab.isone.com/aspect/aspect-gis/-/tree/develop/Tiler-Rust/server/robyn?ref_type=heads)

Для сборки кода `Rust` в бинарную библиотеку (в формате `pyd` для Windows и `so` для Linux) используется модуль `maturin` из
экосисистемы `Python`. Команда для сборки:

```
maturin build --release
```

В репозитории уже представлены собранные библиотеки для `Windows` и `Linux`:

- Windows - [robyn.cp311-win_amd64.pyd](https://gitlab.isone.com/aspect/aspect-gis/-/blob/develop/Tiler-Rust/server/robyn/robyn.cp311-win_amd64.pyd?ref_type=heads)
- Linux - [robyn.cpython-311-x86_64-linux-gnu.so](https://gitlab.isone.com/aspect/aspect-gis/-/blob/develop/Tiler-Rust/server/robyn/robyn.cpython-311-x86_64-linux-gnu.so?ref_type=heads)

Их `штатно не требуется` собирать во время деплоя, они идут в собранном виде из репозитория.

Endpoint`s API представлены в файлах:

- [server/fapi/tile.py](https://gitlab.isone.com/aspect/aspect-gis/-/blob/develop/Tiler-Rust/server/fapi/tile.py?ref_type=heads)
- [server/fapi/vector/tile.py](https://gitlab.isone.com/aspect/aspect-gis/-/blob/develop/Tiler-Rust/server/fapi/vector/tile.py?ref_type=heads)
  [server/fapi/vector/pyramid.py](https://gitlab.isone.com/aspect/aspect-gis/-/blob/develop/Tiler-Rust/server/fapi/vector/pyramid.py?ref_type=heads)
- [server/fapi/raster/tile.py](https://gitlab.isone.com/aspect/aspect-gis/-/blob/develop/Tiler-Rust/server/fapi/raster/tile.py?ref_type=heads)
- [server/fapi/raster/pyramid.py](https://gitlab.isone.com/aspect/aspect-gis/-/blob/develop/Tiler-Rust/server/fapi/raster/pyramid.py?ref_type=heads)

Интерактивная документация по схеме `OpenAPI` представлена с помощью двух систем (сервер запущен на порте 8000):

- http://127.0.0.1:8000/docs (`Swagger`)
- http://127.0.0.1:8000/redoc (`ReDoc`)

В менеджерах задач воркеры `Robyn - Python` отображаются как процессы `Python`. Воркеры `Granian - Python` на Unix системах
отображаются как процессы `granian`, на Windows как процессы `Python`.

## API

`datasource_id` - идентификатор датасорса в формате `UUID4`

`/api/datasources/{datasource_id}` - `GET` запрос, получение конкретного датасорса из БД в формате `JSON`.
Вариант ответа:

```
{
    "id": "143a2012-ece8-4264-a13e-c742d380204f",
    "type": "raster",
    "mosaics": false,
    "dataStore": {
        "type": "raster",
        "store": "internal",
        "file": "Telecom_Serbja.TIF"
    },
    "pyramidSettings": {
        "minzoom": 1,
        "maxzoom": 10,
        "count_processes": 2,
        "resampling": "nearest",
        "pixel_selection_method": "LowestMethod"
    },
    "minzoom": 0,
    "maxzoom": 11,
    "center": [20.45676, 44.798176, 10],
    "encoding": "f32"
}
```

`/api/datasources` - `GET` запрос, получение списка всех датасорсов из БД в формате `JSON`.
В качестве ответа приходит массив из объектов датасорсов:

```
[
    {
        <поля датасорса 1>
    },
    {
        <поля датасорса 2>
    },
    ...
]
```

`/api/datasources` - `POST` запрос, создание нового датасорса.
Вариант запроса:

```
curl -X 'POST' \
  'http://localhost:8000/api/datasources' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  	  "id": "143a2012-ece8-4264-a13e-c742d380204f", // можно не указывать
	  "type": "raster",
	  "mosaics": false,
	  "dataStore": {
	  	"type": "raster",
	  	"store": "internal",
	  	"file": "Telecom_Serbja.TIF"
	  },
	  "pyramidSettings": {
	  	"minzoom": 1,
	  	"maxzoom": 10,
	  	"count_processes": 2,
	  	"resampling": "nearest",
	  	"pixel_selection_method": "LowestMethod"
	  },
	  "minzoom": 0,
	  "maxzoom": 11,
	  "center": [20.45676, 44.798176, 10],
	  "encoding": "f32"
}'
```

В качестве ответа (код `422`) в случае ошибок валидаций приходит список из ошибок, например:

```
{
  "detail": [
    {
      "location": ["body", "raster", "dataStore", "store"],
      "message": "Value error, DataStoreRasterBase.store must have one of the values {'tiles', 'tilejson', 'mbtiles', 'internal'}",
      "type": "value_error"
    },
    {
      "location": [
        "body",
        "raster",
        "pyramidSettings",
        "pixel_selection_method"
      ],
      "message": "Value error, PyramidSettings.pixel_selection_method must have one of the values ['FirstMethod', 'HighestMethod', 'LowestMethod', 'MeanMethod']",
      "type": "value_error"
    }
  ]
}
```

При успешно пройденной валидации приходит следующий ответ (код `200`):

```
{
    "datasource_id": "143a2012-ece8-4264-a13e-c742d380204f",
    "message": "DataSource successfully created",
}
```

`/api/datasources` - `PUT` запрос, обновление существующего датасорса.
Формат ответов (коды `422` и `200`) совпадает со случаем создания нового датасорса как указано выше.

`/api/datasources` - `DELETE` запрос, удаление датасорса. В случае успеха приходит ответ с кодом `200`:

```
{
    "status": 200,
    "message": "DataSource '143a2012-ece8-4264-a13e-c742d380204f' successfully remove"
}
```

`/api/datasources/load_files` - `POST` запрос с пустым телом, выполнить загрузку датасорсов из файлов в формате JSON,
которые находятся в директориях `/datasources/raster` и `/datasources/vector`.
По результатам загрузки возвращается ответ с кодом `200`:

```
{
    "load_vector_datasources": <число загруженных векторных датасорсов>,
    "load_raster_datasources": <число загруженных растровых датасорсов>,
    "errors": <пустой массив или список ошибок валидации>,
}
```

`/api/datasources/reload_files` - `POST` запрос, выполнить повторную загрузку указанных в теле запроса датасорсов из файлов
в формате JSON, которые находятся в директориях `/datasources/raster` и `/datasources/vector`.
Пример запроса (тело запроса - массив строк представляющих идентификаторы перезагружаемых датасорсов):

```
curl -X
    POST
    http://isone.com:8989/api/datasources/reload_files
    -H "accept: application/json"
    -H "Content-Type: application/json"
    -d "[
        \"e43s6uj6-8a11-09rt-b4af-1ddaf41510a9\",
        \"eee20116-1024-41e9-9c78-e718b47668b7\",
        \"e79955e9-70d7-40ff-b3f8-5fb944078059\",
        \"b630e89c-d946-4e89-9fb7-9ce590ea6faf\"
    ]"
```

По результатам загрузки возвращается ответ с кодом `200`:

```
{
    "load_vector_datasources": <число загруженных векторных датасорсов>,
    "load_raster_datasources": <число загруженных растровых датасорсов>,
    "errors": <пустой массив или список ошибок валидации>,
}
```

`/api/pyramid` - `POST` запрос, генерация пирамиды тайлов.
При отсутствии запрошенного датасорса вернется ошибка с кодом `404`:

```
{
    "message":"DataSource with id 'dafc76d4-8bc7w-455a-a2d4-4e2d1cb13b35' does not exists",
    "status":404
}
```

В случае корректности всех данных датасорса и его `id` вернется ответ с кодом `202`:

```
{
    "pyramid_id": "1b728239-7555-499f-b09b-fa2094f3749d", // id запущенной пирамиды в формате UUID4
    "already_running": False // указывает, что пирамида для данного датасорса не запущена на данный момент
}
```

Параметр `already_running` будет иметь значение `true` в ответе когда пользователь попытается повторно запустить
пирамиду для конкретного датасорса при условии, что имеется незавершенная пирамида. Т.е. в один момент времени
для каждого датасорса может выполняться только одна пирамида. Но может быть запущено несколько пирамид для разных
датасорсов.

`/api/tile/{datasource_id}/{z}/{x}/{y}.{ext}` - `GET` запрос, генерация отдельного тайла указанного датасорса
Примеры запросов:

```
http://localhost:8000/api/tile/aa274ed8-f592-4a74-bfed-ef56cbdbcd10/12/2473/1279.png
http://localhost:8000/api/tile/e000dfde-5c30-4783-b8f5-d3ae3138ad39/10/618/320.mvt
http://localhost:8000/api/tile/e000dfde-5c30-4783-b8f5-d3ae3138ad39/10/618/320.pbf
```

При успешной обработке запросов возвращаются байты представляющие файл тайла с кодом `200`.
При наличии тайла в тайловой сетке, но отсутствии данных для него возвращается пустой ответ с кодом `204`, который
корректно обрабатывается библиотекой MapLibre GL JS.

# Сервисный API

Обеспечивает управление воркерами Питона и содержит справочную информацию о процессах приложения.

- `/maintenance/add_workers` - `POST` запрос, добавление воркеров Питона
- `/maintenance/reload_workers` - `POST`, `GET` запрос, принудительная перезагрузка всех воркеров Питона в текущий момент
- `/maintenance/terminate_workers` - `POST`, `GET` запрос, принудительная оставновка всех воркеров Питона в текущий момент
- `/maintenance/info_workers` - `GET` запрос, справочная информация о процессах воркеров Питона

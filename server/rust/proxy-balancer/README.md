# Прокси-балансировщик

Сервис сочетает в себе одновременно 3 функции:
- прокси-сервер
- балансировщик запросов, отправляемых в воркеры Питона
- управление воркерами Питона (запуск, остановка по мере необходимости в зависимости от нагрузок)

В качестве основной библиотеки при работе с `http` используется `hyper`.
Воркеры Питона реализованы на базе `FastAPI`.
Взаимодействие между сервисом и воркерами Питона происходит по `http`.

## Краткие комментарии к коду

[Таски Токио](https://github.com/eugenever/tiler/tree/main/server/rust/proxy-balancer/src/tasks), работающие в фоне:
- [управление воркерами Питона](https://github.com/eugenever/tiler/blob/main/server/rust/proxy-balancer/src/tasks/workers.rs)
- [управление коннектами к SQLite (MBTiles)](https://github.com/eugenever/tiler/blob/main/server/rust/proxy-balancer/src/tasks/sqlite_clients.rs)
- [rate limiter ограничивающий число одновременных запросов в воркеры Питона](https://github.com/eugenever/tiler/blob/main/server/rust/proxy-balancer/src/tasks/semaphore.rs)
- [рестарт воркеров Питона](https://github.com/eugenever/tiler/blob/main/server/rust/proxy-balancer/src/tasks/reload_workers.rs)
- [планировщик произвольных задач/работ на стороне Питона](https://github.com/eugenever/tiler/blob/main/server/rust/proxy-balancer/src/tasks/job.rs)


Обработчики запросов:
- [хендлер верхнего уровня](https://github.com/eugenever/tiler/blob/main/server/rust/proxy-balancer/src/handles/mod.rs)
- [список эндпоинтов](https://github.com/eugenever/tiler/tree/main/server/rust/proxy-balancer/src/handles/endpoints)
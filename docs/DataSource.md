# Описание структуры датасорсов

Датасорсы делятся на два больших класса: растровые и векторные.
Примеры текущих версий датасорсов можно посмотреть в директории [datasources](https://gitlab.isone.com/aspect/aspect-gis/-/tree/develop/Tiler-Rust/datasources?ref_type=heads).

## Растровые датасорсы

Пример описания датасорса для одиночного растра:

```
{
  "id": "b79955e9-70d7-40ff-b3f8-5fb944078059", // можно не указывать, сгенерируется автоматически
  "type": "raster",
  "mosaics": false, // false - одиночный растр, true - мозаика
  "dataStore": {
    "type": "raster",
    "store": "internal", // указывает на тайлы собственной генерации (не внешний источник)
    "dataset": "A1_Srbija_4G_bounded_in_dBm_2021Q1", // совпадает с именем файла растра без расширения или папки мозаики
    "file": "A1_Srbija_4G_bounded_in_dBm_2021Q1.TIF", // имя файла исходного растра из папки /data
    "host": "isone.com",
    "port": 8989
  },
  "pyramidSettings": {
    "resampling": "nearest",
    "save_tile_detail_db": true,
    "count_processes": 4,
    "remove_processing_raster_files": false,
    "minzoom": 0,
    "maxzoom": 11
  },
  "attribution": "Empty",
  "description": "Example A1_Srbija_4G_bounded_in_dBm_2021Q1 Raster DataSource",
  "version": "0.0.1",
  "minzoom": 0,
  "maxzoom": 15,
  "mbtiles": true,
  "bounds": {
    "lng_w": 19.5,
    "lat_s": 42.7,
    "lng_e": 21.7,
    "lat_n": 46.8
  },
  "center": [20.45676, 44.798176, 10],
  "encoding": "f32"
}
```

Параметры `host` и `port` представляют собой адрес удаленного сервера-воркера, на котором запущен экземпляр геосервера.
Данные параметры имеют смысл для растровых датасорсов в рамках распределения нагрузки (на процессоры) и места под
исходные растры. Поскольку для векторных данных используется центролизованное хранилище в виде БД PostgreSQL, то
указанные параметры не имеют смысла для векторных тайлов.

## Векторные датасорсы

Пример описания векторного датасорса с полями `filter`, `fields`, `geomField` (без `SQL`):

```
{
  "id": "aa274ed8-f592-4a74-bfed-ef56cbdbcd10", // можно не указывать, сгенерируется автоматически
  "type": "vector",
  "dataStore": {
    "type": "vector",
    "store": "internal" // указывает на тайлы собственной генерации (не внешний источник)
  },
  "pyramidSettings": {
    "minzoom": 6,
    "maxzoom": 13,
    "count_processes": 2
  },
  "attribution": "Empty",
  "description": "Example PostgreSQL/PostGIS Vector DataSource",
  "version": "0.0.1",
  "buffer": 64,
  "extent": 4096,
  "minzoom": 0,
  "maxzoom": 19,
  "mbtiles": true,
  "bounds": {
    "lng_w": 26.72,
    "lat_s": 48.91,
    "lng_e": 48.05,
    "lat_n": 60.68
  },
  "center": [37.617627, 55.755829, 11],
  "compress_tiles": true,
  "use_cache_only": false,
  "layers": [
    {
      "id": "building",
      "type": "polygon", // тип геометрии
      "storeLayer": "cfo_polygon", // имя таблицы в базе данных
      "geomField": "way", // имя поля геометрии в таблице базы данных
      "description": "Layer of building",
      "minzoom": 13, // минимальный зум отображения слоя
      "maxzoom": 19, // максимальный зум отображения слоя
      "simplify": false, // прогрессивное упрощение слоя в зависимости от зума
      "filter": [ // фильтр объектов геометрий в JFE нотации
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
          "name": "id", // имя поля в описании фильтра (внутри поля filter)
          "name_in_db": "id", // имя поля в таблице базы данных
          "encode": true, // кодирование поля в качестве атрибута feature
          "description": "ID of building geometry in database"
        },
        {
          "name": "osm_id",
          "name_in_db": "osm_id", // имя поля в таблице базы данных
          "encode": true,
          "description": "osm_id of building"
        },
        {
          "name": "building",
          "name_in_db": "building",
          "encode": false, // поле НЕ кодируется в качестве атрибута feature
          "description": "Name of building"
        },
        {
          "name": "aeroway",
          "name_in_db": "aeroway",
          "description": "aeroway"
        }
      ]
    },
    {
      "id": "water",
      "type": "polygon",
      "storeLayer": "cfo_polygon",
      "geomField": "way",
      "description": "Layer of water",
      "minzoom": 9,
      "maxzoom": 19,
      "simplify": true,
      "filter": [
        "all",
        ["==", "$type", "Polygon"],
        [
          "any",
          ["in", "natural", "lake", "water"],
          ["in", ["get", "waterway"], "canal", "mill_pond", "riverbank"],
          ["in", ["get", "landuse"], "basin", "reservoir", "water"]
        ]
      ],
      "fields": [
        {
          "name": "id",
          "encode": true,
          "description": "ID of water geometry in database"
        },
        {
          "name": "osm_id",
          "name_in_db": "osm_id",
          "encode": true,
          "description": "osm_id of water"
        },
        {
          "name": "natural",
          "description": "description natural"
        },
        {
          "name": "waterway",
          "description": "description waterway"
        },
        {
          "name": "landuse",
          "description": "description landuse"
        }
      ]
    },
    {
      "id": "park",
      "type": "polygon",
      "storeLayer": "cfo_polygon",
      "geomField": "way",
      "description": "Layer of park",
      "minzoom": 11,
      "maxzoom": 19,
      "simplify": true,
      "filter": [
        "all",
        ["==", "$type", "Polygon"],
        [
          "any",
          [
            "in",
            "leisure",
            "dog_park",
            "golf_course",
            "pitch",
            "park",
            "playground",
            "garden",
            "common"
          ],
          [
            "in",
            ["get", "landuse"],
            "allotments",
            "cemetery",
            "recreation_ground",
            "village_green"
          ]
        ]
      ],
      "fields": [
        {
          "name": "id",
          "name_in_db": "id",
          "encode": true,
          "description": "ID of park geometry in database"
        },
        {
          "name": "osm_id",
          "name_in_db": "osm_id",
          "encode": true,
          "description": "osm_id of park"
        },
        {
          "name": "leisure",
          "name_in_db": "leisure",
          "description": "description leisure"
        },
        {
          "name": "natural",
          "description": "description natural"
        },
        {
          "name": "landuse",
          "description": "description landuse"
        }
      ]
    }
  ]
}
```

Поле `compress_tiles` при необходимости обеспечивает сжатие тайлов в формате `GZ` перед их сохранением в `MBTiles`.

Поле `use_cache_only` позволяет раздавать только ранее сгенерированные тайлы в рамках пирамиды. Динамический тайлинг в данном
случае отключен.

Поле `fields` может быть указано без определения поля `filter`. Поле `fields` в данном случае содержит поля необходимые
для кодирования в качестве атрибутов features. Свойство `name_in_db` описывает имя поля в базе данных. Если оно
не указывается, то ему присваивается значение из поля `name`. Имена таблиц и поля из фильтра валидируются
по информационной схеме базы данных PostgreSQL.

В случае если указывается поле `filter`, то поле `fields` должно содержать все поля перечисленные в поле `filter`
за исключением поля геометрии, которое указывается в свойстве `geomField` и автоматически добавляется в раздел `SELECT` подзапроса формирующего базовую выборку для конкретного слоя.

Настройки пирамиды тайлов для векторных данных поддерживают только 3 параметра:

- `minzoom` минимальный зум пирамиды
- `maxzoom` максимальный зум пирамиды
- `count_processes` число воркеров PostgreSQL для генерации пирамиды

Для определения границ `bbox`, на основне которых будет генерироваться набор тайлов, необходимо указать поле `bounds`.
Варьируя границами можно создавать различные датасорсы (датасеты) для одного источника данных, что позволяет строить
пирамиды тайлов для интересующих географических областей с требуемым набором зумов.

Для описания поля `filter` используется нотация [JSON Filter Expressions](https://github.com/tschaub/ogcapi-features/tree/json-array-expression/extensions/cql/jfe), которую поддерживают клиентские библиотеки `MapLibre` и `MapBox`. Указанная
спецификация может содержать устаревший и обновленный синтаксисы, который периодически добавляется из спецификации
`OGC Common Query Language (CQL)`. Парсер, преобразующий `JFE` синтаксис поля `filter` поддерживает нотации актуальные
на момент июль 2024г.

На основе комбинации полей `filter`, `fields`, `geomField`, `simplify`, `minzoom` и `maxzoom` с помощью парсера `JFE` генерируется SQL подзапрос для конкретного векторного слоя данных. Данный подзапрос выбирает геометрии и поля, которые в последующем будут выборочно закодированы в виде слоя в Mapbox векторный тайл (MVT) по его границе. Подготовка подзапросов выполняется на стадии валидации векторного датасорса. Итоговое слияние подзапросов (слоёв) в готовый Mapbox векторный тайл происходит в момент его генерации в файле [mvt_postgis](https://gitlab.isone.com/aspect/aspect-gis/-/blob/develop/Tiler-Rust/server/fapi/vector/mvt_postgis.py?ref_type=heads).

Пример описания векторного датасорса без полей `filter`, `fields`, `geomField`. В данном случае используется непосредственно `SQL` для определения слоев:

```
{
  "id": "rf274ed-f592-4a74-bfed-ef56cbdbcd10",
  "type": "vector",
  "dataStore": {
    "type": "vector",
    "store": "internal",
    "host": "10.0.3.221",
    "port": 8000
  },
  "pyramidSettings": {
    "count_processes": 10,
    "minzoom": 1,
    "maxzoom": 12
  },
  "attribution": "Empty",
  "description": "Example PostgreSQL/PostGIS Vector DataSource",
  "version": "0.0.1",
  "buffer": 64,
  "extent": 4096,
  "minzoom": 0,
  "maxzoom": 19,
  "mbtiles": true,
  "bounds": {
    "lng_w": 19.2,
    "lat_s": 42.2,
    "lng_e": 180,
    "lat_n": 77.8
  },
  "center": [37.617627, 55.755829, 11],
  "compress_tiles": true,
  "use_cache_only": false,
  "layers": [
    {
      "id": "aerialway",
      "type": "polygon",
      "description": "Layer of aerialway",
      "minzoom": 13,
      "maxzoom": 20,
      "queries": [
        {
          "minzoom": 13,
          "maxzoom": 20,
          "sql": "SELECT tags, geom FROM osm_ways WHERE tags ? 'aerialway'"
        }
      ]
    },
    {
      "id": "aeroway",
      "type": "polygon",
      "description": "Layer of aeroway",
      "minzoom": 13,
      "maxzoom": 20,
      "queries": [
        {
          "minzoom": 13,
          "maxzoom": 20,
          "sql": "SELECT tags, geom FROM osm_ways WHERE tags ? 'aeroway'"
        }
      ]
    },
    {
      "id": "amenity",
      "type": "polygon",
      "description": "Layer of amenity",
      "minzoom": 13,
      "maxzoom": 20,
      "queries": [
        {
          "minzoom": 13,
          "maxzoom": 20,
          "sql": "SELECT jsonb_build_object('amenity', tags->'amenity') AS tags, geom FROM osm_ways WHERE tags ? 'amenity'"
        }
      ]
    },
    {
      "id": "attraction",
      "type": "polygon",
      "minzoom": 13,
      "maxzoom": 20,
      "queries": [
        {
          "minzoom": 13,
          "maxzoom": 20,
          "sql": "SELECT tags, geom FROM osm_ways WHERE tags ? 'attraction'"
        }
      ]
    },
    {
      "id": "boundary",
      "type": "polygon",
      "minzoom": 13,
      "maxzoom": 20,
      "queries": [
        {
          "minzoom": 13,
          "maxzoom": 20,
          "sql": "SELECT jsonb_build_object('admin_level', tags->'admin_level') AS tags, geom FROM osm_ways WHERE tags ? 'boundary'"
        }
      ]
    },
    {
      "id": "building",
      "type": "polygon",
      "minzoom": 14,
      "maxzoom": 20,
      "queries": [
        {
          "minzoom": 14,
          "maxzoom": 20,
          "sql": "\n                SELECT\n                    jsonb_build_object('building', tags->'building', 'building:part', tags->'building:part')\n                        || jsonb_build_object('extrusion:base',\n                            CASE\n                                WHEN tags ? 'min_height'\n                                    THEN convert_to_number(tags ->> 'min_height', 0)\n                                WHEN tags ? 'building:min_height'\n                                    THEN convert_to_number(tags ->> 'building:min_height', 0)\n                                WHEN tags ? 'building:min_level'\n                                    THEN convert_to_number(tags ->> 'building:min_level', 0) * 3\n                                ELSE 0\n                            END)\n                        || jsonb_build_object('extrusion:height', \n                            CASE\n                                WHEN tags ? 'height'\n                                    THEN convert_to_number(tags ->> 'height', 6)\n                                WHEN tags ? 'building:height'\n                                    THEN convert_to_number(tags ->> 'building:height', 6)\n                                WHEN tags ? 'building:levels'\n                                    THEN convert_to_number(tags ->> 'building:levels', 2) * 3\n                                ELSE 6\n                            END) as tags,\n                    geom\n                FROM osm_ways\n                WHERE (tags ? 'building' OR tags ? 'building:part') AND ((NOT tags ? 'layer') OR convert_to_number(tags ->> 'layer', 0) >= 0)"
        },
        {
          "minzoom": 14,
          "maxzoom": 20,
          "sql": "\n                SELECT\n                    jsonb_build_object('building', tags->'building', 'building:part', tags->'building:part')\n                        || jsonb_build_object('extrusion:base',\n                            CASE\n                                WHEN tags ? 'min_height'\n                                    THEN convert_to_number(tags ->> 'min_height', 0)\n                                WHEN tags ? 'building:min_height'\n                                    THEN convert_to_number(tags ->> 'building:min_height', 0)\n                                WHEN tags ? 'building:min_level'\n                                    THEN convert_to_number(tags ->> 'building:min_level', 0) * 3\n                                ELSE 0\n                            END)\n                        || jsonb_build_object('extrusion:height',\n                            CASE\n                                WHEN tags ? 'height'\n                                    THEN convert_to_number(tags ->> 'height', 6)\n                                WHEN tags ? 'building:height'\n                                    THEN convert_to_number(tags ->> 'building:height', 6)\n                                WHEN tags ? 'building:levels'\n                                    THEN convert_to_number(tags ->> 'building:levels', 2) * 3\n                                ELSE 6\n                            END) as tags,\n                    geom\n                FROM osm_relations\n                WHERE (tags ? 'building' OR tags ? 'building:part') AND ((NOT tags ? 'layer') OR convert_to_number(tags ->> 'layer', 0) >= 0)"
        }
      ]
    },
    {
      "id": "highway",
      "type": "polygon",
      "minzoom": 4,
      "maxzoom": 20,
      "queries": [
        {
          "minzoom": 4,
          "maxzoom": 13,
          "sql": "SELECT jsonb_build_object('highway', tags->'highway', 'tunnel', tags->'tunnel', 'area', tags->'area', 'bicycle', tags->'bicycle', 'access', tags->'access', 'construction', tags->'construction', 'railway', tags->'railway', 'name', tags->'name') AS tags, geom FROM osm_highway_z$zoom"
        },
        {
          "minzoom": 14,
          "maxzoom": 20,
          "sql": "SELECT jsonb_build_object('highway', tags->'highway', 'tunnel', tags->'tunnel', 'area', tags->'area', 'bicycle', tags->'bicycle', 'access', tags->'access', 'construction', tags->'construction', 'railway', tags->'railway', 'name', tags->'name') AS tags, geom FROM osm_ways WHERE tags ? 'highway'"
        }
      ]
    },
    {
      "id": "landuse",
      "type": "polygon",
      "minzoom": 1,
      "maxzoom": 20,
      "queries": [
        {
          "minzoom": 1,
          "maxzoom": 12,
          "sql": "SELECT jsonb_build_object('landuse', tags->'landuse') AS tags, geom FROM osm_landuse_z$zoom WHERE tags ? 'landuse'"
        },
        {
          "minzoom": 13,
          "maxzoom": 20,
          "sql": "SELECT jsonb_build_object('landuse', tags->'landuse') AS tags, geom FROM osm_ways WHERE tags ? 'landuse'"
        },
        {
          "minzoom": 13,
          "maxzoom": 20,
          "sql": "SELECT jsonb_build_object('landuse', tags->'landuse') AS tags, geom FROM osm_relations WHERE tags ? 'landuse'"
        }
      ]
    },
    {
      "id": "leisure",
      "type": "polygon",
      "minzoom": 1,
      "maxzoom": 20,
      "queries": [
        {
          "minzoom": 1,
          "maxzoom": 12,
          "sql": "SELECT jsonb_build_object('leisure', tags->'leisure') AS tags, geom FROM osm_leisure_z$zoom WHERE tags ? 'leisure'"
        },
        {
          "minzoom": 13,
          "maxzoom": 20,
          "sql": "SELECT jsonb_build_object('leisure', tags->'leisure') AS tags, geom FROM osm_ways WHERE tags ? 'leisure'"
        },
        {
          "minzoom": 13,
          "maxzoom": 20,
          "sql": "SELECT jsonb_build_object('leisure', tags->'leisure') AS tags, geom FROM osm_relations WHERE tags ? 'leisure'"
        }
      ]
    },
    {
      "id": "man_made",
      "type": "polygon",
      "minzoom": 14,
      "maxzoom": 20,
      "queries": [
        {
          "minzoom": 14,
          "maxzoom": 20,
          "sql": "SELECT jsonb_build_object('man_made', tags->'man_made', 'name', tags->'name') AS tags, geom FROM osm_ways WHERE tags ? 'man_made'"
        }
      ]
    },
    {
      "id": "natural",
      "type": "polygon",
      "minzoom": 1,
      "maxzoom": 20,
      "queries": [
        {
          "minzoom": 1,
          "maxzoom": 7,
          "sql": "SELECT jsonb_build_object('natural', tags->'natural', 'water', tags->'water', 'surface', tags->'surface') AS tags, geom FROM osm_natural_z$zoom WHERE tags ->> 'natural' IN ('wood', 'scrub', 'heath', 'grassland', 'bare_rock', 'scree', 'shingle', 'sand', 'mud', 'water', 'wetland', 'glacier', 'beach')"
        },
        {
          "minzoom": 8,
          "maxzoom": 12,
          "sql": "SELECT jsonb_build_object('natural', tags->'natural', 'water', tags->'water', 'surface', tags->'surface') AS tags, geom FROM osm_natural_z$zoom WHERE tags ->> 'natural' IN ('wood', 'scrub', 'heath', 'grassland', 'bare_rock', 'scree', 'shingle', 'sand', 'mud', 'water', 'wetland', 'glacier', 'beach')"
        },
        {
          "minzoom": 13,
          "maxzoom": 20,
          "sql": "SELECT jsonb_build_object('natural', tags->'natural', 'water', tags->'water', 'surface', tags->'surface') AS tags, geom FROM osm_ways WHERE tags ? 'natural' AND tags ->> 'natural' NOT IN ('coastline')"
        },
        {
          "minzoom": 13,
          "maxzoom": 20,
          "sql": "SELECT jsonb_build_object('natural', tags->'natural', 'water', tags->'water', 'surface', tags->'surface') AS tags, geom FROM osm_relations WHERE tags ? 'natural' AND tags ->> 'natural' NOT IN ('coastline')"
        }
      ]
    },
    {
      "id": "ocean",
      "type": "polygon",
      "minzoom": 0,
      "maxzoom": 20,
      "queries": [
        {
          "minzoom": 0,
          "maxzoom": 9,
          "sql": "SELECT tags, geom FROM osm_ocean_simplified"
        },
        {
          "minzoom": 10,
          "maxzoom": 20,
          "sql": "SELECT tags, geom FROM osm_ocean"
        }
      ]
    },
    {
      "id": "point",
      "type": "point",
      "minzoom": 1,
      "maxzoom": 20,
      "queries": [
        {
          "minzoom": 1,
          "maxzoom": 2,
          "sql": "SELECT jsonb_build_object('name', tags->'name', 'population', tags -> 'population', 'place', tags -> 'place') as tags, geom FROM osm_point_z$zoom WHERE tags != '{}' AND (tags ->> 'place' = 'country')"
        },
        {
          "minzoom": 3,
          "maxzoom": 8,
          "sql": "SELECT jsonb_build_object('name', tags->'name', 'population', tags -> 'population', 'place', tags -> 'place', 'capital', tags -> 'capital') as tags, geom FROM osm_point_z$zoom WHERE tags != '{}'"
        },
        {
          "minzoom": 9,
          "maxzoom": 20,
          "sql": "SELECT jsonb_build_object('name', tags->'name', 'population', tags->'population', 'capital', tags->'capital', 'entrance', tags->'entrance', 'place', tags->'place', 'telescope:type', tags->'telescope:type', 'tower:construction', tags->'tower:construction', 'advirtising', tags->'advertising', 'military', tags->'military', 'generator:method', tags->'generator:method', 'generator:source', tags->'generator:source', 'tower:type', tags->'tower:type', 'religion', tags->'religion', 'diplomatic', tags->'diplomatic', 'office', tags->'office', 'waterway', tags->'waterway', 'ford', tags->'ford', 'barrier', tags->'barrier', 'aeroway', tags->'aeroway', 'railway', tags->'railway', 'highway', tags->'highway', 'parking', tags->'parking', 'emergency', tags->'emergency', 'information', tags->'information', 'vending', tags->'vending', 'golf', tags->'golf', 'shop', tags->'shop', 'sport', tags->'sport', 'leisure', tags->'leisure', 'man_made', tags->'man_made', 'castle_type', tags->'castle_type', 'artwork_type', tags->'artwork_type', 'memorial', tags->'memorial', 'historic', tags->'historic', 'tourism', tags->'tourism', 'power', tags->'power', 'natural', tags->'natural', 'amenity', tags->'amenity', 'leisure', tags->'leisure') AS tags, geom FROM osm_point_z$zoom WHERE tags != '{}'"
        }
      ]
    },
    {
      "id": "power",
      "type": "polygon",
      "minzoom": 13,
      "maxzoom": 20,
      "queries": [
        {
          "minzoom": 13,
          "maxzoom": 20,
          "sql": "SELECT jsonb_build_object('power', tags->'power') AS tags, geom FROM osm_ways WHERE tags ->> 'power' IN ('cable', 'line', 'minor_line', 'plant', 'substation')"
        }
      ]
    },
    {
      "id": "railway",
      "type": "polygon",
      "minzoom": 9,
      "maxzoom": 20,
      "queries": [
        {
          "minzoom": 9,
          "maxzoom": 12,
          "sql": "SELECT jsonb_build_object('railway', tags->'railway', 'service', tags->'service') AS tags, geom FROM osm_railway_z$zoom"
        },
        {
          "minzoom": 13,
          "maxzoom": 20,
          "sql": "SELECT jsonb_build_object('railway', tags->'railway', 'service', tags->'service') AS tags, geom FROM osm_ways WHERE tags ? 'railway'"
        }
      ]
    },
    {
      "id": "route",
      "type": "polygon",
      "minzoom": 9,
      "maxzoom": 20,
      "queries": [
        {
          "minzoom": 9,
          "maxzoom": 12,
          "sql": "SELECT tags, geom FROM osm_route_z$zoom WHERE tags ? 'route'"
        },
        {
          "minzoom": 13,
          "maxzoom": 20,
          "sql": "SELECT tags, geom FROM osm_ways WHERE tags ? 'route'"
        }
      ]
    },
    {
      "id": "waterway",
      "type": "polygon",
      "minzoom": 6,
      "maxzoom": 20,
      "queries": [
        {
          "minzoom": 6,
          "maxzoom": 10,
          "sql": "SELECT jsonb_build_object('tunnel', tags->'tunnel') AS tags, geom FROM osm_waterway_z$zoom WHERE tags ->> 'waterway' IN ('river')"
        },
        {
          "minzoom": 10,
          "maxzoom": 12,
          "sql": "SELECT jsonb_build_object('tunnel', tags->'tunnel') AS tags, geom FROM osm_waterway_z$zoom WHERE tags ->> 'waterway' IN ('river', 'stream')"
        },
        {
          "minzoom": 13,
          "maxzoom": 20,
          "sql": "SELECT jsonb_build_object('tunnel', tags->'tunnel') AS tags, geom FROM osm_ways WHERE tags ? 'waterway'"
        }
      ]
    }
  ]
}
```

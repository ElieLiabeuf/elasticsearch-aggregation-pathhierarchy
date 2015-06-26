Elasticsearch Aggregation Path Hierarchy Plugin
=========================================

This plugins adds the possibility to create hierarchical aggregations.
Each term is split on a provided separator (default "/") then aggregated by level.
For a complete example see https://github.com/elastic/elasticsearch/issues/8896

This is a multi bucket aggregation.

| elasticsearch | Path hierarchy plugin     |
|---------------|---------------------------|
| 1.6.0         | 1.6.0.0                   |


Installation
------------

`bin/plugin --install path_hierarchy --url "https://github.com/opendatasoft/elasticsearch-aggregation-pathhierarchy/releases/download/v1.6.0.0/elasticsearch-aggregation-pathhierarchy-1.6.0.0.zip"`


Usage
-----

### Parameters

 - `field` or ~~`script`~~ : field to aggregate on
 - `separator` : separator for path hierarchy (default to "/")
 - `order` : order parameter to define how to sort result. Allowed parameters are `_term`, `_count` or sub aggregation name. Default to {"_count": "desc}.
 - `max_depth`: Set maximum depth level. `-1` means no limit. Default to 3.


Examples
-------

#### String field

```
Add data:

PUT /filesystem
{
  "mappings": {
    "file": {
      "properties": {
        "path": {
          "type": "string",
          "index": "not_analyzed",
          "doc_values": true
        }
      }
    }
  }
}

PUT /filesystem/file/1
{
  "path": "/My documents/Spreadsheets/Budget_2013.xls",
  "views": 10
}

PUT /filesystem/file/2
{
  "path": "/My documents/Spreadsheets/Budget_2014.xls",
  "views": 7
}

PUT /filesystem/file/3
{
  "path": "/My documents/Test.txt",
  "views": 1
}



Path hierarchy request :

GET /filesystem/file/_search?search_type=count
{
  "aggs": {
    "tree": {
      "path_hierarchy": {
        "field": "path",
        "separator": "/",
        "order": {"_term": "desc"}
      },
      "aggs": {
        "total_views": {
          "sum": {
            "field": "views"
          }
        }
      }
    }
  }
}


Result :

"aggregations": {
    "tree": {
       "buckets": [
          {
             "key": "My documents/Test.txt",
             "doc_count": 1,
             "total_views": {
                "value": 1
             }
          },
          {
             "key": "My documents/Spreadsheets/Budget_2014.xls",
             "doc_count": 1,
             "total_views": {
                "value": 7
             }
          },
          {
             "key": "My documents/Spreadsheets/Budget_2013.xls",
             "doc_count": 1,
             "total_views": {
                "value": 10
             }
          },
          {
             "key": "My documents/Spreadsheets",
             "doc_count": 2,
             "total_views": {
                "value": 17
             }
          },
          {
             "key": "My documents",
             "doc_count": 3,
             "total_views": {
                "value": 18
             }
          }
       ]
    }
 }

```

#### Script

```

Not working...


```

Initial project
-------

https://github.com/opendatasoft/elasticsearch-aggregation-pathhierarchy

License
-------

This software is under The MIT License (MIT)

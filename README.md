# go-techLog1C
Парсер технологического журнала, основанный на стеке технологий Golang (goroutines) + Redis + Elasticsearch.

### Elasticsearch
Используется как конечная точка хранения распарсенных логов 1С. Нереляционная база данных Elasticsearch позволяет достаточно оперативно получать необходимую информацию, используя поисковые обратные индексы. В связке с Kibana - можно строить выборки, используя декларативный KSQL язык запросов. При использовании расширения XPACK (30 day free trial) можно задействовать модуль машинного обучения (Machine Learning), что позволит выявлять аномалии и различные зависимости от показателей тех журнала, например утечки памяти. 

## Elasticsearch settings:

1.XMX/XMS
-Xms32600m
-Xmx32600m

[Documentation](<https://www.elastic.co/guide/en/elasticsearch/guide/master/_limiting_memory_usage.html>)

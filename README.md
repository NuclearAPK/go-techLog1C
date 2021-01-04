# go-techLog1C
Парсер технологического журнала, основанный на стеке технологий Golang (goroutines) + Redis + Elasticsearch.

## Elasticsearch
Используется как конечная точка хранения распарсенных логов 1С. Нереляционная база данных Elasticsearch позволяет достаточно оперативно получать необходимую информацию, используя поисковые обратные индексы. В связке с Kibana - можно строить выборки, используя декларативный KSQL язык запросов. При использовании расширения XPACK (30 day free trial) можно задействовать модуль машинного обучения (Machine Learning), что позволит выявлять аномалии и различные зависимости от показателей тех журнала, например утечки памяти. 

#### Elasticsearch особенности настроек:
Elasticsearch работает на JVM, поэтому очень требователен к настройкам памяти. По умолчанию размер кучи установлен в 1Gb, что крайне мало при массовой операции вставки документов в индексы Elasticsearch. 

1. **XMX/XMS** рекомендуется установить равным половине оперативной памяти, доступной физической/виртуальной машине. Например, если у вас 16Gb, то возможный вариант настройки:
-Xms8G
-Xmx8G [Документация](<https://www.elastic.co/guide/en/elasticsearch/guide/master/_limiting_memory_usage.html>)

2. **Очистка старых индексов (использование index lifecycle policy)**. Чем детальнее технологический журнал - тем сильнее будет расти его объем, а значит и индексы в Elasticsearch, это может привести к нехватке свободного места на дисках, где хранятся индексы. Лучшая практика - спустя N дней удалять индексы. 
Все что необходимо - задать политику жизненного цикла индексов
https://www.elastic.co/guide/en/elasticsearch/reference/current/set-up-lifecycle-policy.html
После этого задать темплейт, который будет включать индексы тех журнала 
```
PUT _template/tech_journal_template
{
  "index_patterns": ["tech-*"],                 
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 1,
    "index.lifecycle.name": "tech_journal_policy"    
  }
}
```

**Известные проблемы**:
1. circuit_breaking_exception,  [request] Data too large, data for [<reused_arrays>] would be larger than limit of:
Измените параметры XMX/XMS
2. es_rejected_execution_exception: rejected execution of coordinating operation
Уменьшите уровень параллелизма (maxdop), подберите оптимальный размер блока на единицу bulk операции (elastic_bulksize), увеличьте параметр elastic_maxretries в config файле. 


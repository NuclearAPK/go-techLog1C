# go-techLog1C
Парсер технологического журнала, основанный на стеке технологий [Golang goroutines](<https://golang.org/>) + [Redis](<https://redis.io/>) + [Elasticsearch](<https://www.elastic.co>).

Стек является кросс-платформенным.

![Упрощенная схема взаимодействия](https://github.com/NuclearAPK/go-techLog1C/blob/main/notation.png)

## Запуск парсера

Необходимо иметь установленный инстанс СУБД Redis, а так же Elasticsearch (версия > 7.0)
Непосредственно запустить парсер можно командой:
**go run**

или собрать exe/bin командой:
**go build**

**Windows**: собранный **exe** можно запускать через планировщик заданий с заданной периодичностью. В этом случае - в настройки задания планировщика нужно прописать рабочую папку в параметрах. Лучшей практикой является использование **bat** файла, примерное содержание:
```
@echo off
cd "S:\go-techLog1C\"
techLog1C.exe
echo on
```
**Linux**: используйте **crontab**. Пример запуска парсера с периодичностью 5 минут:
```
*/5**** /usr/local/techLog1C
```

#### Настройки парсера
Все настройки указываются в файле settings.yaml
```
# Расположение логов тех журнала 1С
patch: "D:\\tmp\\1C_event_log\\1C_event_log"
#
# Удалять табуляции в контекстных строках
delete_tabs_in_contexts: true
#
# Заменять постфиксы виртуальных таблиц в контекстах, например #tt36 на #tt 
# Может использоваться для группировки контекстов
delete_postfix_in_name_virtual_tables: true
#
# Параметры подключения к Redis
redis_addr: "localhost:6379"
redis_login: ""
redis_password: ""
redis_database: 0
#
# Параметры подключения к Elasticsearch
elastic_addr: "http://localhost:9200"
elastic_login: ""
elastic_password: ""
elastic_maxretries: 10
elastic_bulksize: 2000000
#
# Правила формирования индекса Elasticsearch
# Пример: "tech_journal_{event}_yyyyMMddhh", где event - CONN, EXCP, etc...
elastic_indx: "tech_journal_{event}_yyyyMMddhh"
#
# Типы событий тех журнала, которые могут содержать длинные строки '...' и переносы строк \n
tech_log_details_events: "Context|Txt|Descr|DeadlockConnectionIntersections|ManagerList|ServerList|Sql|Sdbl"
#
# Путь, где будут распологаться логи программы
patch_logfile: "D:\\tmp\\techLogData\\log"
#
# Глубина фиксации ошибок при работе программы:
#   1 - только ошибки
#   2 - ошибки и предупреждения 
#   3 - ошибки, предупреждения и информация
log_level: 3
#
# Уровень параллелизма
maxdop: 14
#
# 0 - отключена сортировка файлов по размеру, 1 - сортировка по убыванию, 2 - сортировка по возрастанию
# полезно включать при массовых операциях, для равномерного распределения файлов по потокам
sorting: 0
```

## Redis
NoSQL key-value СУБД. В стеке выполняет роль кэша, для хранения параметров файлов, которые обрабатываются в текущий момент времени и те, которые были обработаны (последняя позиция файла). Почему не используется простой текстовый файл? Все просто - парсер работает в многопоточном режиме, что требует доступ до файла в режиме записи из нескольких потоков. Redis позволяет решать следующие кейсы:
1. Если файла тех. журнала уже нет, то при запуске парсера анализируются ключи в redis базе и проверяется существование файлов. Если файлы не существуют - ключи таких файлов удаляются. 
2. Так же при запуске еще одного инстанса парсера - будут пропущены файлы тех. журнала, обрабатываемые другими инстансами.

Рекомендуется задать параметр в config файле redis_database, который задает номер базы (в redis 16 баз, от 0 до 15, по умолчанию используется база с индексом 0).

[Redis для Windows](<https://github.com/microsoftarchive/redis/releases>)  

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
Чтобы ее применить к уже существующим индексам, нужно выполнить следующий запрос
```
PUT tech_*/_settings
{
  "index.lifecycle.name": "tech_journal_policy" 
}
```

**Известные проблемы**:
1. circuit_breaking_exception,  [request] Data too large, data for [<reused_arrays>] would be larger than limit of:
Измените параметры XMX/XMS
2. es_rejected_execution_exception: rejected execution of coordinating operation
Уменьшите уровень параллелизма (maxdop), подберите оптимальный размер блока на единицу bulk операции (elastic_bulksize), увеличьте параметр elastic_maxretries в config файле. 
3. Долгая индексация, update mapping index. После загрузки каждого документа - если не задана карта индекса - карта создается, что создает накладные расходы, при количестве документов > 1 млн, обновление карты может не уложиться в таймаут по умолчанию (30 сек.). Поэтому, рекомендуется создавать map карты индекса (см. каталог **maps**)
4. При вставке записей в Elasticsearch возникает ошибка [400] validation_exception: Validation Failed: 1: this action would add...
Необходимо увеличить количество шардов 
```
curl -u USER:PASSWD -X PUT localhost:9200/_cluster/settings -H "Content-Type: application/json" -d '{ "persistent": { "cluster.max_shards_per_node": "3000" } }'
{"acknowledged":true,"persistent":{"cluster":{"max_shards_per_node":"3000"}},"transient":{}}
```
5. Время событий в Кибане показывается относительно UTC. 
Все что необходимо сделать - выставить в настройках необходимый часовой пояс
![Настройки часового пояса](https://github.com/NuclearAPK/go-techLog1C/blob/main/utc.jpg)


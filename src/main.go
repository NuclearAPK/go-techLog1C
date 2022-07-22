package main

import (
	"bytes"
	"crypto/md5"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/gomodule/redigo/redis"
	logr "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

//=======================================================================================
type conf struct {
	Patch                            string `yaml:"patch"`
	RedisAddr                        string `yaml:"redis_addr"`
	RedisLogin                       string `yaml:"redis_login"`
	RedisPassword                    string `yaml:"redis_password"`
	RedisDatabase                    int    `yaml:"redis_database"`
	ElasticAddr                      string `yaml:"elastic_addr"`
	ElasticLogin                     string `yaml:"elastic_login"`
	ElasticPassword                  string `yaml:"elastic_password"`
	ElasticIndx                      string `yaml:"elastic_indx"`
	ElasticMaxRetrires               int    `yaml:"elastic_maxretries"`
	ElasticBulkSize                  int64  `yaml:"elastic_bulksize"`
	TechLogDetailsEvents             string `yaml:"tech_log_details_events"`
	MaxDop                           int    `yaml:"maxdop"`
	Sorting                          int    `yaml:"sorting"`
	PatchLogFile                     string `yaml:"patch_logfile"`
	LogLevel                         int    `yaml:"log_level"`
	LogLifeSpan                      int    `yaml:"log_life_span"`
	DeleteTabsInContexts             bool   `yaml:"delete_tabs_in_contexts"`
	DeletePostfixInNameVirtualTables bool   `yaml:"delete_postfix_in_name_virtual_tables"`
	InsecureSkipVerify               bool   `yaml:"skip_verify_certificates"`
}

type bulkResponse struct {
	Errors bool `json:"errors"`
	Items  []struct {
		Index struct {
			ID     string `json:"_id"`
			Result string `json:"result"`
			Status int    `json:"status"`
			Error  struct {
				Type   string `json:"type"`
				Reason string `json:"reason"`
				Cause  struct {
					Type   string `json:"type"`
					Reason string `json:"reason"`
				} `json:"caused_by"`
			} `json:"error"`
		} `json:"index"`
	} `json:"items"`
}

type files struct {
	Path          string
	Size          int64
	LastPosition  int64
	FileDate      string
	DataCreate    time.Time
	ProcessNameID string
	BlokingID     string
}

func (c *conf) getConfig() *conf {

	yamlFile, err := ioutil.ReadFile("./conf/settings.yaml")
	if err != nil {
		log.Printf("yamlFile.Get err   #%v ", err)
	}
	err = yaml.Unmarshal(yamlFile, c)
	if err != nil {
		log.Fatalf("Unmarshal: %v", err)
	}

	return c
}

// получаем дату время в формате jdata
func getDateEvent(fileDate, word string) string {

	year := fileDate[0:2]
	month := fileDate[2:4]
	day := fileDate[4:6]
	minuts := fileDate[6:8]

	dateEventArray := []string{"20", year, "-", month, "-", day, "T", minuts, ":", word}
	buffer := bytes.Buffer{}

	for _, val := range dateEventArray {
		buffer.WriteString(val)
	}

	return buffer.String()
}

func track() time.Time {
	return time.Now()
}

func duration(start time.Time) {
	logr.WithFields(logr.Fields{
		"object": "Reading tech log 1C",
		"title":  "Succeful reading",
	}).Infof("Final time is: %v\n", time.Since(start))
}

func getFileParametersRedis(conn redis.Conn, idFile string) int64 {

	valueTmpRedis, err := redis.String(conn.Do("GET", idFile))
	if err != nil {
		return 0
	}

	valueRedis, err := strconv.ParseInt(valueTmpRedis, 10, 64)
	if err != nil {
		return 0
	}

	return valueRedis
}

func setFileParametersRedis(conn redis.Conn, idFile string, pos int64) {

	conn.Do("SET", idFile, strconv.FormatInt(pos, 10))
}

// удаление ключа в redis
func deleteFileParametersRedis(conn redis.Conn, idFile string) {
	conn.Do("DEL", idFile)
}

func getMapEvent(str *string) map[string]string {

	var (
		Value    string
		Property string
	)

	strEvent := strings.Split(*str, ",")

	paramets := make(map[string]string)
	for idxStr, Event := range strEvent {

		// поиск символа =
		runeIndex := strings.Index(Event, "=")

		if runeIndex > 0 {

			PropertyTmp := strings.ToLower(Event[0:runeIndex])
			Property = strings.Replace(PropertyTmp, ":", "_", 1)
			ValueTmp := Event[runeIndex+1:]
			Value = ValueTmp

		} else {
			Value = Event
			switch idxStr {
			case 0:
				Property = "duration"
			case 1:
				Property = "event_techlog"
			case 2:
				Property = "level"
			default:
				Property = "unclassified"
			}
		}

		paramets[Property] = Value
	}
	return paramets
}

// заменяет заданные символы в строках, подходящих под регулярные выражения на новые символы
func replaceGaps(str *string, exp string, old string, new string) {
	regexGaps := regexp.MustCompile(exp)
	gapsStrings := regexGaps.FindAllString(*str, -1)
	for _, gapString := range gapsStrings {
		rightStringTmp := strings.Replace(*str, gapString, strings.ReplaceAll(gapString, old, new), -1)
		*str = rightStringTmp
	}
}

func replaceSymbols(str *string, config *conf) {
	if config.DeleteTabsInContexts {
		*str = strings.ReplaceAll(*str, "\t", "")
	}

	if config.DeletePostfixInNameVirtualTables {
		regex := regexp.MustCompile(`(?m)#tt[0-9]+`)
		*str = regex.ReplaceAllString(*str, "#tt")
	}

	var re = regexp.MustCompile(`(?m)\r\n`)

	i := re.FindStringIndex(*str)
	if i != nil && len(i) == 2 && (i[0] < 2 || i[1] == len(*str)) {
		*str = strings.Replace(*str, "\r\n", "", 1)
	}
}

// Reverse возвращает инвентированную строку
func Reverse(s string) string {
	r := []rune(s)
	for i, j := 0, len(r)-1; i < len(r)/2; i, j = i+1, j-1 {
		r[i], r[j] = r[j], r[i]
	}
	return string(r)
}

func isLetter(c rune) bool {
	return ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z')
}

func getFilesPacked(arrFiles []*files, maxdop int) map[int][]files {

	take := make(map[int][]files)

	lenFiles := len(arrFiles)

	var j int = 0
	for i := 0; i < lenFiles; i++ {
		take[j] = append(take[j], *arrFiles[i])
		j++
		if j == maxdop {
			j = 0
		}
	}
	return take
}

func readFile(file files) ([]byte, int64, error) {

	currPosition := file.LastPosition

	openFile, err := os.Open(file.Path)
	if err != nil {
		return nil, 0, err
	}

	buffer := make([]byte, 64)
	var data []byte

	openFile.Seek(file.LastPosition, 0)

	for {
		n, err := openFile.Read(buffer)
		currPosition += int64(n)
		if err == io.EOF { // если конец файла
			break // выходим из цикла
		}
		data = append(data, buffer[:n]...)
	}

	openFile.Close()
	return data, currPosition, nil
}

// формирование наименование индекса по правилам, заданным в conf файле
func getIndexName(config *conf) string {
	today := time.Now()

	str := strings.ReplaceAll(config.ElasticIndx, "yyyy", strconv.Itoa(today.Year()))
	str = strings.ReplaceAll(str, "MM", strconv.Itoa(int(today.Month())))
	str = strings.ReplaceAll(str, "dd", strconv.Itoa(today.Day()))
	str = strings.ReplaceAll(str, "hh", strconv.Itoa(today.Hour()))
	str = strings.ReplaceAll(str, "mm", strconv.Itoa(today.Minute()))
	str = strings.ReplaceAll(str, "ss", strconv.Itoa(today.Second()))

	return str
}

// считываем карты индексов в соответствие
func getMappings() map[string]string {

	mapping := make(map[string]string)
	files, err := getFilesArray("./maps/")

	if err != nil {
		logr.WithFields(logr.Fields{
			"object": "Maps files",
			"title":  "Failure to scan directory",
		}).Error(err)
	}

	separator := string(os.PathSeparator)

	for _, file := range files {
		data, _, err := readFile(file)
		if err != nil {
			logr.WithFields(logr.Fields{
				"object": "Map file",
				"title":  "Failure to read map file",
			}).Error(err)
		}

		tmpKey := strings.Split(file.Path, separator)[1]
		key := tmpKey[0 : len(tmpKey)-4]
		mapping[key] = string(data)
	}
	return mapping
}

func createElasticsearchClient(config *conf) (*elasticsearch.Client, error) {

	cfgElastic := elasticsearch.Config{
		Addresses:     []string{config.ElasticAddr},
		Username:      config.ElasticLogin,
		Password:      config.ElasticPassword,
		RetryOnStatus: []int{502, 503, 504, 429},
		RetryBackoff:  func(i int) time.Duration { return time.Duration(i) * 100 * time.Millisecond },
		MaxRetries:    config.ElasticMaxRetrires,
		EnableMetrics: true,
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout: time.Second * 5,
			}).DialContext,

			ResponseHeaderTimeout: time.Second * 4, // prevent hanging connections
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: config.InsecureSkipVerify,
			},
		},
	}
	es, err := elasticsearch.NewClient(cfgElastic)
	return es, err
}

func jobExtractTechLogs(filesInPackage []files, keyInPackage int, config *conf, c chan int) {

	var (
		rightString string
		indexName   = getIndexName(config)
		res         *esapi.Response
		raw         map[string]interface{}
		blk         *bulkResponse
	)

	// 1. подключаемся к эластичному
	es, _ := createElasticsearchClient(config)

	// 2. подключаемся к redis
	conn, _ := redis.Dial("tcp", config.RedisAddr,
		redis.DialUsername(config.RedisLogin),
		redis.DialPassword(config.RedisPassword),
		redis.DialDatabase(config.RedisDatabase),
	)

	defer conn.Close()

	// 3. считываем мэппинг для индексов elastic из map файлов
	mapping := getMappings()

	// 4. работаем с файлами
	RegExpEvents := fmt.Sprintf("(%s)=", config.TechLogDetailsEvents)
	re := regexp.MustCompile("[0-9][0-9]:[0-9][0-9].[0-9]+-")
	reContextstrings := regexp.MustCompile(RegExpEvents)

	for _, file := range filesInPackage {

		data, currentPosition, err := readFile(file)

		if err != nil {
			deleteFileParametersRedis(conn, file.BlokingID)
			logr.WithFields(logr.Fields{
				"object": "Data",
				"title":  "Open file data",
			}).Fatal(err)
		}

		if data == nil {
			deleteFileParametersRedis(conn, file.BlokingID)
			continue
		}

		headings := re.FindAllString(string(data), -1)
		words := re.Split(string(data), -1)

		if headings == nil {
			deleteFileParametersRedis(conn, file.BlokingID)
			continue
		}

		var mapEventsBuffer = map[string]*bytes.Buffer{}
		mapIndicies := make(map[string]string)

		// 0.9. разбор строк, разделенных регулярным выражением по времени событий
		// пробегаемся по частям строк с заголовками
		var IndexPostfix int = 0

		for idx, word := range headings {
			word := strings.TrimRight(word, "-")

			dataEvent := getDateEvent(file.FileDate, word)
			var multilineMap = make(map[string]string)

			if reContextstrings.MatchString(words[idx+1]) {

				lenWords := len(words[idx+1])
				garbageStrings := reContextstrings.Split(words[idx+1], -1)

				var tmpLen int = 0

				for i := len(garbageStrings) - 1; i > 0; i-- {

					garbageString := strings.TrimRight(garbageStrings[i], ",")

					var sb strings.Builder
					var lenSb int = 0
					lenGarbageString := len(garbageStrings[i])

					for j := (lenWords - lenGarbageString - tmpLen - 1); j > 0; j-- {

						c := words[idx+1][j]
						lenSb++

						if isLetter(rune(c)) {
							sb.WriteByte(c)
						} else if c == ',' {
							break
						}
					}

					tmpLen += lenSb + lenGarbageString
					replaceSymbols(&garbageString, config)
					multilineMap[strings.ToLower(Reverse(sb.String()))] = garbageString
				}
				rightString = strings.TrimRight(garbageStrings[0], ",")
			} else {
				rightString = words[idx+1]
				replaceSymbols(&rightString, config)
			}

			replaceGaps(&rightString, `(?m)('[\S\s]*?')|("[\S\s]*?")`, ",", " ")

			paramets := getMapEvent(&rightString)
			paramets["date"] = dataEvent
			paramets["processNameID"] = file.ProcessNameID

			for keyM, valueM := range multilineMap {
				paramets[keyM] = valueM
			}
			paramets["SourceFile"] = file.Path

			// Конвертация карты в JSON
			empData, err := json.Marshal(paramets)
			if err != nil {
				deleteFileParametersRedis(conn, file.BlokingID)
				logr.WithFields(logr.Fields{
					"object": "Data",
					"title":  "Failure marshal struct",
				}).Fatal(err)
			}

			hash := md5.Sum(empData)
			md5String := hex.EncodeToString(hash[:])

			jsonStr := append(empData, "\n"...)

			// заголовок bulk
			idxName := strings.Replace(indexName, "{event}", strings.ToLower(paramets["event_techlog"]), -1)
			mapIndicies[paramets["event_techlog"]] = idxName

			meta := []byte(fmt.Sprintf(`{ "index" : { "_index" : "%s","_id" : "%s" } }%s`, idxName, md5String, "\n"))

			keyBuffer := paramets["event_techlog"] + "_" + strconv.Itoa(IndexPostfix)
			if mapEventsBuffer[keyBuffer] == nil {
				mapEventsBuffer[keyBuffer] = new(bytes.Buffer)
			}
			// заголовок + source события
			mapEventsBuffer[keyBuffer].Grow(len(meta) + len(jsonStr))
			mapEventsBuffer[keyBuffer].Write(meta)
			mapEventsBuffer[keyBuffer].Write(jsonStr)

			if int64(mapEventsBuffer[keyBuffer].Len()) >= config.ElasticBulkSize {
				IndexPostfix++
			}
		}

		if config.LogLevel == 3 {
			logr.WithFields(logr.Fields{
				"object": "Data",
				"title":  "Succeful reading",
			}).Infof("Package %d, file %s, start_position: %d, end position: %d", keyInPackage, file.Path, file.LastPosition, currentPosition)
		}
		// обходим события. Каждому событию соответстует bulk буффер
		for keyM, buf := range mapEventsBuffer {

			keyMaster := keyM[0:strings.Index(keyM, "_")]
			idxName := mapIndicies[keyMaster]
			res, err = es.Indices.Exists([]string{idxName})
			if err != nil && config.LogLevel == 2 {
				logr.WithFields(logr.Fields{
					"object": "Elastic",
					"title":  "Indices.Exists",
				}).Warning(err)
			}
			res.Body.Close()

			if res.StatusCode == 404 {
				currentMapping := mapping[strings.ToLower(keyMaster)]
				_, err = es.Indices.Create(
					idxName,
					es.Indices.Create.WithBody(strings.NewReader(currentMapping)),
					es.Indices.Create.WithWaitForActiveShards("1"),
					es.Indices.Create.WithTimeout(60),
				)
				if err != nil {
					deleteFileParametersRedis(conn, file.BlokingID)
					logr.WithFields(logr.Fields{
						"object": "Elastic",
						"title":  "Cannot create index",
					}).Fatal(err)
				}
			}

			// 0.11. отправляем в эластик
			res, err = es.Bulk(
				bytes.NewReader(buf.Bytes()),
				es.Bulk.WithIndex(idxName),
				es.Bulk.WithRefresh("false"),
			)
			if err != nil {
				deleteFileParametersRedis(conn, file.BlokingID)
				logr.WithFields(logr.Fields{
					"object": "Elastic",
					"title":  "Failure indexing batch",
				}).Fatal(err)
			}
			// если ошибка запроса - выводим ошибку
			if res.IsError() {
				if err := json.NewDecoder(res.Body).Decode(&raw); err != nil {
					deleteFileParametersRedis(conn, file.BlokingID)
					logr.WithFields(logr.Fields{
						"object": "Elastic",
						"title":  "Failure to to parse response body",
					}).Fatal(err)
				} else {
					deleteFileParametersRedis(conn, file.BlokingID)
					logr.WithFields(logr.Fields{
						"object": "Elastic",
						"title":  "Request",
					}).Errorf("[%d] %s: %s (%s)",
						res.StatusCode,
						raw["error"].(map[string]interface{})["type"],
						raw["error"].(map[string]interface{})["reason"],
						file,
					)
				}
				// Успешный ответ может по-прежнему содержать ошибки для определенных документов...
			} else {
				if err := json.NewDecoder(res.Body).Decode(&blk); err != nil {
					deleteFileParametersRedis(conn, file.BlokingID)
					logr.WithFields(logr.Fields{
						"object": "Elastic",
						"title":  "Failure to to parse response body",
					}).Fatal(err)
				} else {
					for _, d := range blk.Items {
						// ... для любых других HTTP статусов > 201 ...
						if d.Index.Status > 201 {
							deleteFileParametersRedis(conn, file.BlokingID)
							// ... и распечатать статус ответа и информацию об ошибке ...
							logr.WithFields(logr.Fields{
								"object": "Elastic",
								"title":  "Request",
							}).Errorf("[%d]: %s: %s: %s: %s",
								d.Index.Status,
								d.Index.Error.Type,
								d.Index.Error.Reason,
								d.Index.Error.Cause.Type,
								d.Index.Error.Cause.Reason,
							)
						}
					}
				}
			}
			// Закрываем тело ответа, чтобы предотвратить достижение предела для горутин или дескрипторов файлов.
			res.Body.Close()
		}
		deleteFileParametersRedis(conn, file.BlokingID)          // удаляем ключ
		setFileParametersRedis(conn, file.Path, currentPosition) // записываем позицию в базу
	}
	c <- keyInPackage
}

func initLogging(c *conf) {

	year, month, day := time.Now().Date()
	if _, err := os.Stat(c.PatchLogFile); os.IsNotExist(err) {
		os.Mkdir(c.PatchLogFile, 2)
	}

	logFileName := c.PatchLogFile + "techLog1C_" + strconv.Itoa(year) + strconv.Itoa(int(month)) + strconv.Itoa(day) + ".json"
	fileLog, err := os.OpenFile(logFileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)

	if err != nil {
		log.Fatal(err)
	}

	logr.SetFormatter(&logr.JSONFormatter{})
	logr.SetOutput(fileLog)

}

func deleteOldLogFiles(c *conf) {

	lifeSpan := c.LogLifeSpan
	if lifeSpan == 0 {
		lifeSpan = 1
	}

	files, err := getFilesArray(c.PatchLogFile)
	if err != nil {
		logr.WithFields(logr.Fields{
			"object": "Scan log directory",
			"title":  "Select life span files",
		}).Warning(err)
		return
	}

	t := time.Now()

	for _, file := range files {
		fname := filepath.Base(file.Path)
		if !strings.HasPrefix(fname, "techLog1C_") {
			// избегает удаления не наших лог файлов, например при неверном указании пользователем нашего лог каталога
			continue
		}
		duration := t.Sub(file.DataCreate)

		if (int(duration.Hours()) - 24*c.LogLifeSpan) >= 0 {
			if err = os.Remove(file.Path); err != nil {
				logr.WithFields(logr.Fields{
					"object": "Scan log directory",
					"title":  "Delete life span files",
				}).Warning(err)
			}
		}
	}
}

func getFilesArray(root string) ([]files, error) {
	var arrFiles []files

	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			arrFiles = append(arrFiles, files{Path: path, Size: info.Size(), DataCreate: info.ModTime()})
		}
		return nil
	})
	return arrFiles, err
}

//=======================================================================================
func main() {

	defer duration(track())

	// считываем конфиг
	var config conf
	config.getConfig()

	// подключаем логи
	initLogging(&config)
	deleteOldLogFiles(&config)

	// maxdop установка
	runtime.GOMAXPROCS(config.MaxDop)

	// удалим ключи, которые больше не используются
	conn, err := redis.Dial("tcp", config.RedisAddr,
		redis.DialUsername(config.RedisLogin),
		redis.DialPassword(config.RedisPassword),
		redis.DialDatabase(config.RedisDatabase),
	)

	if err != nil {
		logr.WithFields(logr.Fields{
			"object": "Redis",
			"title":  "Unable to connect",
		}).Fatal(err)
	}
	defer conn.Close()

	// проверим что es доступен
	es, err := createElasticsearchClient(&config)
	if err != nil {
		logr.WithFields(logr.Fields{
			"object": "Elastic",
			"title":  "Creating the client",
		}).Fatal(err)
	}

	res, err := es.Info()
	if err != nil {
		logr.WithFields(logr.Fields{
			"object": "Elastic",
			"title":  "Unable to get response",
		}).Fatal(err)
	}
	res.Body.Close()

	keys, _ := redis.Strings(conn.Do("KEYS", "*"))
	for _, key := range keys {
		var currKey string
		if strings.Contains(key, "job_") {
			currKey = strings.Replace(key, "job_", "", -1)
		} else {
			currKey = key
		}
		if _, err := os.Stat(currKey); err != nil {
			if os.IsNotExist(err) {
				// если файла больше нет - удалим запись из базы
				deleteFileParametersRedis(conn, key)
			} else {
				logr.WithFields(logr.Fields{
					"object": "File tech journal",
				}).Warning(err)
			}
		}
	}

	c := make(chan int)

	// получаем файлы логов, сортируем по размеру, определяем в пакеты заданий
	arr, err := getFilesArray(config.Patch)
	if err != nil {
		logr.WithFields(logr.Fields{
			"object": "Data",
			"title":  "Failure to scan directory",
		}).Error(err)
	}

	/* проверяем что файлы не заблокированы, ставим блокировку в основном сеансе
	считываем позицию файла и сравниваем с текущим размером, если ничего не изменилось -
	удаляем такие файлы из массива
	*/

	var listFiles []*files
	separator := string(os.PathSeparator)

	for i := 0; i < len(arr); i++ {

		// проверим что файла нет в текущей обработке
		jobFile := "job_" + arr[i].Path
		if getFileParametersRedis(conn, jobFile) == 1 {
			continue
		}

		// получаем последнюю прочитанную позицию из redis
		lastPosition := getFileParametersRedis(conn, arr[i].Path)
		if lastPosition == arr[i].Size || arr[i].Size < 100 {
			continue
		}

		// получаем дату из имени файла и тип процесса
		fileSplitter := strings.Split(arr[i].Path, separator)
		lenArray := len(fileSplitter)
		if lenArray < 2 {
			continue
		}

		// устанавливаем блокировку на файл
		setFileParametersRedis(conn, jobFile, 1)

		arr[i].LastPosition = lastPosition
		arr[i].FileDate = strings.TrimRight(fileSplitter[lenArray-1], ".log")
		arr[i].ProcessNameID = strings.ToLower(fileSplitter[lenArray-2])
		arr[i].BlokingID = jobFile

		listFiles = append(listFiles, &arr[i])
	}

	if config.Sorting != 0 {
		sort.SliceStable(listFiles, func(i, j int) bool {
			if config.Sorting == 1 {
				return listFiles[i].Size > listFiles[j].Size
			}
			return listFiles[i].Size < listFiles[j].Size
		})
	}

	packages := getFilesPacked(listFiles, config.MaxDop)

	for keyInPackage, filesInPackage := range packages {
		go jobExtractTechLogs(filesInPackage, keyInPackage, &config, c)
	}

	for i := 0; i < len(packages); i++ {
		gopherID := <-c // Получает значение от канала
		logr.WithFields(logr.Fields{
			"job id": gopherID,
			"status": "ok",
		}).Info("Job extract tech log 1C")
	}
}

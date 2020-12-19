package main

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
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
	Patch                string `yaml:"patch"`
	RedisAddr            string `yaml:"redis_addr"`
	RedisLogin           string `yaml:"redis_login"`
	RedisPassword        string `yaml:"redis_password"`
	ElasticAddr          string `yaml:"elastic_addr"`
	ElasticLogin         string `yaml:"elastic_login"`
	ElasticPassword      string `yaml:"elastic_password"`
	ElasticIndx          string `yaml:"elastic_indx"`
	TechLogDetailsEvents string `yaml:"tech_log_details_events"`
	MaxDop               int    `yaml:"maxdop"`
	PatchLogFile         string `yaml:"patch_logfile"`
	LogLevel             int    `yaml:"log_level"`
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

// FilePathWalkDir - получаем все файлы внутри директории
func FilePathWalkDir(root string) ([]string, error) {
	var files []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			files = append(files, path)
		}
		return nil
	})
	return files, err
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
	log.Printf("Final time is: %v\n", time.Since(start))
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

			PropertyTmp := Event[0:runeIndex]
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

func replaceGaps(str *string) {
	regexGaps := regexp.MustCompile("'{1}[\\w+,]+[\\w+]{1}'{1}")
	gapsStrings := regexGaps.FindAllString(*str, -1)
	for _, gapString := range gapsStrings {
		rightStringTmp := strings.Replace(*str, gapString, strings.ReplaceAll(gapString, ",", " "), -1)
		*str = rightStringTmp
	}
}

// Reverse returns its argument string reversed rune-wise left to right.
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

func getFilesPacked(files []string, maxdop int) map[int][]string {

	take := make(map[int][]string)

	lenFiles := len(files)

	if (lenFiles <= maxdop) || (maxdop == 0) {

		take[0] = files

	} else {

		var num int = lenFiles / maxdop
		var remainder int = lenFiles - num*maxdop

		for i := 0; i < maxdop; i++ {

			var Package []string

			for j := 0; j < num; j++ {
				Package = append(Package, files[j+num*i])
			}

			take[i] = Package
		}

		if remainder < maxdop {
			for i := 0; i < remainder; i++ {
				take[i] = append(take[i], files[lenFiles-i-1])
			}
		}
	}

	return take
}

func readFile(file string, pos *int64) ([]byte, error) {

	openFile, err := os.Open(file)
	if err != nil {
		return nil, err
	}

	buffer := make([]byte, 64)
	var data []byte

	openFile.Seek(*pos, 0)

	for {
		n, err := openFile.Read(buffer)
		*pos = *pos + int64(n)
		if err == io.EOF { // если конец файла
			break // выходим из цикла
		}
		data = append(data, buffer[:n]...)
	}

	openFile.Close()
	return data, nil
}

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

func jobExtractTechLogs(filesInPackage []string, keyInPackage int, config *conf, c chan int) {

	var (
		rightString string
		indexName   = getIndexName(config)
		buf         bytes.Buffer
		res         *esapi.Response
		raw         map[string]interface{}
		blk         *bulkResponse
	)

	// 0.2. подключаемся к эластичному
	cfgElastic := elasticsearch.Config{
		Addresses: []string{config.ElasticAddr},
		Username:  config.ElasticLogin,
		Password:  config.ElasticPassword,
	}
	es, err := elasticsearch.NewClient(cfgElastic)
	if err != nil {
		logr.WithFields(logr.Fields{
			"object": "Elastic",
			"title":  "Creating the client",
		}).Fatal(err)
	}

	res, err = es.Info()
	if err != nil {
		logr.WithFields(logr.Fields{
			"object": "Elastic",
			"title":  "Unable to get response",
		}).Fatal(err)
	}

	// 0.3. подключаемся к redis
	conn, err := redis.Dial("tcp", config.RedisAddr,
		redis.DialUsername(config.RedisLogin),
		redis.DialPassword(config.RedisPassword),
	)

	if err != nil {
		logr.WithFields(logr.Fields{
			"object": "Redis",
			"title":  "Unable to connect",
		}).Fatal(err)
	}
	defer conn.Close()

	// 0.4. работаем с файлами
	RegExpEvents := fmt.Sprintf("(%s)=", config.TechLogDetailsEvents)
	re := regexp.MustCompile("[0-9][0-9]:[0-9][0-9].[0-9]+-")
	reContextstrings := regexp.MustCompile(RegExpEvents)

	for _, file := range filesInPackage {

		// проверим что файла нет в текущей обработке
		jobFile := "job_" + file
		inProgress := getFileParametersRedis(conn, jobFile)
		if inProgress == 1 {
			continue
		}

		// устанавливаем блокировку на файл
		setFileParametersRedis(conn, jobFile, 1)

		// 0.5 получаем дату из имени файла и тип процесса
		fileSplitter := strings.Split(file, "\\")
		lenArray := len(fileSplitter)
		if lenArray < 2 {
			continue
		}

		fileDate := strings.TrimRight(fileSplitter[lenArray-1], ".log")

		// 0.6. проверяем индекс, если нет - создаем
		idx := indexName + "_" + strings.ToLower(fileSplitter[lenArray-2])
		res, err = es.Indices.Exists([]string{idx})
		if err != nil && config.LogLevel == 2 {
			logr.WithFields(logr.Fields{
				"object": "Elastic",
				"title":  "Indices.Exists",
			}).Warning(err)
		}

		res.Body.Close()

		if res.StatusCode == 404 {
			_, err = es.Indices.Create(idx)
			if err != nil {
				deleteFileParametersRedis(conn, jobFile)
				logr.WithFields(logr.Fields{
					"object": "Elastic",
					"title":  "Cannot create index",
				}).Fatal(err)
			}
		}

		// 0.7. получаем идентификатор файла для DB Redis
		idFile := fileSplitter[len(fileSplitter)-2] + "_" + fileDate
		pos := getFileParametersRedis(conn, idFile)

		// 0.8. считываем файл с позиции idFile
		data, err := readFile(file, &pos)

		if err != nil {
			deleteFileParametersRedis(conn, jobFile)
			logr.WithFields(logr.Fields{
				"object": "Data",
				"title":  "Open file data",
			}).Fatal(err)
		}

		if data == nil {
			continue
		}

		headings := re.FindAllString(string(data), -1)
		words := re.Split(string(data), -1)

		if headings == nil {
			continue
		}

		// 0.9. разбор строк, разделенных регулярным выражением по времени событий
		// пробегаемся по частям строк с заголовками
		for idx, word := range headings {
			word := strings.TrimRight(word, "-")

			dataEvent := getDateEvent(fileDate, word)
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

					for j := lenWords - lenGarbageString - tmpLen; j > 0; j-- {

						c := words[idx+1][j]
						lenSb++

						if isLetter(rune(c)) {
							sb.WriteByte(c)
						} else if c == ',' {
							break
						}
					}

					tmpLen += lenSb + lenGarbageString
					multilineMap[Reverse(sb.String())] = garbageString
				}

				rightString = strings.TrimRight(garbageStrings[0], ",")
			} else {
				rightString = words[idx+1]
			}

			replaceGaps(&rightString)

			paramets := getMapEvent(&rightString)
			paramets["date"] = dataEvent

			for keyM, valueM := range multilineMap {
				paramets[keyM] = valueM
			}
			paramets["SourceFile"] = file

			// Конвертация карты в JSON
			empData, err := json.Marshal(paramets)
			if err != nil {
				deleteFileParametersRedis(conn, jobFile)
				logr.WithFields(logr.Fields{
					"object": "Data",
					"title":  "Failure marshal struct",
				}).Fatal(err)
			}

			hash := md5.Sum(empData)
			md5String := hex.EncodeToString(hash[:])

			jsonStr := append(empData, "\n"...)

			// заголовок bulk
			meta := []byte(fmt.Sprintf(`{ "index" : { "_id" : "%s" } }%s`, md5String, "\n"))

			buf.Grow(len(meta) + len(jsonStr))
			buf.Write(meta)
			buf.Write(jsonStr)

		}

		if config.LogLevel == 3 {
			logr.WithFields(logr.Fields{
				"object": "Data",
				"title":  "Succeful reading",
			}).Infof("Package %d, file %s", keyInPackage, file)
		}

		// 0.11. отправляем в эластик
		res, err = es.Bulk(bytes.NewReader(buf.Bytes()), es.Bulk.WithIndex(idx))
		if err != nil {
			deleteFileParametersRedis(conn, jobFile)
			logr.WithFields(logr.Fields{
				"object": "Elastic",
				"title":  "Failure indexing batch",
			}).Fatal(err)
		}
		// если ошибка запроса - выводим ошибку
		if res.IsError() {
			if err := json.NewDecoder(res.Body).Decode(&raw); err != nil {
				deleteFileParametersRedis(conn, jobFile)
				logr.WithFields(logr.Fields{
					"object": "Elastic",
					"title":  "Failure to to parse response body",
				}).Fatal(err)
			} else {
				deleteFileParametersRedis(conn, jobFile)
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
				deleteFileParametersRedis(conn, jobFile)
				logr.WithFields(logr.Fields{
					"object": "Elastic",
					"title":  "Failure to to parse response body",
				}).Fatal(err)
			} else {
				for _, d := range blk.Items {
					// ... для любых других HTTP статусов > 201 ...
					if d.Index.Status > 201 {
						deleteFileParametersRedis(conn, jobFile)
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
					} else {
						setFileParametersRedis(conn, idFile, pos) // 0.12. записываем позицию в базу
						deleteFileParametersRedis(conn, jobFile)  // 0.13. удаляем ключ
					}
				}
			}
		}

		// Закрываем тело ответа, чтобы предотвратить достижение предела для горутин или дескрипторов файлов.
		res.Body.Close()
		buf.Reset() // сбрасываем буфер
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

//=======================================================================================
func main() {

	defer duration(track())

	// считываем конфиг
	var config conf
	config.getConfig()

	// подключаем логи
	initLogging(&config)

	// maxdop установка
	runtime.GOMAXPROCS(config.MaxDop)
	c := make(chan int)

	files, err := FilePathWalkDir(config.Patch)
	if err != nil {
		logr.WithFields(logr.Fields{
			"object": "Data",
			"title":  "Failure to scan directory",
		}).Error(err)
	}

	// выделяем пакеты файлов
	if len(files) == 0 {
		return
	}

	packages := getFilesPacked(files, config.MaxDop)
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

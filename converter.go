package main

import (
	"bufio"
	"compress/gzip"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/oschwald/geoip2-golang"
	"github.com/pkg/profile"
	"github.com/pquerna/ffjson/ffjson"

	"github.com/random9s/Analytics-Pipeline/cache"
	"github.com/random9s/Analytics-Pipeline/log"
)

//Available flags
var (
	fname    string
	help     bool
	in       bool
	cpu, mem bool
	tuner    int

	readLines, writeLines int64
)

func parseFlags() {
	flag.StringVar(&fname, "f", "", "log file name")
	flag.BoolVar(&cpu, "cpu", false, "profile cpu (can only run cpu or mem, not both)")
	flag.BoolVar(&mem, "mem", false, "profile memory (can only run cpu or mem, not both)")
	flag.BoolVar(&in, "i", false, "read from stdin")
	flag.BoolVar(&help, "h", false, "print help")
	flag.IntVar(&tuner, "t", 1, "number will be multiplied by number of logical cores")
	flag.Parse()

	if help {
		flag.PrintDefaults()
		os.Exit(0)
	}

	if fname == "" && !in {
		flag.PrintDefaults()
		fmt.Println("file name must be provided")
		os.Exit(1)
	}
}

func exitOnErr(err error) {
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

//csvfields
var csvFields = []string{
	"event_fc",           //0
	"http_user_agent",    //1
	"event_ori",          //2
	"event_uid",          //3
	"event_ord",          //4
	"uri_did",            //5
	"event_lc",           //6
	"event_lf",           //7
	"uri_l",              //8
	"event_dr",           //9
	"event_sp",           //10
	"uri_tz",             //11
	"event_st",           //12
	"remote_addr",        //13
	"uri_av",             //14
	"event_rid",          //15
	"uri_an",             //16
	"uri_app",            //17
	"request_time_float", //18
	"event_res",          //19
	"uri_ov",             //20
	"uri_os",             //21
	"event_typ",          //22
	"uri_kv",             //23
	"event_ct",           //24
	"uri_sv",             //25
	"client_id",          //26
	"request_uri",        //27
	"event_vs",           //28
	"event_ps",           //29
	"event_ts",           //30
	"event_n",            //31
	"event_m",            //32
	"event_tc",           //33
	"uri_dm",             //34
	"uri_fv",             //35
	"event_tg",           //36
	"event_sn",           //37
	"uri_q",              //38
	"uri_uid",            //39
	"uri_id",             //40
	"event_sc",           //41
	"geo_country",        //42
	"geo_city",           //43
}

func toString(i interface{}) string {
	var str string
	switch i.(type) {
	case uint, uint8, uint16, uint32, uint64,
		int, int8, int16, int32, int64:
		var v = i.(int64)
		if v != 0 {
			str = strconv.FormatInt(v, 10)
		}
	case float32, float64:
		var v = i.(float64)
		if v != 0.0 {
			str = strconv.FormatFloat(v, 'E', -1, 64)
		}
	}

	return str
}

func handleLog(c *cache.Cache, db *geoip2.Reader, logT *log.Log) []string {
	var out = make([]string, len(csvFields), len(csvFields))

	//handle event section
	var e = logT.Event
	if logT.Event != nil {
		out[0] = toString(e.Fc)
		out[2] = e.Ori
		out[3] = e.UID
		out[4] = e.Ord
		out[6] = toString(e.Lc)
		out[7] = toString(e.Lf)
		out[9] = toString(e.Dr)
		out[10] = e.Sp
		out[12] = e.St
		out[15] = e.Rid
		out[19] = toString(e.Resolution)
		out[22] = toString(e.Type)
		out[24] = toString(e.Ct)
		out[28] = e.Vs
		out[29] = e.Ps

		var res = toString(e.Timestamp)
		if e.Timestamp > 0 {
			var t1 = int64(e.Timestamp / 1000)
			if t1 > 0 {
				var ut = time.Unix(t1, 0)
				res = ut.Format("2006-01-02 03:04:05.0")
			}
		}

		out[30] = res
		out[31] = e.Name
		out[32] = e.M
		out[33] = toString(e.Tc)
		out[36] = e.Tg
		out[37] = e.Sn
		out[41] = e.Sc
	}

	//loop key/value of request parameters
	reqURI, err := logT.ParseReqURI()
	exitOnErr(err)
	for k, v := range reqURI.Query() {
		var k = "uri_" + strings.ToLower(k)
		var vStr = strings.Join(v, "+")

		for i := 0; i < len(csvFields); i++ {
			if strings.Compare(k, csvFields[i]) == 0 {
				out[i] = vStr
			}
		}

		switch k {
		case "uri_d":
			out[5] = vStr
		case "uri_dt":
			out[34] = vStr
		case "uri_v":
			out[25] = vStr
		case "uri_p":
			out[35] = vStr
		}
	}

	//Add remainding stuff
	out[1] = logT.HTTPUserAgent
	out[13] = logT.RemoteAddr
	out[18] = logT.ParseRequestTime()
	out[26] = logT.ClientID
	out[27] = reqURI.Path

	var cleanIP = strings.Trim(logT.RemoteAddr, "\n")
	var city, country string
	city, country, ok := c.Load(cleanIP)
	if !ok {
		ip := net.ParseIP(cleanIP)
		if ip != nil {
			record, err := db.City(ip)
			exitOnErr(err)

			city, ok = record.City.Names["en"]
			if !ok {
				city = "nil"
			}
			country, ok = record.Country.Names["en"]
			if !ok {
				country = "nil"
			}

			c.Add(cleanIP, city, country)
		}
	}

	out[42] = country
	out[43] = city
	return out
}

type customWriter struct {
	fp *os.File
	zw *gzip.Writer
	w  *csv.Writer
}

func fanOut(in chan *string, out chan *[]string, wg *sync.WaitGroup, c *cache.Cache, db *geoip2.Reader) {
	wg.Add(1)

	go func() {
		//all log lines start with something similar to this, so we'll just cut this from the beginning of every line
		var trimN = len("[2017-12-01 20:55:08 ~ SDK ~ 0] ")

		for linePtr := range in {
			line := *linePtr

			//All log lines should begin with the same thing, so we can trim that immediately
			line = line[trimN:]

			//Always remove newline
			if line[len(line)-1] == byte(10) {
				line = line[:len(line)-1]
			}

			//check if bad value exists and replace with empty string
			if strings.Contains(line, "\"event\":[]") {
				line = strings.Replace(line, "\"event\":[]", "", 1)
			}

			//unmarshal new log line
			var logT = new(log.Log)
			exitOnErr(ffjson.Unmarshal([]byte(line), logT))

			//create csv line
			var csvLine = handleLog(c, db, logT)
			out <- &csvLine
		}

		wg.Done()
	}()
}

func main() {
	if cpu {
		defer profile.Start().Stop()
	} else if mem {
		defer profile.Start(profile.MemProfile).Stop()
	}

	parseFlags()

	//prep cache
	c := cache.New()

	//open geoip database
	db, err := geoip2.Open("GeoLite2-City.mmdb")
	exitOnErr(err)
	defer db.Close()

	//create reader to read data from source
	var reader *bufio.Reader
	if fname != "" {
		//open gzip file
		fp, err := os.Open(fname)
		exitOnErr(err)
		defer fp.Close()

		//Create gzip reader
		zipReader, err := gzip.NewReader(fp)
		exitOnErr(err)
		defer zipReader.Close()

		//wrap zip reader in bufio
		reader = bufio.NewReader(zipReader)
	} else if in {
		zipReader, err := gzip.NewReader(os.Stdin)
		exitOnErr(err)
		defer zipReader.Close()

		reader = bufio.NewReader(zipReader)
	}

	//create map for files and close all files on exit
	var dateFiles = make(map[string]*customWriter)

	var in = make(chan *string)
	var out = make(chan *[]string)
	var wg = sync.WaitGroup{}

	for i := 0; i < runtime.NumCPU()*tuner; i++ {
		fanOut(in, out, &wg, c, db)
	}

	var done = make(chan bool)
	go func() {
		var i = 0

		for csvLinePtr := range out {
			var csvLine = *csvLinePtr

			//var t1 = logT.ParseRequestTime()
			var t1 = csvLine[18] //request time
			var outfile = fmt.Sprintf("%s/sdk-log-%s.csv.gz", filepath.Dir(fname), strings.Replace(strings.Split(t1, " ")[0], "-", ".", -1))

			cw, exists := dateFiles[outfile]
			if !exists {
				//create and wrap file pointer with gzipped csv writer
				fp, err := os.OpenFile(outfile, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0766)
				exitOnErr(err)
				zw := gzip.NewWriter(fp)
				w := csv.NewWriter(zw)
				//create new custom writer
				cw = &customWriter{
					fp,
					zw,
					w,
				}
				//store for later use
				dateFiles[outfile] = cw
			}

			exitOnErr(cw.w.Write(csvLine))
			writeLines++

			//batch records to write to disk every 100k
			if i%1000000 == 0 && i != 0 {
				for _, v := range dateFiles {
					v.w.Flush()
					exitOnErr(v.w.Error())
				}
			}
		}

		done <- true
	}()

	//read until EOF
	for {
		//Read and clean json line
		line, err := reader.ReadString('\n')
		if err == io.EOF {
			break
		}
		exitOnErr(err)

		in <- &line
		readLines++
	}

	close(in)
	wg.Wait()

	close(out)
	<-done

	for _, v := range dateFiles {
		//final buffer flush
		v.w.Flush()
		exitOnErr(v.w.Error())

		exitOnErr(v.zw.Close())
		exitOnErr(v.fp.Close())
	}

	fmt.Printf("Read %d lines, wrote %d lines\n", readLines, writeLines)
}

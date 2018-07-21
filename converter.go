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
	"regexp"
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
	"uri_n",              //7
	"event_lf",           //8
	"uri_l",              //9
	"event_dr",           //10
	"event_sp",           //11
	"uri_tz",             //12
	"event_st",           //13
	"remote_addr",        //14
	"uri_av",             //15
	"event_rid",          //16
	"uri_access_token",   //17
	"uri_an",             //18
	"uri_app",            //19
	"request_time_float", //20
	"event_res",          //21
	"uri_ov",             //22
	"uri_os",             //23
	"event_typ",          //24
	"uri_kv",             //25
	"event_ct",           //26
	"uri_sv",             //27
	"client_id",          //28
	"request_uri",        //29
	"event_vs",           //30
	"event_ps",           //31
	"event_ts",           //32
	"event_n",            //33
	"event_m",            //34
	"event_tc",           //35
	"uri_dm",             //36
	"uri_fv",             //37
	"event_tg",           //38
	"event_sn",           //39
	"uri_q",              //40
	"uri_appkey",         //41
	"uri_length",         //42
	"uri_pretty",         //43
	"uri_uid",            //44
	"uri_title",          //45
	"uri_category",       //46
	"uri_id",             //47
	"event_sc",           //48
	"uri_f",              //49
	"geo_country",        //50
	"geo_city",           //51
}

func handleLog(c *cache.Cache, db *geoip2.Reader, logT *log.Log) []string {
	var out = make([]string, len(csvFields), len(csvFields))

	//handle event section
	var e = logT.Event
	if logT.Event != nil {
		out[0] = strconv.FormatInt(e.Fc, 10)
		out[2] = e.Ori
		out[3] = e.UID
		out[4] = e.Ord
		out[6] = strconv.FormatInt(e.Lc, 10)
		out[8] = strconv.FormatInt(e.Lf, 10)
		out[10] = strconv.FormatInt(e.Dr, 10)
		out[11] = e.Sp
		out[13] = e.St
		out[16] = e.Rid
		out[21] = strconv.FormatInt(e.Resolution, 10)
		out[24] = strconv.FormatInt(e.Type, 10)
		out[26] = strconv.FormatInt(e.Ct, 10)
		out[30] = e.Vs
		out[31] = e.Ps

		var res = strconv.FormatInt(e.Timestamp, 10)
		if e.Timestamp > 0 {
			var t1 = int64(e.Timestamp / 1000)
			var ut = time.Unix(t1, 0)
			res = ut.Format("2006-01-02 03:04:05.0")
		}
		out[32] = res

		out[33] = e.Name
		out[34] = e.M
		out[35] = strconv.FormatInt(e.Tc, 10)
		out[38] = e.Tg
		out[39] = e.Sn
		out[48] = e.Sc
	}

	//loop key/value of request parameters
	u, err := logT.ParseReqURI()
	exitOnErr(err)
	for k, v := range u.Query() {
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
			out[36] = vStr
		case "uri_v":
			out[27] = vStr
		case "uri_p":
			out[37] = vStr
		}
	}

	//Add remainding stuff
	out[1] = logT.HTTPUserAgent
	out[14] = logT.RemoteAddr
	out[20] = logT.ParseRequestTime()
	out[28] = logT.ClientID
	out[29] = logT.ReqURI

	var cleanIP = strings.Trim(logT.RemoteAddr, "\n")
	var city, country string
	city, country, ok := c.Load(cleanIP)
	if !ok {
		ip := net.ParseIP(cleanIP)
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

	out[50] = country
	out[51] = city
	return out
}

type customWriter struct {
	fp *os.File
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
				//check if beginning of bad value exists and replace with empty string
			} else if strings.Contains(line, "\"event\":[") {
				re := regexp.MustCompile("\"event\":[*]")
				line = re.ReplaceAllString(line, "")
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

	go func() {
		for csvLinePtr := range out {
			var csvLine = *csvLinePtr

			//var t1 = logT.ParseRequestTime()
			var t1 = csvLine[20] //request time
			var outfile = fmt.Sprintf("%s/sdk-log-%s.csv.gz", filepath.Dir(fname), strings.Replace(strings.Split(t1, " ")[0], "-", ".", -1))

			cw, exists := dateFiles[outfile]
			if !exists {
				//create and wrap file pointer with gzipped csv writer
				fp, err := os.OpenFile(outfile, os.O_RDWR|os.O_CREATE, 0766)
				exitOnErr(err)
				w := csv.NewWriter(gzip.NewWriter(fp))
				//create new custom writer
				cw = &customWriter{
					fp,
					w,
				}
				//store for later use
				dateFiles[outfile] = cw
			}

			exitOnErr(cw.w.Write(csvLine))
			cw.w.Flush()
			exitOnErr(cw.w.Error())
		}
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
	}

	close(in)
	wg.Wait()
	close(out)

	for _, v := range dateFiles {
		v.fp.Close()
	}
}

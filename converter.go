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
	"strconv"
	"strings"

	geoip2 "github.com/oschwald/geoip2-golang"
	"github.com/pquerna/ffjson/ffjson"
	"github.com/random9s/Analytics-Pipeline/cache"
	"github.com/random9s/Analytics-Pipeline/log"
)

//Available flags
var (
	fname string
	help  bool
)

func parseFlags() {
	flag.StringVar(&fname, "f", "", "log file name")
	flag.BoolVar(&help, "h", false, "print help")
	flag.Parse()

	if help {
		flag.PrintDefaults()
		os.Exit(0)
	}

	if fname == "" {
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
	"uri_p",              //5
	"uri_did",            //6
	"uri_v",              //7
	"event_lc",           //8
	"uri_n",              //9
	"event_lf",           //10
	"uri_l",              //11
	"event_dr",           //12
	"event_sp",           //13
	"uri_tz",             //14
	"event_st",           //15
	"remote_addr",        //16
	"uri_av",             //17
	"event_rid",          //18
	"uri_access_token",   //19
	"uri_an",             //20
	"uri_app",            //21
	"request_time_float", //22
	"event_res",          //23
	"uri_ov",             //24
	"uri_os",             //25
	"event_typ",          //26
	"uri_kv",             //27
	"event_ct",           //28
	"uri_sv",             //29
	"client_id",          //30
	"request_uri",        //31
	"event_vs",           //32
	"event_ps",           //33
	"event_ts",           //34
	"event_n",            //35
	"event_m",            //36
	"event_tc",           //37
	"uri_dm",             //38
	"uri_fv",             //39
	"event_tg",           //40
	"event_sn",           //41
	"uri_q",              //42
	"uri_appkey",         //43
	"uri_tmp",            //44
	"uri_length",         //45
	"uri_pretty",         //46
	"uri_uid",            //47
	"uri_title",          //48
	"uri_category",       //49
	"uri_id",             //50
	"event_sc",           //51
	"uri_f",              //52
	"geo_country",        //53
	"geo_city",           //54
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
		out[8] = strconv.FormatInt(e.Lc, 10)
		out[10] = strconv.FormatInt(e.Lf, 10)
		out[12] = strconv.FormatInt(e.Dr, 10)
		out[13] = e.Sp
		out[15] = e.St
		out[18] = e.Rid
		out[23] = strconv.FormatInt(e.Resolution, 10)
		out[26] = strconv.FormatInt(e.Type, 10)
		out[28] = strconv.FormatInt(e.Ct, 10)
		out[32] = e.Vs
		out[33] = e.Ps
		out[34] = strconv.FormatInt(e.Timestamp, 10)
		out[35] = e.Name
		out[36] = e.M
		out[37] = strconv.FormatInt(e.Tc, 10)
		out[40] = e.Tg
		out[41] = e.Sn
		out[51] = e.Sc
	}

	//loop key/value of request parameters
	u, err := logT.ParseReqURI()
	exitOnErr(err)
	for k, v := range u.Query() {
		var k = fmt.Sprintf("uri_%s", strings.ToLower(k))
		for i := 0; i < len(csvFields); i++ {
			if strings.Compare(k, csvFields[i]) == 0 {
				out[i] = strings.Join(v, "+")
			}
		}
	}

	//Add remainding stuff
	out[1] = logT.HTTPUserAgent
	out[16] = logT.RemoteAddr
	out[22] = strconv.FormatFloat(logT.ReqTime, 'E', -1, 64)
	out[30] = logT.ClientID
	out[31] = logT.ReqURI

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

	out[53] = country
	out[54] = city
	return out
}

func main() {
	parseFlags()

	//prep cache
	c := cache.New()

	//open geoip database
	db, err := geoip2.Open("GeoLite2-City.mmdb")
	exitOnErr(err)
	defer db.Close()

	//open gzip file
	fp, err := os.Open(fname)
	exitOnErr(err)
	defer fp.Close()

	//Create gzip reader
	zipReader, err := gzip.NewReader(fp)
	exitOnErr(err)
	defer zipReader.Close()

	//wrap zip reader in bufio
	reader := bufio.NewReader(zipReader)

	//create out file
	var spl = strings.Split(fname, ".")
	var baseName = spl[0]
	var outpath = fmt.Sprintf("%s.csv.gz", baseName)
	outfp, err := os.OpenFile(outpath, os.O_RDWR|os.O_CREATE, 0666)
	exitOnErr(err)
	defer outfp.Close()

	//create csv gzipped writer
	//csvfp := csv.NewWriter(gzip.NewWriter(outfp))
	csvfp := csv.NewWriter(outfp)

	//write csv header
	exitOnErr(csvfp.Write(csvFields))
	csvfp.Flush()
	exitOnErr(csvfp.Error())

	for {
		//Read and parse json log
		line, err := reader.ReadString('\n')
		if err == io.EOF {
			break
		}
		exitOnErr(err)

		spl := strings.Split(line, " ~ SDK ~ 0] ")
		line = strings.TrimRight(spl[1], "\n")

		//unmarshal new log line
		var logT = new(log.Log)
		exitOnErr(ffjson.Unmarshal([]byte(line), logT))

		//create csv line
		var csvLine = handleLog(c, db, logT)

		//write csv line
		exitOnErr(csvfp.Write(csvLine))
		csvfp.Flush()
		exitOnErr(csvfp.Error())
	}
}

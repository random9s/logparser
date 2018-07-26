package log

import (
	"net"
	"net/url"
	"strings"
	"time"
)

//Log contains log data
type Log struct {
	ReqTime       float64 `json:"REQUEST_TIME_FLOAT"`
	ReqURI        string  `json:"REQUEST_URI"`
	RemoteAddr    string  `json:"REMOTE_ADDR"`
	ClientID      string  `json:"CLIENT_ID"`
	HTTPUserAgent string  `json:"HTTP_USER_AGENT"`
	Event         *Event  `json:"event"`
}

//Event ...
type Event struct {
	Name       string `json:"n"`
	Timestamp  int64  `json:"ts"`
	UID        string `json:"uid"`
	Fc         int64  `json:"fc"`
	Dr         int64  `json:"dr"`
	Vs         string `json:"vs"`
	M          string `json:"m"`
	Tc         int64  `json:"tc"`
	Tg         string `json:"tg"`
	Sn         string `json:"sn"`
	Ps         string `json:"ps"`
	Ct         int64  `json:"ct"`
	Lc         int64  `json:"lc"`
	Sc         string `json:"sc"`
	Lf         int64  `json:"lf"`
	Sp         string `json:"sp"`
	St         string `json:"st"`
	Rid        string `json:rid"`
	Resolution int64  `json:"res"`
	Ori        string `json:"ori"`
	Ord        string `json:"ord"`
	Type       int64  `json:"typ"`
}

//ParseRequestTime ...
func (l *Log) ParseRequestTime() string {
	var t = int64(l.ReqTime / 1.0)
	var ut = time.Unix(t, 0)
	return ut.Format("2006-01-02 03:04:05.0")
}

//ParseReqURI ...
func (l *Log) ParseReqURI() (*url.URL, error) {
	return url.ParseRequestURI(l.ReqURI)
}

//ParseIP ...
func (l *Log) ParseIP() net.IP {
	var cleanIP = strings.Trim(l.RemoteAddr, "\n")
	if cleanIP == "" {
		return nil
	}

	return net.ParseIP(cleanIP)
}

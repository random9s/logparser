package cache

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
)

//Cache ...
type Cache struct {
	c map[string][]string
	*sync.RWMutex
}

//New checks for saved cache file and loads, if exists, or creates empty cache
func New() *Cache {
	var c *Cache
	//	c, err := fromDisk()
	//	if err != nil {
	c = &Cache{
		make(map[string][]string),
		new(sync.RWMutex),
	}
	//}

	return c
}

//Flush writes cache contents to disk
func (c *Cache) Flush() error {
	return c.toDisk()
}

//Add ...
func (c *Cache) Add(ip, city, country string) {
	c.Lock()
	defer c.Unlock()

	c.c[ip] = []string{city, country}

	return
}

//Load ...
func (c *Cache) Load(ip string) (string, string, bool) {
	c.RLock()
	defer c.RUnlock()

	v, ok := c.c[ip]
	if !ok {
		return "", "", false
	}

	if len(v) != 2 {
		return "", "", false
	}

	return v[0], v[1], true
}

func fromDisk() (*Cache, error) {
	var c = make(map[string][]string)

	//open cache file
	fp, err := os.OpenFile(".cache", os.O_RDONLY, 0766)
	if err != nil {
		return nil, err
	}

	defer fp.Close()

	//create reader buffer
	var r = bufio.NewReader(fp)
	for {
		line, err := r.ReadString('\n')
		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, err
		}

		var spl = strings.Split(line, ",")
		c[spl[0]] = []string{spl[1], spl[2]}
	}

	return &Cache{
		c,
		new(sync.RWMutex),
	}, nil
}

func (c *Cache) toDisk() error {
	//create file in necessary
	fp, err := os.OpenFile(".cache", os.O_RDWR|os.O_CREATE, 0766)
	if err != nil {
		return err
	}
	defer fp.Close()

	//create writer buffer
	var w = bufio.NewWriter(fp)
	for k, v := range c.c {
		var line = fmt.Sprintf("%s,%s,%s\n", k, v[0], v[1])
		w.WriteString(line)
	}

	//flush contents to disk
	return w.Flush()
}

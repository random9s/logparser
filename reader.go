package main

/*
func main() {
	var fname string
	flag.StringVar(&fname, "f", "", "csv file name")
	flag.Parse()

	if fname == "" {
		flag.PrintDefaults()
		return
	}

	fp, err := os.Open(fname)
	if err != nil {
		log.Fatal(err)
	}

	r, err := gzip.NewReader(fp)
	if err != nil {
		log.Fatal(err)
	}

	reader := csv.NewReader(r)
	records, err := reader.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(records[:4])
}
*/

package backfiller

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/briandowns/spinner"
	"github.com/schollz/progressbar/v3"
	"github.com/wormhole-foundation/wormhole-explorer/fly/storage"
	"github.com/wormhole-foundation/wormhole/sdk/vaa"
	"go.mongodb.org/mongo-driver/mongo"
	"go.uber.org/zap"
)

type Backfiller struct {
	Filename string
	Workpool *Workpool
}

type Workpool struct {
	Workers int
	Queue   chan string
	WG      sync.WaitGroup
	DB      *mongo.Database
	Log     *zap.Logger
	Bar     *progressbar.ProgressBar
}

func (b *Backfiller) Run() error {
	f, err := os.Open(b.Filename)
	if err != nil {
		return err
	}

	s := spinner.New(spinner.CharSets[14], 100*time.Millisecond)
	s.Color("red")
	s.Suffix = fmt.Sprintf(" counting lines")

	s.Start()
	pLines, err := b.countLines()
	if err != nil {
		return err
	}
	s.Stop()

	fmt.Printf("lines: %d \n ", pLines)

	b.Workpool.Bar = progressbar.Default(int64(pLines))

	counter := 0
	defer f.Close()

	r := bufio.NewReader(f)

	for {
		line, _, err := r.ReadLine() //loading chunk into buffer
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatalf("a real error happened here: %v\n", err)
		}
		b.Workpool.Queue <- string(line)
		counter += 1
		if counter%100 == 0 {
			//			s.Suffix = fmt.Sprintf(" : %d lines", counter)
		}
	}

	for i := 0; i < b.Workpool.Workers; i++ {
		b.Workpool.Queue <- "exit"
	}

	b.Workpool.WG.Wait()

	fmt.Printf("processed %d lines\n", counter)

	return nil
}

func (b *Backfiller) countLines() (int, error) {
	file, _ := os.Open(b.Filename)
	defer file.Close()

	buf := make([]byte, 32*1024)
	count := 0
	lineSep := []byte{'\n'}

	for {
		c, err := file.Read(buf)
		count += bytes.Count(buf[:c], lineSep)

		switch {
		case err == io.EOF:
			return count, nil

		case err != nil:
			return count, err
		}
	}

}

func NewWorkpool(ctx context.Context, workers int) *Workpool {

	wp := Workpool{
		Workers: workers,
		Queue:   make(chan string, workers*1000),
		WG:      sync.WaitGroup{},
		Log:     zap.NewExample(),
	}

	db, err := storage.GetDB(ctx, wp.Log, os.Getenv("MONGODB_URI"), "wormhole")
	if err != nil {
		panic(err)
	}

	wp.DB = db

	for i := 0; i < workers; i++ {
		go wp.process(ctx)
	}

	wp.WG.Add(workers)

	return &wp
}

func (w *Workpool) process(ctx context.Context) error {
	repo := storage.NewRepository(w.DB, w.Log)

	for {
		select {
		case line := <-w.Queue:
			if line == "exit" {
				w.WG.Done()
				return nil
			}
			tokens := strings.Split(line, ",")
			//fmt.Printf("bcid %s, emmiter %s, seq %s\n", header[0], header[1], header[2])

			data, err := hex.DecodeString(tokens[1])
			if err != nil {
				fmt.Printf("error decoding: %v\n", err)
				break
			}

			v, err := vaa.Unmarshal(data)

			if err != nil {
				fmt.Printf("error unmarshaling vaa: %v\n", err)
				break
			}

			//TODO: improve performance by using bulk insert
			err = repo.UpsertVaa(ctx, v, data)
			if err != nil {
				fmt.Printf("error upserting vaa: %v\n", err)
				break
			}

			w.Bar.Add(1) // its safe to call Add concurrently

		}
	}

}

// BackfillerConfiguration represents the application configuration when running as backfiller with default values.
type BackfillerConfiguration struct {
	MongoURI      string `env:"MONGODB_URI,required"`
	MongoDatabase string `env:"MONGODB_DATABASE,required"`
	Filename      string `env:"FILENAME,required"`
	WorkerCount   int    `env:"WORKER_COUNT"`
}

func Run(cfg *BackfillerConfiguration) {
	var filename string
	flag.StringVar(&filename, "file", "", "file to process (mandatory)")

	flag.Parse()

	if os.Getenv("MONGODB_URI") == "" {
		os.Setenv("MONGODB_URI", "mongodb://localhost:27017/")
		fmt.Println("MONGODB_URI not set, using default")
	}

	if filename == "" {
		flag.Usage()
		os.Exit(1)
	}

	ctx := context.Background()

	wp := NewWorkpool(ctx, 100)

	b := Backfiller{
		Filename: filename,
		Workpool: wp,
	}
	err := b.Run()
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println("done!")
}

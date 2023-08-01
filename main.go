package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"github.com/kelseyhightower/envconfig"
	"github.com/pkg/errors"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const AuthFile = "bepensa.json"
const MaxFileSize = 10 * 1 << 20 // 10 MB

const InBucket = "bucket_rmscm02056_yalo"
const OutBucket = "cmrc-integrations"

const RootFolder = "mx_sellout"

const (
	TemplateName   = "bepensa-order"
	BotId          = "bepensa-mx-prd"
	StorefrontName = "bepensa-mx-b2b"
	KeyExpression  = "Record.get('id')"
)

type Config struct {
	ApiUrl          string `split_words:"true" default:"https://api-ww-us-001.yalochat.com/commerce"`
	TemplatesToken  string `split_words:"true"`
	ExecutionsToken string `split_words:"true"`
}

func templatesUrl(cfg *Config) string {
	return cfg.ApiUrl + "/integrations-templates"
}

func executionsUrl(cfg *Config) string {
	return cfg.ApiUrl + "/integrations-executions"
}

type File struct {
	Bucket string
	Name   string
}

func findFiles(
	ctx context.Context,
	inClient *storage.Client,
	bucket string,
) ([]File, error) {
	var names []string

	t := time.Now().Format("20060102")
	query := &storage.Query{
		Prefix:    RootFolder + "/",
		MatchGlob: "**/" + t + "*.csv",
		Delimiter: "/",
	}

	bkt := inClient.Bucket(bucket)

	it := bkt.Objects(ctx, query)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			panic(err)
		}
		names = append(names, attrs.Name)
	}
	files := make([]File, len(names))
	for i, name := range names {
		files[i] = File{
			Bucket: bucket,
			Name:   name,
		}
	}
	return files, nil
}

// splitFile gets the file using the inClient and splits it into pieces
func splitFile(
	ctx context.Context,
	inClient *storage.Client,
	outClient *storage.Client,
	file File,
) ([]File, error) {
	obj := inClient.Bucket(file.Bucket).Object(file.Name)

	var files []File

	r, err := obj.NewReader(ctx)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	reader := csv.NewReader(r)

	header, err := reader.Read()
	if err != nil {
		return nil, err
	}

	var wc *storage.Writer
	var writer *csv.Writer
	var bytesWritten int

	for i := 0; ; i++ {
		record, err := reader.Read()

		// Stop at EOF.
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		// Check if it's time to create a new part file.
		if wc == nil || bytesWritten > MaxFileSize {
			if wc != nil {
				err = wc.Close()
				if err == nil {
					files = append(files, File{OutBucket, wc.Name})
				} else {
					return files, err
				}
			}

			partFileName := fmt.Sprintf(file.Name+"_part_%06d.csv", i)

			objPart := outClient.Bucket(OutBucket).Object(partFileName)

			// Read it back.
			wc = objPart.NewWriter(ctx)
			wc.ContentType = "text/csv"

			writer = csv.NewWriter(wc)
			err = writer.Write(header)
			if err != nil {
				return files, err
			}

			bytesWritten = 0
		}

		row5, err := strconv.ParseFloat(record[5], 64)
		if err != nil {
			return files, errors.Wrap(err, "could not convert row 5 to float")
		}
		// Avoid strict equality due to floating point errors
		if row5-0. > 0.0001 {
			err = writer.Write(record)
			if err != nil {
				return files, err
			}
		}
		writer.Flush()
		// TODO check Err()
		if row5-0. > 0.0001 {
			bytesWritten += len([]byte(strings.Join(record, ","))) + len([]byte("\n"))
		}
	}

	if wc != nil {
		wc.Close()
	}
	return files, nil
}

func ingestFile(
	ctx context.Context,
	tmplClient *TemplatesClient,
	execClient *ExecutionsClient,
	file File,
) (string, error) {
	args := map[string]string{
		"storefrontName": StorefrontName,
		"botId":          BotId,
		"keyExpression":  KeyExpression,
		"bucket":         file.Bucket,
		"file":           file.Name,
	}
	tmpl, err := tmplClient.RenderTemplate(ctx, TemplateName, args)
	if err != nil {
		return "", err
	}

	return execClient.CreateExecution(ctx, tmpl)
}

func main() {
	ctx := context.Background()

	cfg := &Config{}
	envconfig.Process("", cfg)

	f, err := os.Open(AuthFile)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	authJson, err := io.ReadAll(f)

	inClient, err := storage.NewClient(ctx, option.WithCredentialsJSON(authJson))
	if err != nil {
		panic(err)
	}

	outClient, err := storage.NewClient(ctx)
	if err != nil {
		panic(err)
	}

	httpClient := http.DefaultClient
	tmplClient, err := NewTemplatesClient(
		httpClient,
		WithTemplatesEndpoint(templatesUrl(cfg)),
		WithTemplatesToken(cfg.TemplatesToken))

	execClient, err := NewExecutionsClient(
		httpClient,
		WithExecutionsEndpoint(executionsUrl(cfg)),
		WithExecutionsToken(cfg.ExecutionsToken))

	if err != nil {
		panic(err)
	}

	files, err := findFiles(ctx, inClient, InBucket)
	if err != nil {
		panic(err)
	}

	filesExecs := make([][]string, len(files))
	var i, j int
	var fi, fp File
Files:
	for i, fi = range files {
		var parts []File
		parts, err = splitFile(ctx, inClient, outClient, fi)
		fmt.Println(fi.Name + ": " + strconv.Itoa(len(parts)) + " parts")
		if err != nil {
			break Files
		}
		filesExecs[i] = make([]string, len(parts))
		for j, fp = range parts {
			var id string
			id, err = ingestFile(ctx, tmplClient, execClient, fp)
			if err != nil {
				break Files
			}
			filesExecs[i][j] = id
		}
	}
	if err != nil {
		fmt.Printf("Error ingesting\n- file: %s\n- part: %s\n\nerror: %s", fi, fp, err)
	}
	for n, k := range filesExecs {
		fmt.Println(files[n])
		fmt.Println(k)
	}
}

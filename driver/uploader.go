package driver

import (
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	maxPartSize = int64(10 * 1024 * 1024)
)

type FileInfoT struct {
	filePath string
	size     int64
	md5      string
}

type S3Uploader struct {
	client *s3.Client
	log    *log.Logger
	ctx    context.Context
}

func (s S3Uploader) Upload(s3Path string, fchan FileInfoT) (*manager.UploadOutput, error) {
	tmpPaths := strings.SplitN(s3Path, "/", 2)
	bucketName := tmpPaths[0]
	objPath := tmpPaths[1]
	f, err := os.Open(fchan.filePath)
	if err != nil {
		return nil, err
	}
	optFunc := func(opt *manager.Uploader) {
		opt.PartSize = maxPartSize
		opt.Concurrency = 10
	}

	uploader := manager.NewUploader(s.client)
	r, err := uploader.Upload(s.ctx, &s3.PutObjectInput{
		Bucket:           &bucketName,
		Key:              &objPath,
		Body:             f,
		BucketKeyEnabled: false,
		//ContentMD5:           &fchan.md5,
		ContentType:          aws.String("application/octet-stream"),
		Metadata:             map[string]string{"md5": fchan.md5},
		ServerSideEncryption: "",
	}, optFunc)
	return r, err
}

func GetFileInfo(filePath string) (FileInfoT, error) {
	finfo := FileInfoT{}
	file, err := os.Open(filePath)
	if err != nil {
		return finfo, err
	}
	defer file.Close()
	fInfo, _ := file.Stat()
	finfo = FileInfoT{filePath, fInfo.Size(), getMd5(file)}
	return finfo, err
}

func LocalfileInS3(svc *s3.Client, s3Path string, localFile FileInfoT) bool {
	s := strings.SplitN(s3Path, "/", 2)
	head, err := svc.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: &s[0],
		Key:    &s[1],
	})
	if err != nil {
		return false
	}
	if val, ok := head.Metadata["md5"]; ok {
		return localFile.md5 == val
	}
	return false
}

func getMd5(f io.Reader) string {
	hash := md5.New()
	_, err := io.Copy(hash, f)
	if err != nil {
		return ""
	}
	b := fmt.Sprintf("%x", hash.Sum(nil))
	return b
}

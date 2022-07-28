package driver

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"path"
	"path/filepath"
	"time"

	"github.com/jevyzhu/light-stemcell-builder/config"
	"github.com/jevyzhu/light-stemcell-builder/driver/manifests"
	"github.com/jevyzhu/light-stemcell-builder/resources"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// The SDKCreateMachineImageManifestDriver uploads a machine image to S3 and creates an import volume manifest
type SDKCreateMachineImageManifestDriverGen2 struct {
	s3Client *s3.Client
	logger   *log.Logger
}

var cx = context.TODO()

const S3_FOLDER = "bosh-stemcell"

func PresignOpt(s *s3.PresignOptions) {
	s.Expires = 2 * time.Hour
}

// NewCreateMachineImageManifestDriver creates a MachineImageDriver machine image manifest generation
func NewCreateMachineImageManifestDriverGen2(logDest io.Writer, creds config.Credentials) *SDKCreateMachineImageManifestDriverGen2 {
	logger := log.New(logDest, "SDKCreateMachineImageManifestDriverGen2 ", log.LstdFlags)

	cfg, err := awsConfig.LoadDefaultConfig(cx,
		awsConfig.WithCredentialsProvider(credentials.StaticCredentialsProvider{
			Value: aws.Credentials{
				AccessKeyID:     creds.AccessKey,
				SecretAccessKey: creds.SecretKey,
			},
		}), awsConfig.WithRegion(creds.Region))
	if err != nil {
		panic(err)
	}
	client := s3.NewFromConfig(cfg)

	return &SDKCreateMachineImageManifestDriverGen2{
		s3Client: client,
		logger:   logger,
	}
}

// Create uploads a machine image to S3 and returns a presigned URL to an import volume manifest
func (d *SDKCreateMachineImageManifestDriverGen2) Create(driverConfig resources.MachineImageDriverConfig) (resources.MachineImage, error) {
	createStartTime := time.Now()
	defer func(startTime time.Time) {
		d.logger.Printf("completed Create() in %f minutes\n", time.Since(startTime).Minutes())
	}(createStartTime)

	finfo, err := GetFileInfo(driverConfig.MachineImagePath)
	if err != nil {
		return resources.MachineImage{}, err
	}
	imageBaseName := filepath.Base(driverConfig.MachineImagePath)
	keyName := path.Join(driverConfig.BucketFolder, imageBaseName)

	s3Path := path.Join(driverConfig.BucketName, keyName)
	s3Uploader := S3Uploader{d.s3Client, d.logger, cx}
	s3Uploader.log.Printf("uploading image to s3://%s/%s\n", driverConfig.BucketName, keyName)
	uploadStartTime := time.Now()
	if !LocalfileInS3(d.s3Client, s3Path, finfo) {
		_, err := s3Uploader.Upload(s3Path, finfo)
		if err != nil {
			s3Uploader.log.Printf("[%s]: FAILED upload: %v", finfo.filePath, err)
			return resources.MachineImage{}, err
		}
	}
	s3Uploader.log.Printf("finished uploaded image to s3 after %f minutes\n", time.Since(uploadStartTime).Minutes())

	headReqOutput, err := d.s3Client.HeadObject(cx, &s3.HeadObjectInput{
		Bucket: aws.String(driverConfig.BucketName),
		Key:    aws.String(keyName),
	})

	if err != nil {
		return resources.MachineImage{}, fmt.Errorf("fetching properties for uploaded machine image: %s in bucket: %s: %s", keyName, driverConfig.BucketName, err)
	}

	sizeInBytes := headReqOutput.ContentLength

	volumeSizeGB := driverConfig.VolumeSizeGB
	const gbInBytes = 1 << 30
	if volumeSizeGB == 0 {
		// default to size of image if VolumeSize is not provided
		volumeSizeGB = int64(math.Ceil(float64(sizeInBytes) / gbInBytes))
	}

	m, err := d.generateManifest(driverConfig.BucketName, keyName, sizeInBytes, volumeSizeGB, driverConfig.FileFormat)
	if err != nil {
		return resources.MachineImage{}, fmt.Errorf("FAILED to generate machine image manifest: %s", err)
	}

	manifestURL, err := d.uploadManifest(driverConfig.BucketName, driverConfig.BucketFolder, imageBaseName, driverConfig.ServerSideEncryption, m)
	if err != nil {
		return resources.MachineImage{}, err
	}
	machineImage := resources.MachineImage{
		GetURL:     manifestURL,
		DeleteURLs: []string{m.SelfDestructURL, m.Parts.Part.DeleteURL},
	}
	return machineImage, nil
}

func (d *SDKCreateMachineImageManifestDriverGen2) generateManifest(bucketName string, keyName string, sizeInBytes int64, volumeSizeGB int64, fileFormat string) (*manifests.ImportVolumeManifest, error) {
	// Generate presigned GET request
	presignClient := s3.NewPresignClient(d.s3Client)
	presignResult, err := presignClient.PresignGetObject(cx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(keyName),
	}, PresignOpt)

	if err != nil {
		return nil, fmt.Errorf("failed to sign request: %s", err)
	}

	presignedGetURL := presignResult.URL
	d.logger.Printf("generated presigned GET URL %s\n", presignedGetURL)

	// Generate presigned HEAD request for the machine image
	presignResult, err = presignClient.PresignHeadObject(cx, &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(keyName),
	}, PresignOpt)
	if err != nil {
		return nil, fmt.Errorf("failed to sign request: %s", err)
	}
	presignedHeadURL := presignResult.URL

	d.logger.Printf("generated presigned HEAD URL %s\n", presignedHeadURL)

	// Generate presigned DELETE request for the machine image
	presignResult, err = presignClient.PresignDeleteObject(cx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(keyName),
	}, PresignOpt)

	if err != nil {
		return nil, fmt.Errorf("failed to sign request: %s", err)
	}
	presignedDeleteURL := presignResult.URL

	d.logger.Printf("generated presigned DELETE URL %s\n", presignedDeleteURL)

	imageProps := manifests.MachineImageProperties{
		KeyName:      keyName,
		HeadURL:      presignedHeadURL,
		GetURL:       presignedGetURL,
		DeleteURL:    presignedDeleteURL,
		SizeBytes:    sizeInBytes,
		VolumeSizeGB: volumeSizeGB,
		FileFormat:   fileFormat,
	}

	return manifests.New(imageProps), nil
}

func (d *SDKCreateMachineImageManifestDriverGen2) uploadManifest(bucketName string, bucketFolder string, imgBaseName string, serverSideEncryption string, m *manifests.ImportVolumeManifest) (string, error) {
	fileName := fmt.Sprintf("%s-manifest", imgBaseName)
	manifestKey := path.Join(bucketFolder, fileName)
	s3Path := path.Join(bucketName, manifestKey)

	// create presigned GET request for the manifest
	presignClient := s3.NewPresignClient(d.s3Client)
	presignResult, err := presignClient.PresignGetObject(cx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(manifestKey),
	}, PresignOpt)

	if err != nil {
		return "", fmt.Errorf("ERROR: not sign manifest GET request: %v", err)
	}

	manifestGetURL := presignResult.URL

	d.logger.Printf("generated presigned manifest GET URL %s\n", manifestGetURL)

	// create presigned DELETE request for the manifest
	presignResult, err = presignClient.PresignDeleteObject(cx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(manifestKey),
	}, PresignOpt)

	if err != nil {
		return "", fmt.Errorf("failed to sign manifest delete request: %s", err)
	}
	manifestDeleteURL := presignResult.URL
	d.logger.Printf("generated presigned manifest DELETE URL %s\n", manifestDeleteURL)
	m.SelfDestructURL = manifestDeleteURL

	manifestBytes, err := xml.Marshal(m)
	if err != nil {
		return "", fmt.Errorf("serializing machine image manifest: %s", err)
	}

	err = ioutil.WriteFile(fileName, manifestBytes, 0644)
	if err != nil {
		return "", err
	}

	s3Uploader := S3Uploader{d.s3Client, d.logger, cx}
	finfo, err := GetFileInfo(fileName)
	if err != nil {
		return "", err
	}
	uploadStartTime := time.Now()
	_, err = s3Uploader.Upload(s3Path, finfo)
	if err != nil {
		return manifestGetURL, err
	}
	s3Uploader.log.Printf("finished uploaded machine image manifest to s3 after %f seconds\n", time.Since(uploadStartTime).Seconds())
	return manifestGetURL, nil
}

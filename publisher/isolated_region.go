package publisher

import (
	"fmt"
	"io"
	"log"
	"time"

	"github.com/jevyzhu/light-stemcell-builder/collection"
	"github.com/jevyzhu/light-stemcell-builder/driverset"
	"github.com/jevyzhu/light-stemcell-builder/resources"
)

type IsolatedRegionPublisher struct {
	Region               string
	BucketName           string
	BucketFolder         string
	KeepImg              bool
	ServerSideEncryption string
	AmiProperties        resources.AmiProperties
	logger               *log.Logger
}

func NewIsolatedRegionPublisher(logDest io.Writer, c Config) *IsolatedRegionPublisher {
	return &IsolatedRegionPublisher{
		Region:               c.RegionName,
		BucketName:           c.BucketName,
		BucketFolder:         c.BucketFolder,
		ServerSideEncryption: c.ServerSideEncryption,
		AmiProperties: resources.AmiProperties{
			Name:               c.AmiName,
			Description:        c.Description,
			Accessibility:      c.Visibility,
			VirtualizationType: c.VirtualizationType,
			Tags:               c.Tags,
		},
		logger:  log.New(logDest, "IsolatedRegionPublisher ", log.LstdFlags),
		KeepImg: c.KeepImg,
	}
}

func (p *IsolatedRegionPublisher) Publish(ds driverset.IsolatedRegionDriverSet, machineImageConfig MachineImageConfig) (*collection.Ami, error) {
	createStartTime := time.Now()
	defer func(startTime time.Time) {
		p.logger.Printf("completed Publish() in %f minutes\n", time.Since(startTime).Minutes())
	}(createStartTime)

	machineImageDriverConfig := resources.MachineImageDriverConfig{
		MachineImagePath:     machineImageConfig.LocalPath,
		BucketName:           p.BucketName,
		BucketFolder:         p.BucketFolder,
		ServerSideEncryption: p.ServerSideEncryption,
		FileFormat:           machineImageConfig.FileFormat,
		VolumeSizeGB:         machineImageConfig.VolumeSizeGB,
	}

	machineImageDriver := ds.MachineImageDriver()
	machineImage, err := machineImageDriver.Create(machineImageDriverConfig)
	if err != nil {
		return nil, fmt.Errorf("creating machine image: %s", err)
	}

	defer func() {
		if !p.KeepImg {
			err := machineImageDriver.Delete(machineImage)
			if err != nil {
				p.logger.Printf("Failed to delete machine image %s: %s", machineImage.GetURL, err)
			}
		}
	}()

	volumeDriverConfig := resources.VolumeDriverConfig{
		MachineImageManifestURL: machineImage.GetURL,
	}

	volumeDriver := ds.VolumeDriver()
	volume, err := volumeDriver.Create(volumeDriverConfig)
	if err != nil {
		return nil, fmt.Errorf("creating volume: %s", err)
	}

	defer func() {
		err := volumeDriver.Delete(volume)
		if err != nil {
			p.logger.Printf("Failed to delete volume %s: %s", volume.ID, err)
		}
	}()

	snapshotDriverConfig := resources.SnapshotDriverConfig{
		VolumeID: volume.ID,
	}

	snapshotDriver := ds.CreateSnapshotDriver()
	snapshot, err := snapshotDriver.Create(snapshotDriverConfig)
	if err != nil {
		return nil, fmt.Errorf("creating snapshot: %s", err)
	}

	createAmiDriver := ds.CreateAmiDriver()
	createAmiDriverConfig := resources.AmiDriverConfig{
		SnapshotID:    snapshot.ID,
		AmiProperties: p.AmiProperties,
	}

	sourceAmi, err := createAmiDriver.Create(createAmiDriverConfig)
	if err != nil {
		return nil, fmt.Errorf("creating ami: %s", err)
	}

	amis := collection.Ami{
		VirtualizationType: p.AmiProperties.VirtualizationType,
	}
	amis.Add(sourceAmi)

	// TODO: cleanup machine images and volumes

	return &amis, nil
}

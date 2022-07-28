package publisher

import "github.com/jevyzhu/light-stemcell-builder/config"

type Config struct {
	config.AmiRegion
	config.AmiConfiguration
}

type MachineImageConfig struct {
	LocalPath    string
	FileFormat   string
	VolumeSizeGB int64
}

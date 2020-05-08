package cmd

import (
	"fmt"
	"os"
	"strings"

	gofigure "github.com/NCAR/go-figure"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

func ConfigurationError(err error) {
	fmt.Println(err)
	pflag.Usage()
	os.Exit(1)
}

func Load() (*viper.Viper, error) {
	v := viper.New()
	configFiles := pflag.CommandLine.StringSlice("config", []string{"./dqd.yaml"}, "Load a DQD config file")
	configDirs := pflag.CommandLine.StringSlice("configDir", []string{"/etc/dqd", "/dqd/config"}, "Lookup for config files in these folders")
	configOverrides := pflag.CommandLine.StringSlice("set", []string{}, "Override specific configuration keys")
	pflag.Parse()
	v.SetConfigType("yaml")
	err := gofigure.Parse(v, *configDirs)
	if err != nil {
		panic(fmt.Errorf("Fatal error config file: %s \n", err))
	}
	for _, f := range *configFiles {
		r, err := os.Open(f)
		defer r.Close()
		if err == nil {
			v.MergeConfig(r)
		} else {
			if f != "./dqd.yaml" {
				return nil, err
			}
		}

	}

	for _, override := range *configOverrides {
		entry := strings.Split(override, "=")
		if len(entry) != 2 {
			return nil, fmt.Errorf("invalid set value '%v'", override)
		}
		v.Set(entry[0], entry[1])
	}

	return v, nil
}

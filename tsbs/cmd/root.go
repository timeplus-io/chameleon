package cmd

import (
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	homedir "github.com/mitchellh/go-homedir"
	"github.com/spf13/viper"

	"github.com/timeplus-io/chameleon/tsbs/server"

	"github.com/timeplus-io/chameleon/tsbs/config"
)

var cfgFile string

var rootCmd = &cobra.Command{
	Use:   "tsbs",
	Short: "tsbs stream data",
	Long:  ``,
	RunE:  server.Run,
}

func Execute() {
	config.Conf.ApplyToCobra(rootCmd)

	for arg := range config.Conf {
		viper.BindPFlag(arg, rootCmd.Flags().Lookup(arg))
	}

	viper.WriteConfig()

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.tsbs.yaml)")

}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// Search config in home directory with name ".generator" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigName(".generator")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		log.Infof("using config file:%s", viper.ConfigFileUsed())
	}
}

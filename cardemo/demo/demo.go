package demo

import (
	"github.com/spf13/cobra"
	"github.com/timeplus-io/chameleon/cardemo/log"
)

func Run(_ *cobra.Command, _ []string) error {
	if app, err := NewCarSharingDemoApp(); err != nil {
		log.Logger().WithError(err).Error("failed to initialize demo app")
		return err
	} else {
		app.Start()
	}

	return nil
}

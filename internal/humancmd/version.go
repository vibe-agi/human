package humancmd

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/spf13/cobra"
	"github.com/vibe-agi/human/internal/buildinfo"
)

func newVersionCommand() *cobra.Command {
	var outputJSON bool
	command := &cobra.Command{
		Use:   "version",
		Short: "print build and runtime version information",
		Args:  cobra.NoArgs,
		// Version reporting must remain available even when a local Human
		// configuration file is malformed. Defining the closest persistent hook
		// keeps Cobra from invoking the root command's configuration loader.
		PersistentPreRunE: func(*cobra.Command, []string) error { return nil },
		RunE: func(command *cobra.Command, _ []string) error {
			info := buildinfo.Current()
			if outputJSON {
				encoder := json.NewEncoder(command.OutOrStdout())
				encoder.SetEscapeHTML(false)
				return encoder.Encode(info)
			}
			return writeHumanVersion(command.OutOrStdout(), info)
		},
	}
	command.Flags().BoolVar(&outputJSON, "json", false, "print stable machine-readable JSON")
	return command
}

func writeHumanVersion(writer io.Writer, info buildinfo.Info) error {
	_, err := fmt.Fprintf(
		writer,
		"human %s\ncommit: %s\nbuilt: %s\nruntime: %s\nplatform: %s/%s\n",
		info.Version,
		info.Commit,
		info.BuildDate,
		info.GoVersion,
		info.OS,
		info.Arch,
	)
	return err
}

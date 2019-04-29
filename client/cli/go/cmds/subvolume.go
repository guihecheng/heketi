package cmds

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"text/template"

	"github.com/heketi/heketi/pkg/glusterfs/api"
	"github.com/spf13/cobra"
)

var (
	sv_size int
	sv_name string
	vol_id  string
)

func init() {
	RootCmd.AddCommand(subvolumeCommand)
	subvolumeCommand.AddCommand(subvolumeCreateCommand)
	subvolumeCommand.AddCommand(subvolumeDeleteCommand)
	subvolumeCommand.AddCommand(subvolumeInfoCommand)
	subvolumeCommand.AddCommand(subvolumeListCommand)

	subvolumeCreateCommand.Flags().IntVar(&sv_size, "size", 0,
		"\n\tSize of subvolume in GiB")
	subvolumeCreateCommand.Flags().StringVar(&vol_id, "volume", "",
		"\n\tId of volume where subvolumes resides.")
	subvolumeCreateCommand.Flags().StringVar(&sv_name, "name", "",
		"\n\tOptional: Name of subvolume. Only set if really necessary")

	subvolumeCreateCommand.SilenceUsage = true
	subvolumeDeleteCommand.SilenceUsage = true
	subvolumeInfoCommand.SilenceUsage = true
	subvolumeListCommand.SilenceUsage = true
}

var subvolumeCommand = &cobra.Command{
	Use:   "subvolume",
	Short: "Heketi Subvolume Management",
	Long:  "Heketi Subvolume Management",
}

var subvolumeCreateCommand = &cobra.Command{
	Use:   "create",
	Short: "Create a subvolume under a GlusterFS volume",
	Long:  "Create a subvolume under a GlusterFS volume",
	Example: `  * Create a 100GiB subvolume under a volume:
      $ heketi-cli subvolume create --size=100 --volume=subvolume-backend-id`,
	RunE: func(cmd *cobra.Command, args []string) error {
		// Check subvolume size
		if sv_size == 0 {
			return errors.New("Missing subvolume size")
		}

		// Create request blob
		req := &api.SubvolumeCreateRequest{}
		req.Size = sv_size
		req.VolumeId = vol_id

		if sv_name != "" {
			req.Name = sv_name
		}

		// Create a client
		heketi, err := newHeketiClient()
		if err != nil {
			return err
		}

		// Add subvolume
		subvolume, err := heketi.SubvolumeCreate(req)
		if err != nil {
			return err
		}

		if options.Json {
			data, err := json.Marshal(subvolume)
			if err != nil {
				return err
			}
			fmt.Fprintf(stdout, string(data))
		} else {
			printSubvolumeInfo(subvolume)
		}

		return nil
	},
}

var subvolumeDeleteCommand = &cobra.Command{
	Use:     "delete",
	Short:   "Deletes the subvolume",
	Long:    "Deletes the subvolume",
	Example: "  $ heketi-cli subvolume delete subvolume-id",
	RunE: func(cmd *cobra.Command, args []string) error {
		s := cmd.Flags().Args()

		//ensure proper number of args
		if len(s) < 1 {
			return errors.New("Subvolume id missing")
		}

		//set subvolumeId
		subvolumeId := cmd.Flags().Arg(0)

		// Create a client
		heketi, err := newHeketiClient()
		if err != nil {
			return err
		}

		//set url
		err = heketi.SubvolumeDelete(subvolumeId)
		if err == nil {
			fmt.Fprintf(stdout, "Subvolume %v deleted\n", subvolumeId)
		}

		return err
	},
}

var subvolumeInfoCommand = &cobra.Command{
	Use:     "info",
	Short:   "Retrieves information about the subvolume",
	Long:    "Retrieves information about the subvolume",
	Example: "  $ heketi-cli volume info subvolume-id",
	RunE: func(cmd *cobra.Command, args []string) error {
		//ensure proper number of args
		s := cmd.Flags().Args()
		if len(s) < 1 {
			return errors.New("Subvolume id missing")
		}

		// Set subvolume id
		subvolumeId := cmd.Flags().Arg(0)

		// Create a client to talk to Heketi
		heketi, err := newHeketiClient()
		if err != nil {
			return err
		}

		info, err := heketi.SubvolumeInfo(subvolumeId)
		if err != nil {
			return err
		}

		if options.Json {
			data, err := json.Marshal(info)
			if err != nil {
				return err
			}
			fmt.Fprintf(stdout, string(data))
		} else {
			printSubvolumeInfo(info)
		}
		return nil

	},
}

var subvolumeListCommand = &cobra.Command{
	Use:     "list",
	Short:   "Lists the subvolumes managed by Heketi",
	Long:    "Lists the subvolumes managed by Heketi",
	Example: "  $ heketi-cli subvolume list",
	RunE: func(cmd *cobra.Command, args []string) error {
		// Create a client
		heketi, err := newHeketiClient()
		if err != nil {
			return err
		}

		// List subvolumes
		list, err := heketi.SubvolumeList()
		if err != nil {
			return err
		}

		if options.Json {
			data, err := json.Marshal(list)
			if err != nil {
				return err
			}
			fmt.Fprintf(stdout, string(data))
		} else {
			for _, id := range list.Subvolumes {
				subvolume, err := heketi.SubvolumeInfo(id)
				if err != nil {
					return err
				}

				fmt.Fprintf(stdout, "Id:%-35v Volume:%-35v Name:%v%v\n",
					id,
					subvolume.VolumeId,
					subvolume.Name)
			}
		}

		return nil
	},
}

var subvolumeTemplate = `
{{- /* remove whitespace */ -}}
Name: {{.Name}}
Size: {{.Size}}
Subvolume Id: {{.Id}}
Volume Id: {{.VolumeId}}
{{end}}
`

func printSubvolumeInfo(subvolume *api.SubvolumeInfoResponse) {
	t, err := template.New("subvolume").Parse(subvolumeTemplate)
	if err != nil {
		panic(err)
	}
	err = t.Execute(os.Stdout, subvolume)
	if err != nil {
		panic(err)
	}
}

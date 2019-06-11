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
	dv_size    int
	dv_name    string
	cluster_id string
)

func init() {
	RootCmd.AddCommand(dirvolumeCommand)
	dirvolumeCommand.AddCommand(dirvolumeCreateCommand)
	dirvolumeCommand.AddCommand(dirvolumeDeleteCommand)
	dirvolumeCommand.AddCommand(dirvolumeInfoCommand)
	dirvolumeCommand.AddCommand(dirvolumeListCommand)

	dirvolumeCreateCommand.Flags().IntVar(&dv_size, "size", 0,
		"\n\tSize of dirvolume in GiB")
	dirvolumeCreateCommand.Flags().StringVar(&cluster_id, "cluster", "",
		"\n\tId of cluster where dirvolumes resides.")
	dirvolumeCreateCommand.Flags().StringVar(&dv_name, "name", "",
		"\n\tOptional: Name of dirvolume. Only set if really necessary")

	dirvolumeCreateCommand.SilenceUsage = true
	dirvolumeDeleteCommand.SilenceUsage = true
	dirvolumeInfoCommand.SilenceUsage = true
	dirvolumeListCommand.SilenceUsage = true
}

var dirvolumeCommand = &cobra.Command{
	Use:   "dirvolume",
	Short: "Heketi Dirvolume Management",
	Long:  "Heketi Dirvolume Management",
}

var dirvolumeCreateCommand = &cobra.Command{
	Use:   "create",
	Short: "Create a dirvolume under a GlusterFS volume",
	Long:  "Create a dirvolume under a GlusterFS volume",
	Example: `  * Create a 100GiB dirvolume under a volume:
      $ heketi-cli dirvolume create --size=100 --cluster=dirvolume-backend-cluster-id`,
	RunE: func(cmd *cobra.Command, args []string) error {
		// Check dirvolume size
		if dv_size == 0 {
			return errors.New("Missing dirvolume size")
		}

		// Create request blob
		req := &api.DirvolumeCreateRequest{}
		req.Size = dv_size
		req.ClusterId = cluster_id

		if dv_name != "" {
			req.Name = dv_name
		}

		// Create a client
		heketi, err := newHeketiClient()
		if err != nil {
			return err
		}

		// Add dirvolume
		dirvolume, err := heketi.DirvolumeCreate(req)
		if err != nil {
			return err
		}

		if options.Json {
			data, err := json.Marshal(dirvolume)
			if err != nil {
				return err
			}
			fmt.Fprintf(stdout, string(data))
		} else {
			printDirvolumeInfo(dirvolume)
		}

		return nil
	},
}

var dirvolumeDeleteCommand = &cobra.Command{
	Use:     "delete",
	Short:   "Deletes the dirvolume",
	Long:    "Deletes the dirvolume",
	Example: "  $ heketi-cli dirvolume delete dirvolume-id",
	RunE: func(cmd *cobra.Command, args []string) error {
		s := cmd.Flags().Args()

		//ensure proper number of args
		if len(s) < 1 {
			return errors.New("Dirvolume id missing")
		}

		//set dirvolumeId
		dirvolumeId := cmd.Flags().Arg(0)

		// Create a client
		heketi, err := newHeketiClient()
		if err != nil {
			return err
		}

		//set url
		err = heketi.DirvolumeDelete(dirvolumeId)
		if err == nil {
			fmt.Fprintf(stdout, "Dirvolume %v deleted\n", dirvolumeId)
		}

		return err
	},
}

var dirvolumeInfoCommand = &cobra.Command{
	Use:     "info",
	Short:   "Retrieves information about the dirvolume",
	Long:    "Retrieves information about the dirvolume",
	Example: "  $ heketi-cli volume info dirvolume-id",
	RunE: func(cmd *cobra.Command, args []string) error {
		//ensure proper number of args
		s := cmd.Flags().Args()
		if len(s) < 1 {
			return errors.New("Dirvolume id missing")
		}

		// Set dirvolume id
		dirvolumeId := cmd.Flags().Arg(0)

		// Create a client to talk to Heketi
		heketi, err := newHeketiClient()
		if err != nil {
			return err
		}

		info, err := heketi.DirvolumeInfo(dirvolumeId)
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
			printDirvolumeInfo(info)
		}
		return nil

	},
}

var dirvolumeListCommand = &cobra.Command{
	Use:     "list",
	Short:   "Lists the dirvolumes managed by Heketi",
	Long:    "Lists the dirvolumes managed by Heketi",
	Example: "  $ heketi-cli dirvolume list",
	RunE: func(cmd *cobra.Command, args []string) error {
		// Create a client
		heketi, err := newHeketiClient()
		if err != nil {
			return err
		}

		// List dirvolumes
		list, err := heketi.DirvolumeList()
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
			for _, id := range list.Dirvolumes {
				dirvolume, err := heketi.DirvolumeInfo(id)
				if err != nil {
					return err
				}

				fmt.Fprintf(stdout, "Id:%-35v Cluster:%-35v Name:%v\n",
					id,
					dirvolume.ClusterId,
					dirvolume.Name)
			}
		}

		return nil
	},
}

var dirvolumeTemplate = `
{{- /* remove whitespace */ -}}
Name: {{.Name}}
Size: {{.Size}}
Dirvolume Id: {{.Id}}
Cluster Id: {{.ClusterId}}
`

func printDirvolumeInfo(dirvolume *api.DirvolumeInfoResponse) {
	t, err := template.New("dirvolume").Parse(dirvolumeTemplate)
	if err != nil {
		panic(err)
	}
	err = t.Execute(os.Stdout, dirvolume)
	if err != nil {
		panic(err)
	}
}

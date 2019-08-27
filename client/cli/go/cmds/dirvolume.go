package cmds

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"text/template"

	"github.com/heketi/heketi/pkg/glusterfs/api"
	"github.com/spf13/cobra"
)

var (
	dv_size        int
	dv_name        string
	cluster_id     string
	dv_expand_size int
	dv_id          string
	iplist         string
)

func init() {
	RootCmd.AddCommand(dirvolumeCommand)
	dirvolumeCommand.AddCommand(dirvolumeCreateCommand)
	dirvolumeCommand.AddCommand(dirvolumeDeleteCommand)
	dirvolumeCommand.AddCommand(dirvolumeInfoCommand)
	dirvolumeCommand.AddCommand(dirvolumeListCommand)
	dirvolumeCommand.AddCommand(dirvolumeExpandCommand)
	dirvolumeCommand.AddCommand(dirvolumeExportCommand)
	dirvolumeCommand.AddCommand(dirvolumeUnexportCommand)

	dirvolumeCreateCommand.Flags().IntVar(&dv_size, "size", 0,
		"\n\tSize of dirvolume in GiB")
	dirvolumeCreateCommand.Flags().StringVar(&cluster_id, "cluster", "",
		"\n\tId of cluster where dirvolumes resides.")
	dirvolumeCreateCommand.Flags().StringVar(&dv_name, "name", "",
		"\n\tOptional: Name of dirvolume. Only set if really necessary")

	dirvolumeExpandCommand.Flags().IntVar(&dv_expand_size, "expand-size", 0,
		"\n\tAmount in GiB to add to the dirvolume")
	dirvolumeExpandCommand.Flags().StringVar(&dv_id, "dirvolume", "",
		"\n\tId of dirvolume to expand")

	dirvolumeExportCommand.Flags().StringVar(&iplist, "iplist", "",
		"\n\twhite list IPs that should have access")
	dirvolumeExportCommand.Flags().StringVar(&dv_id, "dirvolume", "",
		"\n\tId of dirvolume to export")

	dirvolumeUnexportCommand.Flags().StringVar(&iplist, "iplist", "",
		"\n\tIPs to kick out from white list")
	dirvolumeUnexportCommand.Flags().StringVar(&dv_id, "dirvolume", "",
		"\n\tId of dirvolume to unexport")

	dirvolumeCreateCommand.SilenceUsage = true
	dirvolumeDeleteCommand.SilenceUsage = true
	dirvolumeExpandCommand.SilenceUsage = true
	dirvolumeInfoCommand.SilenceUsage = true
	dirvolumeListCommand.SilenceUsage = true
	dirvolumeExportCommand.SilenceUsage = true
	dirvolumeUnexportCommand.SilenceUsage = true
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
Export IPs: {{.Export.IpList}}
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

var dirvolumeExpandCommand = &cobra.Command{
	Use:   "expand",
	Short: "Expand a dirvolume",
	Long:  "Expand a dirvolume",
	Example: `  * Add 10GiB to a dirvolume
    $ heketi-cli dirvolume expand --dirvolume=60d46d518074b13a04ce1022c8c7193c --expand-size=10
`,
	RunE: func(cmd *cobra.Command, args []string) error {
		// Check volume size
		if dv_expand_size == 0 {
			return errors.New("Missing dirvolume amount to expand")
		}

		if dv_id == "" {
			return errors.New("Missing dirvolume id")
		}

		// Create request
		req := &api.DirvolumeExpandRequest{}
		req.Size = dv_expand_size

		// Create client
		heketi, err := newHeketiClient()
		if err != nil {
			return err
		}

		// Expand dirvolume
		dirvolume, err := heketi.DirvolumeExpand(dv_id, req)
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

var dirvolumeExportCommand = &cobra.Command{
	Use:   "export",
	Short: "Export a dirvolume",
	Long:  "Export a dirvolume",
	Example: `  * Allow dirvolume access to IPs: 10.0.0.1,10.0.0.2
    $ heketi-cli dirvolume export --dirvolume=60d46d518074b13a04ce1022c8c7193c --iplist=10.0.0.1,10.0.0.2
`,
	RunE: func(cmd *cobra.Command, args []string) error {
		// Check empty list
		if len(iplist) == 0 {
			return errors.New("Missing IP(s) to allow access")
		}

		if dv_id == "" {
			return errors.New("Missing dirvolume id")
		}

		// Create request
		req := &api.DirvolumeExportRequest{}
		req.IpList = strings.Split(iplist, ",")

		// Create client
		heketi, err := newHeketiClient()
		if err != nil {
			return err
		}

		// Export dirvolume
		dirvolume, err := heketi.DirvolumeExport(dv_id, req)
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

var dirvolumeUnexportCommand = &cobra.Command{
	Use:   "unexport",
	Short: "Unexport a dirvolume",
	Long:  "Unexport a dirvolume",
	Example: `  * Deny dirvolume access to IPs: 10.0.0.1,10.0.0.2
    $ heketi-cli dirvolume unexport --dirvolume=60d46d518074b13a04ce1022c8c7193c --iplist=10.0.0.1,10.0.0.2
`,
	RunE: func(cmd *cobra.Command, args []string) error {
		// Check empty list
		if len(iplist) == 0 {
			return errors.New("Missing IP(s) to deny access")
		}

		if dv_id == "" {
			return errors.New("Missing dirvolume id")
		}

		// Create request
		req := &api.DirvolumeExportRequest{}
		req.IpList = strings.Split(iplist, ",")

		// Create client
		heketi, err := newHeketiClient()
		if err != nil {
			return err
		}

		// Unexport dirvolume
		dirvolume, err := heketi.DirvolumeUnexport(dv_id, req)
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

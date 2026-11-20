module github.com/aperturerobotics/bbolt

go 1.26.4

require (
	github.com/aperturerobotics/fsnotify v1.9.1-0.20261120090407-765f22ccaa33
	github.com/spf13/cobra v1.10.2
	github.com/spf13/pflag v1.1.0
	github.com/stretchr/testify v1.12.1
	go.etcd.io/gofail v0.2.0
	golang.org/x/sync v0.25.0
	golang.org/x/sys v0.50.0
)

require (
	github.com/aclements/go-moremath v0.0.0-20210112150236-f10218a38794 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	golang.org/x/mod v0.43.0 // indirect
	golang.org/x/perf v0.0.0-20261117185222-4febdb082b2c // indirect
	golang.org/x/telemetry v0.0.0-20261116191925-e77a43154acb // indirect
	golang.org/x/tools v0.53.0 // indirect
)

tool (
	go.etcd.io/gofail
	golang.org/x/perf/cmd/benchstat
	golang.org/x/tools/cmd/goimports
)

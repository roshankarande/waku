module github.com/roshankarande/waku/apps/desktop-go

go 1.27.1

require (
	github.com/egoist/mygo v0.0.0
	github.com/ncruces/go-sqlite3 v0.35.6
)

require (
	github.com/ebitengine/purego v0.11.1 // indirect
	github.com/go-text/typesetting v0.3.5 // indirect
	github.com/ncruces/go-sqlite3-wasm/v6 v6.3.35304 // indirect
	github.com/ncruces/julianday v1.0.0 // indirect
	golang.org/x/image v0.46.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
)

replace github.com/egoist/mygo => github.com/roshankarande/mygo v0.0.0-20261004175020-50be73e3d1ba

// Canonical mirrors avoid vanity-host discovery in restricted builders.
replace golang.org/x/sys => github.com/golang/sys v0.48.0

replace golang.org/x/text => github.com/golang/text v0.42.0

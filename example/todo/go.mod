module github.com/wotek/flux/example/todo

go 1.27.1

require (
	github.com/wotek/flux v0.0.0
	golang.org/x/sync v0.23.0
)

require github.com/google/uuid v1.6.0 // indirect

replace github.com/wotek/flux => ../..

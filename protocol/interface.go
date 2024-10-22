package protocol

type Driver interface {
	Config() any
	Setup() error
	Read()
}

package types

func ToPointer[T any](val T) *T {
	return &val
}

const NullStruct = "github.com/datazip-inc/olake/types/Null"

func Keys[T comparable](v map[T]any) []T {
	setArray := []T{}
	for key := range v {
		setArray = append(setArray, key)
	}

	return setArray
}

func Channel[T any](arr []T, buffer int64) <-chan T {
	var channel chan T
	if buffer > 0 {
		channel = make(chan T, buffer)
	} else {
		channel = make(chan T)
	}

	func() {
		defer func() {
			close(channel)
		}()

		for _, elem := range arr {
			channel <- elem
		}
	}()

	return channel
}

//go:build windows
// +build windows

package msnet

func GetMaxOpenFiles() int {
	return 1024 * 1024 * 2
}

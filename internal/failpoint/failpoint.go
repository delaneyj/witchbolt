//go:build !failpoint

package failpoint

// Enable is a no-op when failpoint build tag is not set.
func Enable(name string, action string) error {
	return nil
}

// Disable is a no-op when failpoint build tag is not set.
func Disable(name string) error {
	return nil
}

// Inject is a no-op when failpoint build tag is not set.
func Inject(name string) (string, bool) {
	return "", false
}

// InjectStruct is a no-op when failpoint build tag is not set.
func InjectStruct(name string) {}

package wallet

// Transfer service errors.
const (
	ErrTransferNotFound = Error("transfer not found")
	ErrTransferExists   = Error("transfer already exists")
)

// Error defines errors which are relevant to all wallet services.
type Error string

func (e Error) Error() string {
	return string(e)
}

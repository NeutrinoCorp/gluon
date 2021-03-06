package gluon

type Error struct {
	kind        string
	description string
	parentErr   error
}

var _ error = Error{}

func NewError(kind, description string, parent error) Error {
	return Error{
		kind:        kind,
		description: "gluon: " + description,
		parentErr:   parent,
	}
}

func (e Error) Error() string {
	return e.Description()
}

func (e Error) Kind() string {
	return e.kind
}

func (e Error) Description() string {
	return e.description
}

func (e Error) Parent() error {
	return e.parentErr
}

func (e Error) ParentDescription() string {
	if e.parentErr == nil {
		return ""
	}
	return e.parentErr.Error()
}

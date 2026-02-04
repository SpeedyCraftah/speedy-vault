package constants

type ObjectOperationFlags uint8
const (
	ObjectCreate ObjectOperationFlags = 1 << iota // Allows the creation of a complete object (aka single part), but doesn't allow modification afterwards.
	ObjectUpdate                                  // Allows updating/replacing an existing object, but doesn't allow creating one.
	ObjectDelete                                  // Allows deleting an existing object, but doesn't allow read or write access.
	ObjectRead                                    // Allows read access to an object.

	ObjectFlagBoundary_ 						  // Placeholder for determining the end of the flags enum.
)

// All permissions enabled.
const ObjectOperationFlagsAll = ObjectFlagBoundary_ - 1;

// Determines whether the flags specified have access to all of the specified features.
func (flags ObjectOperationFlags) HasRequired(requiredFlags ObjectOperationFlags) bool {
	return (requiredFlags & flags) != requiredFlags;
}

// Determines whether the flags have access to any of the specified features.
func (flags ObjectOperationFlags) HasAny(anyFlags ObjectOperationFlags) bool {
	return (anyFlags & flags) != 0;
}
package constants

type BucketAccessRuleAction uint8

const (
	AllowPublic BucketAccessRuleAction = iota // Allow access to everyone, no matter if the URL is signed or not (effectively public access).
	AllowSigned                               // Allow access only if the URL is signed and the signature is valid and hasn't expired (discretionary access), this is also the default behaviour if no rule is matched.
	DenyAll                                   // Blocks access outright regardless of if the URL is signed or not, you can also use it to override the default AllowSigned rule and block all access by placing it at the end matching all keys.
)

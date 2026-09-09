package synctree

import "os"

func fileIdentity(info os.FileInfo) string     { return "" }
func ownedByCurrentUser(info os.FileInfo) bool { return false }

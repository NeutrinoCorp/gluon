package gutil

// ReverseString returns its argument string reversed rune-wise left to right.
//
// Implementation took from: https://github.com/golang/example/blob/master/stringutil/reverse.go
func ReverseString(s string) string {
	r := []rune(s)
	for i, j := 0, len(r)-1; i < len(r)/2; i, j = i+1, j-1 {
		r[i], r[j] = r[j], r[i]
	}
	return string(r)
}

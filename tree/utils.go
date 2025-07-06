package tree

func insertAt[T any](slice []T, index int, elem T) []T {
	// make a new slice of exactly the right length
	newSlice := make([]T, len(slice)+1)

	// copy the left part
	copy(newSlice, slice[:index])

	// insert the new element
	newSlice[index] = elem

	// copy the right part
	copy(newSlice[index+1:], slice[index:])

	return newSlice
}

func UpperBound(val int, values []Key) int {
	n := len(values)
	low, high := 0, n-1
	ind := n
	var mid int
	for low <= high {
		mid = (high-low)/2 + low
		if val < values[mid].Value {
			ind = mid
			high = mid - 1
		} else {
			low = mid + 1
		}
	}
	return ind
}

func BinarySearch(val int, values []LeafPair) int {
	n := len(values)
	low, high := 0, n-1
	ind := -1
	var mid int
	for low <= high {
		mid = (high-low)/2 + low
		if val < values[mid].K.Value {
			high = mid - 1
		} else if val == values[mid].K.Value {
			ind = mid
			return ind
		} else {
			low = mid + 1
		}
	}
	return ind
}

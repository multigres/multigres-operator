/*
Copyright 2026 Numtide.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package names generates deterministic, unique names for Kubernetes objects.
//
// Strategy:
//  1. For dynamic, user-defined, or nested resources (e.g., Cells, TableGroups, Shards),
//     we use JoinWithConstraints to append a safety hash. This prevents collisions when
//     strings are truncated to fit Kubernetes length limits (e.g., 63 chars for labels).
//  2. For singleton or static resources (e.g., GlobalTopo, Multiadmin) that are 1:1 with
//     the cluster and have predictable short names, we use simple string concatenation
//     without hashing for better readability and predictability.
package name

import (
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"strings"
	"unicode"
)

const (
	// hashBytes is the number of bytes included in the result of Hash().
	// This must never be changed since it would break backwards compatibility.
	hashBytes = 4

	// hashLength is the number of characters in the hex-encoded string returned from Hash().
	hashLength = 2 * hashBytes

	// truncationMark is a special separator used when appending the hash to a
	// truncated name to indicate that truncation occurred.
	truncationMark = "---"

	// minTruncatedLength is the shortest possible length of a name that had to
	// be truncated. There has to be at least one character in front of the
	// truncationMark since names can't start with '-', then the truncationMark
	// itself, and finally the hash.
	minTruncatedLength = 1 + len(truncationMark) + hashLength
)

// Constraints specifies rules that the output of JoinWithConstraints must follow.
type Constraints struct {
	// MaxLength is the maximum length of the output, to be enforced after any
	// transformations and including the hash suffix. If a name has to be
	// truncated to fit within this maximum length, the hash at the end will be
	// preceded by a special truncation mark: "---" rather than the usual "-".
	//
	// MaxLength must be at least 12 because that's the shortest possible
	// truncated value (1 char + truncation mark + hash). Passing a value less
	// than 12 will result in a panic.
	MaxLength int
	// ValidFirstChar is a function that returns whether the given rune is
	// allowed as the first character in the output.
	ValidFirstChar func(r rune) bool
}

var (
	// DefaultConstraints are the name constraints for objects in Kubernetes
	// that don't have any special rules.
	DefaultConstraints = Constraints{
		MaxLength:      253,
		ValidFirstChar: isLowercaseAlphanumeric,
	}
	// ServiceConstraints are name constraints for Service objects.
	ServiceConstraints = Constraints{
		MaxLength:      63,
		ValidFirstChar: isLowercaseLetter,
	}
	// StatefulSetConstraints are name constraints for StatefulSet objects.
	// We need to account for the suffix appended to the Pod name, e.g. "-0",
	// as well as the controller-revision-hash label which appends a hash.
	// To be safe, we reserve 11 characters for the suffix/hash.
	// 63 - 11 = 52.
	StatefulSetConstraints = Constraints{
		MaxLength:      52,
		ValidFirstChar: isLowercaseLetter,
	}
	// PodConstraints are name constraints for directly-managed Pod objects.
	// Pods are DNS labels (max 63 chars). We reserve 3 characters for the
	// "-{index}" suffix (supports indices 0-99, matching ReplicasPerCell max).
	// 63 - 3 = 60.
	PodConstraints = Constraints{
		MaxLength:      60,
		ValidFirstChar: isLowercaseLetter,
	}
)

// Hash computes a hash suffix for the given name parts.
func Hash(parts []string) string {
	h := fnv.New32a()
	for _, part := range parts {
		h.Write([]byte(part)) //nolint:gosec // hash.Write never returns an error
		// It doesn't matter if the parts have nulls in them somehow.
		// The important thing is that this separator is not the same as '-'.
		// To collide, both the "hyphen-joined-string" and the hash must match,
		// but you can't mimic two different separators at the same time.
		h.Write([]byte{0}) //nolint:gosec // hash.Write never returns an error
	}
	sum := h.Sum(nil)
	// FNV-1a 32-bit produces exactly 4 bytes, which hex-encodes to 8 characters.
	// We only care about avoiding collisions in the case when
	// the concatenated parts without the hash match exactly.
	// That leaves almost no degrees of freedom even if you're
	// trying to collide on purpose.
	return hex.EncodeToString(sum)
}

// JoinWithConstraints builds a name by concatenating a number of parts with '-' as
// the separator, and then enforcing some constraints on the resulting name while
// maintaining uniqueness and determinism with respect to the input values.
//
// It will append a hash at the end that depends only on the parts supplied.
// If the function is called again with the same parts, in the same order,
// the hash will also be the same. This determinism allows you to use the resulting
// name to ensure idempotency when creating objects.
//
// However, the hash will differ if the parts are rearranged, or if substrings
// within parts are moved to adjacent parts. The resulting generated name,
// while deterministic, is thus guaranteed to be unique for a given list of parts,
// even if the parts themselves are allowed to contain the separator.
//
// For example: JoinWithConstraints(cons, "a-b", "c") != JoinWithConstraints(cons, "a", "b-c")
// Although both will begin with "a-b-c-", the hash at the end will be different.
//
// The constraints passed in should be appropriate for the kind of object
// (e.g. Pod, Service) whose name is being generated, to ensure the name is
// accepted by Kubernetes validation. Most objects in Kubernetes accept any name
// that conforms to the DefaultConstraints, with the notable exception of Service
// objects which must conform to ServiceConstraints. Custom constraints, such as
// for a CRD that adds its own naming requirements, can be expressed by defining a
// new Constraints object.
func JoinWithConstraints(cons Constraints, parts ...string) string {
	// Always panic immediately if specified Constraints are invalid so we
	// notice the programming error even if the inputs don't happen to trigger
	// the constraints.
	if cons.MaxLength < minTruncatedLength {
		panic(
			fmt.Sprintf(
				"MaxLength of %v is invalid; must be at least %v",
				cons.MaxLength,
				minTruncatedLength,
			),
		)
	}

	if len(parts) == 0 {
		return ""
	}

	// Generate the hash suffix with the original input values so the name will
	// be unique regardless of any transformation or truncation we may have done
	// on the rest of the name.
	hash := Hash(parts)

	// Transform the input parts to ensure they meet the constraints.
	newParts := make([]string, 0, len(parts)+1)
	transform := func(r rune) rune {
		if isLowercaseAlphanumeric(r) || r == '-' {
			return r
		}
		if isUppercaseLetter(r) {
			return unicode.ToLower(r)
		}
		return '-'
	}
	for _, part := range parts {
		newParts = append(newParts, strings.Map(transform, part))
	}

	// From here on, we can assume the strings in newParts contain only ASCII,
	// which simplifies offset-based access.

	// Check if we need to add a prefix to make sure the first character is valid.
	firstPart := newParts[0]
	if len(firstPart) == 0 || !cons.ValidFirstChar(rune(firstPart[0])) {
		newParts[0] = "x" + firstPart
	}

	// If the predicted length is ok, we just need to append the hash.
	partialResult := strings.Join(newParts, "-")
	predictedLength := len(partialResult) + 1 + len(hash)
	if predictedLength <= cons.MaxLength {
		return partialResult + "-" + hash
	}

	// Otherwise, we need to truncate the partial result before appending the
	// hash to ensure the full hash fits. We need to cut off enough to get back
	// to MaxLength, and then a little extra to make room for the
	// triple-separator mark we use to indicate that the name was truncated.
	cutLength := predictedLength - cons.MaxLength + 2
	partialResult = partialResult[:len(partialResult)-cutLength]
	return partialResult + truncationMark + hash
}

func isLowercaseLetter(r rune) bool {
	return r >= 'a' && r <= 'z'
}

func isUppercaseLetter(r rune) bool {
	return r >= 'A' && r <= 'Z'
}

func isDigit(r rune) bool {
	return r >= '0' && r <= '9'
}

func isLowercaseAlphanumeric(r rune) bool {
	return isLowercaseLetter(r) || isDigit(r)
}

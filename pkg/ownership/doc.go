// Package ownership is the single source of truth for which field manager
// writes which status field, and when that field is expected to change. The
// table lives in Fields; OwnerOf looks a field path up in it.
//
// Every row has one of three classes:
//
//   - Observation: reconstructible from the world; survives a status wipe
//     unchanged.
//   - Gate: the table entry names the reader and what unblocks it when the
//     input is degraded, unreadable or stale; a gate whose input is fed by the
//     waiter is a defect.
//   - WorkflowMemory: exactly one writer; written by an owned SSA field or
//     under optimistic lock; declared, because wiping it changes behaviour.
//
// The operator suite (test/suite) fails any case whose objects disagree with
// the table.
package ownership

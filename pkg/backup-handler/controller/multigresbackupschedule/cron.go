package multigresbackupschedule

import (
	"fmt"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
)

// parseSchedule parses a standard 5-field cron expression evaluated in UTC.
// Time-zone prefixes are rejected: schedules are always UTC.
func parseSchedule(spec string) (cron.Schedule, error) {
	trimmed := strings.TrimSpace(spec)
	if strings.HasPrefix(trimmed, "TZ=") || strings.HasPrefix(trimmed, "CRON_TZ=") {
		return nil, fmt.Errorf("schedule %q: time zones are not supported; schedules are UTC", spec)
	}
	s, err := cron.ParseStandard(trimmed)
	if err != nil {
		return nil, fmt.Errorf("schedule %q: %w", spec, err)
	}
	return s, nil
}

// lookbackWindows bound the search for the most recent scheduled time, so a
// frequent schedule doesn't iterate over a long history.
var lookbackWindows = []time.Duration{
	time.Hour,
	24 * time.Hour,
	8 * 24 * time.Hour,
	32 * 24 * time.Hour,
	367 * 24 * time.Hour,
}

// mostRecent returns the latest scheduled time t with since < t <= now, or
// the zero time if there is none.
func mostRecent(s cron.Schedule, since, now time.Time) time.Time {
	since, now = since.UTC(), now.UTC()
	for _, w := range lookbackWindows {
		start := now.Add(-w)
		if start.Before(since) {
			start = since
		}
		var last time.Time
		for t := s.Next(start); !t.IsZero() && !t.After(now); t = s.Next(t) {
			last = t
		}
		if !last.IsZero() || !start.After(since) {
			return last
		}
	}
	return time.Time{}
}

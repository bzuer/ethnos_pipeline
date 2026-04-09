"""Shared constants for the load pipeline."""

# Record processing statuses
STATUS_INSERTED = "INSERTED"
STATUS_UPDATED = "UPDATED"
STATUS_NO_CHANGE = "NO_CHANGE"
STATUS_SKIPPED = "SKIPPED"
STATUS_ERROR = "ERROR"

# Organization types
ORG_TYPE_PUBLISHER = "PUBLISHER"
ORG_TYPE_INSTITUTE = "INSTITUTE"
ORG_TYPE_FUNDER = "FUNDER"

# Venue types
VENUE_TYPE_JOURNAL = "JOURNAL"
VENUE_TYPE_CONFERENCE = "CONFERENCE"
VENUE_TYPE_BOOK_SERIES = "BOOK_SERIES"
VENUE_TYPE_REPOSITORY = "REPOSITORY"
VENUE_TYPE_OTHER = "OTHER"

# License version ranking (monotonic upgrade: never downgrade)
LICENSE_VERSION_RANK = {
    "submittedversion": 1,
    "acceptedversion": 2,
    "publishedversion": 3,
}

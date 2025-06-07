# Setlist Analytics - Moose Project

A data analytics platform for ingesting and analyzing concert setlist data, built with [Moose](https://docs.fiveonefour.com/moose).

## Overview

This project enables you to:
- **Ingest setlist data** from various concert websites and sources
- **Store and process** concert data in ClickHouse for fast analytics
- **Query song statistics** like play counts, longest versions, and performance patterns
- **Track venue and tour data** across different bands and time periods
- **Analyze song transitions** and setlist patterns

Starting with **Goose** setlists from [El Goose](https://elgoose.net/setlists/), the platform is designed to be extensible to other bands and data sources.

## Data Models

### Show
Represents a concert/performance with metadata:
- Band name, date, venue information
- Tour details and show notes
- Verification status and source URL

### SetlistEntry  
Individual song performances within shows:
- Song name, set type (Set 1, Set 2, Encore), and position
- Duration, jam indicators, transitions
- Performance notes and guest musicians

## Quick Start

1. **Start the Moose development environment:**
   ```bash
   moose dev
   ```

2. **Ingest sample data:**
   ```bash
   python app/scripts/sample_data.py
   ```

3. **Query your data via the APIs:**
   - **Song Statistics**: `GET /consumption/song-stats`
   - **Show Information**: `GET /consumption/shows`  
   - **Setlist Entries**: `GET /consumption/setlist-entries`

## API Endpoints

### Ingestion APIs
- `POST /ingest/Show` - Add show/concert data
- `POST /ingest/SetlistEntry` - Add individual song performances

### Consumption APIs
- `GET /consumption/song-stats` - Song play counts, durations, jam counts
- `GET /consumption/shows` - Show information with filtering
- `GET /consumption/setlist-entries` - Individual song performances

### Example Queries

**Get most played songs:**
```bash
curl "http://localhost:4000/consumption/song-stats?limit=10"
```

**Get shows for a specific band:**
```bash
curl "http://localhost:4000/consumption/shows?band_name=Goose&limit=20"
```

**Get setlist for a specific show:**
```bash
curl "http://localhost:4000/consumption/setlist-entries?show_id=goose-2025-01-12-moon-palace"
```

## Analytics Views

The platform automatically creates materialized views for common analytics:

- **SongStats** - Aggregated song performance statistics
- **VenueStats** - Venue performance statistics  
- **DailyStats** - Daily setlist statistics by band
- **SongTransitions** - Song transition patterns

## Data Processing

- **Stream Processing**: Real-time enrichment of setlist data
- **Dead Letter Queues**: Error handling for failed ingestions
- **Data Validation**: Type-safe data models with automatic validation
- **Monitoring**: Console logging of ingestion and processing events

## Extending to Other Bands

The data models are designed to be band-agnostic. To add support for other bands:

1. Update your ingestion scripts to parse their setlist format
2. Use the same `Show` and `SetlistEntry` models
3. Set the `band_name` field appropriately
4. All analytics and queries will work across multiple bands

## Architecture

- **Backend**: Python with Moose framework
- **Database**: ClickHouse for OLAP analytics
- **Streaming**: Redpanda for real-time data processing
- **APIs**: Auto-generated REST APIs with type validation

## Development

The project uses hot reloading - changes to data models are automatically applied to your local infrastructure when you save files.

Monitor your data ingestion in the console, and use the built-in analytics APIs to explore your setlist data!

---

*Built with [Moose](https://docs.fiveonefour.com/moose) - the modern data stack for developers*

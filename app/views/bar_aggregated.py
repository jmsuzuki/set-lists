# Setlist Analytics Views  
# Creates materialized views for common setlist analytics queries

# TODO: Fix MaterializedView API usage once we understand the correct syntax
# from moose_lib import MaterializedView
# from app.ingest.models import setlist_entry_pipeline, show_pipeline


# Song performance statistics materialized view
# song_stats_mv = MaterializedView(
#     "SongStats",
#     """
#     SELECT 
#         song_name,
#         band_name,
#         COUNT(*) as total_plays,
#         ROUND(AVG(song_duration_minutes), 2) as avg_duration,
#         MAX(song_duration_minutes) as longest_version,
#         MIN(show_date) as first_played,
#         MAX(show_date) as last_played,
#         COUNT(CASE WHEN is_jam = true THEN 1 END) as jam_count,
#         COUNT(CASE WHEN is_tease = true THEN 1 END) as tease_count,
#         COUNT(DISTINCT show_date) as unique_shows
#     FROM {table: Identifier}
#     WHERE song_name != ''
#     GROUP BY song_name, band_name
#     """,
#     select_tables=[setlist_entry_pipeline.get_table()]
# )


# Venue statistics materialized view  
# venue_stats_mv = MaterializedView(
#     "VenueStats",
#     """
#     SELECT 
#         venue_name,
#         venue_city,
#         venue_state,
#         venue_country,
#         COUNT(*) as total_shows,
#         COUNT(DISTINCT band_name) as unique_bands,
#         MIN(show_date) as first_show,
#         MAX(show_date) as last_show
#     FROM {table: Identifier}
#     WHERE venue_name != ''
#     GROUP BY venue_name, venue_city, venue_state, venue_country
#     """,
#     select_tables=[show_pipeline.get_table()]
# )


# Daily setlist statistics materialized view
# daily_stats_mv = MaterializedView(
#     "DailyStats",
#     """
#     SELECT 
#         show_date,
#         band_name,
#         COUNT(*) as total_songs,
#         COUNT(DISTINCT show_id) as total_shows,
#         AVG(song_duration_minutes) as avg_song_duration,
#         COUNT(CASE WHEN is_jam = true THEN 1 END) as jam_count,
#         COUNT(CASE WHEN set_type = 'Encore' THEN 1 END) as encore_count
#     FROM {table: Identifier}
#     GROUP BY show_date, band_name
#     """,
#     select_tables=[setlist_entry_pipeline.get_table()]
# )


# Song transition patterns materialized view
# transitions_mv = MaterializedView(
#     "SongTransitions",
#     """
#     SELECT 
#         song_name as from_song,
#         transitions_into as to_song,
#         band_name,
#         COUNT(*) as transition_count,
#         MIN(show_date) as first_occurrence,
#         MAX(show_date) as last_occurrence
#     FROM {table: Identifier}
#     WHERE transitions_into IS NOT NULL 
#     AND transitions_into != ''
#     AND song_name != ''
#     GROUP BY song_name, transitions_into, band_name
#     """,
#     select_tables=[setlist_entry_pipeline.get_table()]
# )


# Example prompts

**Discovery (start here with unfamiliar data)**
- "What data do you have access to?" (`cluster_overview`)
- "Describe the biggest index and tell me what the geographic data looks like." (`describe_index`, `detect_geo_fields`)
- "Give me an insight report on `demo-quakes`." (`profile_index`, with a density map)

**Analytics**
- "Top 10 countries by number of events, with average magnitude." (`aggregate`)
- "Monthly trend of events above magnitude 5 over the last year." (`aggregate` with a date group and `interval=month`)
- "Which cities have the highest 95th-percentile depth?" (`aggregate`, `metric=p95`, `order_by_metric`)
- "How complete is the `status` field? What values does it take?" (`field_stats`)

**Spatial**
- "What's within 50 km of Tokyo? Nearest first." (`geo_distance_search`)
- "Where are the hotspots? How concentrated is the data?" (`geo_heatmap`)
- "How many stations are within 1, 5, 10 and 50 km of 37.77,-122.42?" (`geo_distance_bands`)
- "Show everything inside this polygon: POLYGON((-123 37, -122 37, -122 38, -123 38, -123 37))" (`geo_shape_search`)
- "Plot the offline stations on a map." (`map_documents` with `filters={"status":"offline"}`)
- "Group by country and show where each group is centred." (`aggregate` with `include_geo_centroid`)

**Power users**
- "Run ES|QL: FROM demo-quakes | STATS n=COUNT(*), m=AVG(magnitude) BY country | SORT n DESC | LIMIT 5" (`run_esql`)
- "Use Query DSL to find docs with a magnitude but no location." (`query_dsl`)

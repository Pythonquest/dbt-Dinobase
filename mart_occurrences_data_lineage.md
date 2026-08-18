# Data Lineage: mart_occurrences

## Overview
This document traces the complete data lineage for the `mart_occurrences` data mart, showing how data flows through the bronze (raw), silver (staging), and gold (mart) layers of the dbt pipeline.

---

## BRONZE LAYER (Raw Data Sources)

### Source: pbdb_raw.occurrences

**Description:** Raw occurrence data loaded from the PBDB API into BigQuery with compact vocabulary field names.

**Key Columns:**
- `oid` (occurrence_id): Unique occurrence identifier
- `cid` (collection_id): Collection identifier
- `rid` (reference_id): Reference identifier
- `idn` (identified_name): Identified taxonomic name
- `tid` (taxon_id): Accepted taxon identifier
- `tna` (taxon_name): Accepted taxonomic name
- `rnk` (taxonomic_rank): Accepted taxonomic rank (numeric)
- `oei`/`oli`: Early/late interval names
- `eag`/`lag`: Early/late age bounds in Ma (millions of years ago)
- `lng`/`lat`: Modern coordinates
- `pln`/`pla`: Paleocoordinates
- `pgm` (paleo_model): Paleocoordinate model used
- `gpl` (geoplate_id): Geoplate identifier

**Data Quality:** Raw API data with minimal transformation, subject to API vocabulary constraints.

---

## SILVER LAYER (Staging Models)

### Model: stg_occurrences

**Location:** `dbt/pbdb_dbt/models/staging/stg_occurrences.sql`

**Purpose:** Clean and standardize the raw occurrences data by renaming columns to human-readable names and applying consistent formatting.

**SQL Code:**
```sql
with source_data as (
    select *
    from {{ source('pbdb_raw', 'occurrences') }}
),

renamed as (
    select
        -- Primary identifiers
        cid as collection_id,
        oid as occurrence_id,
        rid as reference_id,

        -- Taxonomic information
        idn as identified_name,
        tid as taxon_id,
        tna as taxon_name,
        tdf as taxonomic_difference_flag,
        rnk as taxonomic_rank,

        -- Temporal information
        eag as early_age_ma,
        oei as early_interval,
        iid as interval_id,
        lag as latest_age_ma,
        oli as late_interval,

        -- Modern coordinates
        lng as longitude,
        lat as latitude,

        -- Paleocoordinates
        pln as paleo_longitude,
        pla as paleo_latitude,
        pm1 as paleo_model,
        gpl as geoplate_id,

        -- Data quality
        flg as record_flags,

        -- dbt metadata
        current_timestamp() as _dbt_loaded_at

    from source_data
)

select * from renamed
```

**What it Does:**
- Maps raw PBDB API field names to descriptive column names (e.g., `oid` → `occurrence_id`)
- Preserves all data without filtering or aggregation
- Adds a load timestamp (`_dbt_loaded_at`) for tracking
- Serves as the single source of truth for clean occurrence records

**Output:** Clean, renamed occurrence records with consistent naming conventions

---

### Related Staging Models (Dependencies)

#### Model: stg_collections
**Purpose:** Clean and standardize collection data (geographic and stratigraphic context for occurrences)

**Key Columns:**
- `collection_id`, `collection_name`, `reference_id`
- `longitude`, `latitude`, `paleo_longitude`, `paleo_latitude`, `paleo_model`, `geoplate_id`
- `country_code`, `state_province`, `county`
- `formation`, `strat_group`, `strat_member`, `environment`

#### Model: stg_taxa
**Purpose:** Clean and deduplicate taxonomic data with hierarchy information

**Key Columns:**
- `taxon_id`, `taxon_name`, `taxon_rank_code`
- Classification hierarchy: `phylum`, `taxon_class`, `taxon_order`, `family`, `genus`
- Occurrence statistics: `occurrence_count`
- Range data: `first_appearance_early_ma`, `first_appearance_late_ma`, `last_appearance_early_ma`, `last_appearance_late_ma`

---

## GOLD LAYER (Mart Models)

### Intermediate Model: int_occurrences_enriched

**Location:** `dbt/pbdb_dbt/models/intermediate/int_occurrences_enriched.sql`

**Purpose:** Enrich occurrence records by joining with collection and taxonomic data, creating a denormalized fact table ready for consumption by analytical marts.

**SQL Code:**
```sql
with occurrences as (
    select * from {{ ref('stg_occurrences') }}
),

collections as (
    select * from {{ ref('stg_collections') }}
),

taxa as (
    select * from {{ ref('stg_taxa') }}
),

enriched as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['o.occurrence_id']) }} as occurrence_surrogate_key,

        -- Occurrence identifiers
        o.occurrence_id,
        o.collection_id,
        o.reference_id,

        -- Taxonomic fields from occurrence record
        o.identified_name,
        o.taxon_id,
        o.taxon_name,
        o.taxonomic_difference_flag,
        o.taxonomic_rank,

        -- Classification hierarchy from taxa
        t.phylum,
        t.taxon_class,
        t.taxon_order,
        t.family,
        t.genus,

        -- Temporal (from occurrence)
        o.early_age_ma,
        o.early_interval,
        o.late_interval,
        o.latest_age_ma,
        o.interval_id,
        case
            when o.early_age_ma is not null and o.latest_age_ma is not null
                then (o.early_age_ma + o.latest_age_ma) / 2.0
            else coalesce(o.early_age_ma, o.latest_age_ma)
        end as midpoint_age_ma,

        cast(floor(
            case
                when o.early_age_ma is not null and o.latest_age_ma is not null
                    then (o.early_age_ma + o.latest_age_ma) / 2.0
                else coalesce(o.early_age_ma, o.latest_age_ma)
            end / 10
        ) * 10 as int64) as time_bin_start,

        -- First / last appearance from taxa
        t.first_appearance_early_ma,
        t.first_appearance_late_ma,
        t.last_appearance_early_ma,
        t.last_appearance_late_ma,

        -- Modern coordinates (occurrence-level, fallback to collection)
        coalesce(o.longitude, c.longitude) as longitude,
        coalesce(o.latitude, c.latitude) as latitude,

        -- Paleocoordinates (occurrence-level, fallback to collection)
        coalesce(o.paleo_longitude, c.paleo_longitude) as paleo_longitude,
        coalesce(o.paleo_latitude, c.paleo_latitude) as paleo_latitude,
        coalesce(o.paleo_model, c.paleo_model) as paleo_model,
        coalesce(cast(o.geoplate_id as string), c.geoplate_id) as geoplate_id,

        -- Location context from collection
        c.collection_name,
        c.country_code,
        c.state_province,
        c.county,

        -- Stratigraphy from collection
        c.formation,
        c.strat_group,
        c.strat_member,

        -- Environment from collection
        c.environment,

        -- Data quality
        o.record_flags,

        -- Metadata
        o._dbt_loaded_at

    from occurrences o
    left join collections c on o.collection_id = c.collection_id
    left join taxa t on o.taxon_id = t.taxon_id
)

select * from enriched
```

**What it Does:**
- **Denormalization:** Joins staging occurrence, collection, and taxa tables into a single wide table
- **Surrogate Key:** Generates a hash-based surrogate key from occurrence_id for efficient storage
- **Computed Columns:**
  - `midpoint_age_ma`: Calculates the midpoint between early and late ages
  - `time_bin_start`: Bins ages into 10-million-year intervals for time series analysis
- **Coordinate Resolution:** Uses occurrence-level coordinates with fallback to collection-level coordinates
- **Hierarchy Enhancement:** Adds full taxonomic classification (phylum through genus) from taxa lookup
- **Context Enrichment:** Includes geographical (country, state, county) and stratigraphic (formation, group, member) context from collections
- **Left Joins:** Preserves all occurrences even if collection or taxa data is missing

**Key Design Decisions:**
- Uses LEFT JOINs to ensure no loss of occurrence records
- Coalesces spatial coordinates with occurrence-level priority (more specific data)
- Time binning enables efficient aggregation for diversity through time analysis

---

### Final Mart: mart_occurrences

**Location:** `dbt/pbdb_dbt/models/marts/mart_occurrences.sql`

**Purpose:** Present the enriched occurrence data to end users and analytical tools.

**SQL Code:**
```sql
with enriched as (
    select * from {{ ref('int_occurrences_enriched') }}
)

select
    {{ dbt_utils.star(from=ref('int_occurrences_enriched')) }}
from enriched
```

**What it Does:**
- **Direct Pass-Through:** Exposes all columns from `int_occurrences_enriched` without modification
- **Consistent Schema:** Uses dbt's `star()` macro to dynamically include all columns, ensuring schema consistency if upstream changes
- **Consumer-Ready:** Provides a clean, documented interface for analysts and downstream tools

**Design Philosophy:**
This simple pass-through pattern allows:
- Single source of enriched occurrence data
- Easy schema documentation via dbt
- Future flexibility to add filtering, partitioning, or additional transformations without breaking downstream dependencies

---

## Data Lineage Flow

```
pbdb_raw.occurrences (BRONZE)
    ↓
stg_occurrences (SILVER)
    ↓
int_occurrences_enriched (SILVER→GOLD INTERMEDIATE)
    ← stg_collections (left join)
    ← stg_taxa (left join)
    ↓
mart_occurrences (GOLD)
```

## Key Insights

1. **Three-Layer Architecture:**
   - **Bronze:** Raw API data with minimal transformation
   - **Silver:** Cleaned, renamed, standardized tables
   - **Gold:** Denormalized facts ready for analysis

2. **Enrichment Strategy:**
   - The intermediate model consolidates related dimensions (collection, taxa) into a single fact table
   - LEFT JOINs preserve data integrity while adding context
   - Computed fields (midpoint_age, time_bin) support specific analytical patterns

3. **Coordinate Handling:**
   - Occurrence-level coordinates take precedence (more precise)
   - Falls back to collection-level coordinates for consistency
   - Supports both modern and paleographic coordinate systems

4. **Design Principle:**
   - Simple mart model delegates complexity to intermediate layer
   - Enables maintainability and allows future enhancements without disrupting consumers

---

## Related Downstream Marts

The `int_occurrences_enriched` intermediate model is also used by:
- `mart_diversity_through_time` - Aggregates occurrences into diversity metrics by time bins
- `mart_occurrences_extended` - Joins occurrence data with reference metadata and geologic time intervals


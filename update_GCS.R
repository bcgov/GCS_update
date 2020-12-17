##### 0. Package loading #####

library('dplyr')
library('magrittr') # For %<>% operator
library('tidyr')
library('readr')
library('sf') # For working with spatial files
library('stringr') # String manipulation
library('tictoc') # For timing
library('doParallel')
library('here') # for file navigation

##### 1. Initialisation and declarations #####

# Define variables
root_source <- ".."  # i.e., L:/ (one folder above project folder)
DMTI_source <- here("DMTI_Files","DMTI_2020_06","MEP")                                      ## <--- Update with new data. Check each run.

####
PCCF_source <- here("PCCF_Files","2020_02","PCCF_Feb2020","pccfNat_fccpNat_022020.txt")     ## <--- Update with new data. Check each run.
####

GEOFILES_source <- here(root_source, "GEOFILES")
GCS_source <- here("Shapefiles")
GCS_output <- here("Ready_4_Upload") 
GCS_prev_name <- 'GCS_202003.gpkg' # Points to the previous version of GCS                  ## <--- Update with new data. Check each run.
GCS_curr_name <- 'GCS_202006.gpkg' # Points to the new version of GCS                       ## <--- Update with new data. Check each run.
MHA_LHA_list <- c('035','037','201','202') ## MHAs are no longer relevant as of Mar 2018.
replacePOSITIONsPCCF <- 4 # Elements in the GCS with these POSITIONs 
                          # are replaced with their PCCF counterpart
p4s <- '+proj=longlat +datum=WGS84 +no_defs' # EPSG: 4326
epsg <- 4326
distance_epsg <- 3347 # Needs to be a CRS with easting and northing to get centroids

# Identify/register 2 CPUs for processing in parallel
join_cluster <- makeCluster(2)
registerDoParallel(join_cluster)

# Perform a spatial join in parallel. The target/first argument - x - is split
# into two parts with each part being joined with the join/second argument - y - and
# then returned unsplit.
# For a non-parallel (or linear) implementation, use the part defined under the
# Linear comment below, commenting out the default parallel implementation.
spatial_join <- function (x, y, ...) {
  # Parallel
  x_pieces <- list(1,2) # Interim pieces of x
  x_pieces[[1]] <- x %>% slice(1:floor(dim(x)[1]/2))
  x_pieces[[2]] <- x %>% slice(floor(dim(x)[1]/2+1:dim(x)[1]))
  geo_join <- foreach(i = 1:2,
                      .export = 'st_intersects',
                      .packages = 'sf'
                      ) %dopar% {
                        st_intersects(x_pieces[[i]], 
                                      y, 
                                      prepared = TRUE, ...)
                        }
  return(c(geo_join[[1]],geo_join[[2]]))
  # Linear
  #return(st_intersects(x, y, prepared = TRUE, ...))
}

tic('Geocoding postal codes')# Start timer

##### 2. Read source files #####

# There are two sources used in this application:
# 2.1 DMTI's CanMap Postal Code (OM) Suite
#     This includes up-to-date active and retired postal codes within BC
# 2.2 The previous release of the GCS
#     This is used as a way to identify any postal codes that may have been
#     dropped from DMTI's source. Any dropped records are appended and re-geocoded
#     to maintain some form of consistency over time.

##### 2.1 Read DMTI files ####
# BC Stats receives two files from DMTI Spatial (http://www.dmtispatial.com/canmap/):
# The first contains ACTIVE postal codes (BCmep.shp) while the second contains 
# RETIRED postal codes (BCmep_retired.shp). In order to maintain a comprehensive 
# list of postal codes, we read in both sources as a COMBINED source which is 
# later used in the geocoding.
DMTI_combined <- read_sf(
  file.path(DMTI_source, 'BCmep.shp')
) %>%
  mutate(LONGITUDE = HP_LONG, LATITUDE = HP_LAT) %>% 
  rbind(
    read_sf(
      file.path(DMTI_source, 'BCmep_retired.shp')
    )
  ) %>% 
  filter(SLI == 1) %>%
  select(MEP_ID,
         POSTALCODE, 
         SLI, 
         PROV, 
         COMM_NAME, 
         BIRTH_DATE, 
         RET_DATE, 
         LONGITUDE, 
         LATITUDE,  
         POSITION   
  ) %>%
  mutate( # Change all text columns to text
    POSTALCODE = as.character(POSTALCODE),
    PROV = as.character(PROV),
    COMM_NAME = as.character(COMM_NAME),
    BIRTH_DATE = as.character(BIRTH_DATE),
    RET_DATE = as.character(RET_DATE),
    SOURCE = 'NEW'
  )

# In order to perform spatial joins, one has to ensure that the projection of the
# spatial files are consistent. Here we set it to EPSG 4326
DMTI_combined %<>% st_set_crs(epsg)


##### 2.2 Read PCCF File #####
# The Postal Code Conversion File (PCCF) is a file created by Statistics Canada
# for a similar purpose to that of DMTI Spatial's BCmep. It, however, is only
# released annually.
# We use the PCCF as a substitute for postal codes that have a poor quality
# geocoding in the DMTI Spatial source (those with high/large POSITION values).

PCCF <- read_fwf(
  PCCF_source,
  fwf_positions(
    c(1, 10, 137, 138, 149, 162, 164, 196, 204), # start positions
    c(6, 11, 137, 148, 161, 162, 193, 203, 211), # end positions
    c( # column names
      'POSTALCODE',
      'PROV',
      'REP_POINT',
      'LATITUDE',
      'LONGITUDE',
      'SLI',
      'COMM_NAME',
      'BIRTH_DATE',
      'RET_DATE'
    )
  ),
  col_types = 'cciddiccc' # c = character, d = double, i = integer
) %>%
  dplyr::filter(
    str_sub(POSTALCODE, 1, 1) == 'V', # Select only BC postal codes
    SLI == 1
  ) %>%
  dplyr::mutate(
    MEP_ID = -1,
    POSITION = -1,
    PROV = 'BC',
    SOURCE = 'PCCF'
  ) %>%
  st_as_sf(
    coords = c('LONGITUDE', 'LATITUDE'), # LONG/LAT are removed and turned into geometry
    dim = 'XY',
    remove = FALSE
  ) %>% 
  st_set_crs(epsg)

# Remove duplicate postal codes within the PCCF (with SLI == 1)
# First arrange by
#   POSTALCODE
#   RET_DATE
# Those postal codes with a "small" RET_DATE will be the one that is currently active, 
# or has RET_DATE == '19000001'
PCCF %<>% arrange(POSTALCODE, RET_DATE)
PCCF %<>% filter(!duplicated(PCCF$POSTALCODE)) # Remove duplicates


##### 2.3 Exchange DMTI for PCCF based on POSITION #####
# Extract poor quality postal codes to replace with PCCF postal codes. This is based on the
# POSITION variable within the DMTI source:
#
# POSITION Code Description
#
# 1  Block-face representative point from CanMap? streets - Higher precision interpolated;
#    the postal code address range geocoded to street segment(s) within the FSA and Municipal 
#    boundary.
# 2  Block-face representation from CanMap? streets - Lower precision interpolated; 
#    the postal code address range geocoded to closest address on street segment(s) and 
#    within the FSA boundary.
# 3  Postal Code placed to CanMap Postal CodeOM File - Local Delivery Unit (LDU) centroid.
# 4  Postal Code placed to CanMap Postal CodeOM File - Forward Sortation Area (FSA) Centroid.
# 5  Postal Code placed to CanMap Placename (PPN) centroid.

for (replacePOSITION in replacePOSITIONsPCCF) {
  poorQuality <- DMTI_combined %>%
    dplyr::filter(POSITION == replacePOSITION) %>%
    dplyr::filter(POSTALCODE %in% PCCF$POSTALCODE[which(PCCF$REP_POINT < 4)])
  
  DMTI_combined %<>% dplyr::filter(!POSTALCODE %in% poorQuality$POSTALCODE)
  poorQuality %<>% as.data.frame() %>%
    #    dplyr::select(MEP_ID, POSTALCODE) %>%
    dplyr::select(-geometry) %>%
    dplyr::left_join(
      PCCF %>% 
        dplyr::select(POSTALCODE, LATITUDE, LONGITUDE) %>%
        rename(
          PCCF_LAT = LATITUDE,
          PCCF_LONG = LONGITUDE
        ),
      by = 'POSTALCODE'
    ) %>%
    #    dplyr::arrange(RET_DATE) %>%                      # Retain only latest,
    #    dplyr::distinct(POSTALCODE, .keep_all = TRUE) %>% # unique postal codes
    dplyr::mutate(
      #      MEP_ID = -MEP_ID,
      POSITION = -replacePOSITION,
      LONGITUDE = PCCF_LONG,
      LATITUDE = PCCF_LAT,
      SOURCE = 'PCCF'
    ) %>% 
    dplyr::select(
      -PCCF_LONG,
      -PCCF_LAT
    ) %>%
    st_as_sf(
      coords = c('LONGITUDE', 'LATITUDE'),
      dim = 'XY',
      remove = FALSE
    ) %>% st_set_crs(epsg)
  DMTI_combined %<>% rbind(
    poorQuality
  )
}
remove(
  replacePOSITION, poorQuality, replacePOSITIONsPCCF
)

# POSITION field is no longer important
DMTI_combined %<>% select(-POSITION)
PCCF %<>% select(-POSITION)

##### 2.4 Read and add historic GCS content #####

# The previous version of the GCS may contain postal codes that were not captured
# within DMTI_combined (originally BCmep and BCmep_retired). For completeness, we
# identify postal codes that exist in a previous version but not in the current
# and add them. (These postal codes will certainly have their retired date not
# equal to 19000001.)

# Read previous version of GCS
GCS_prev <- read_sf(
  file.path(GCS_source, GCS_prev_name)
) %>%
  select(
    MEP_ID, 
    POSTALCODE, 
    SLI, 
    PROV, 
    COMM_NAME, 
    BIRTH_DATE, 
    RET_DATE, 
    LONGITUDE, 
    LATITUDE
  ) %>% mutate(
    MEP_ID     = as.numeric(MEP_ID),
    POSTALCODE = as.character(POSTALCODE),
    SLI        = as.numeric(SLI),
    PROV       = as.character(PROV),
    COMM_NAME  = as.character(COMM_NAME),
    BIRTH_DATE = as.character(BIRTH_DATE),
    RET_DATE   = as.character(RET_DATE),
    LONGITUDE  = as.numeric(LONGITUDE),
    LATITUDE   = as.numeric(LATITUDE),
    SOURCE     = 'OLD'
  ) %>%
  rename(geometry = geom) %>%
  st_set_geometry('geometry') %>%
  st_transform(epsg) # Project to match postal code geography

# Find unmatched PCs from a previous GCS
unmatched_pcs <- as.data.frame(GCS_prev) %>%
  dplyr::anti_join(as.data.frame(DMTI_combined),
                   by = 'POSTALCODE') %>%
  st_as_sf(
    coords = c('LONGITUDE', 'LATITUDE'),
    dim = 'XY',
    remove = FALSE
  ) %>% 
  st_set_crs(epsg)

DMTI_combined %<>% rbind(unmatched_pcs) # Add unmatched postal codes

remove(unmatched_pcs, GCS_prev)

# Additional COMM_NAME processing to replace numbers into strings
# One Hundred Mile House municipality is called 100 Mile House in COMM_NAME, replace with string. 
# We don't need to replce other COMM_NAME with numbers since there is no equivalent municipality name
DMTI_combined$COMM_NAME[grep("100 MILE HOUSE", DMTI_combined$COMM_NAME)] <- "ONE HUNDRED MILE HOUSE"


##### 3. Spatial joins with current geographies #####

# The next set of procedures performs a spatial join between DMTI_combined - the
# combined ACTIVE and RETIRED postal codes within the province - and another
# spatial geography. The current list of joins include
#  - 2011 Census
#    - Municipal name (MUN_NAME_2011)
#    - Dissemination Area (DA_2011)
#    - Census division (CD_2011)
#    - Census subdivision (CSD_2011; also used to create CDCSD_2011)
#    - Development/Economic Region (DR_2011)
#    - Census Metropolitan Area/Census Agglomeration (CMACA_2011)
#    - Census Tract (CT_2011)
#    - Designated Place (DPL_2011)
#    - Federal Electoral District (FED_2011)
#  - 2016 Census
#    - Municipal name (MUN_NAME_2016)
#    - Dissemination Area (DA_2016)
#    - Census division (CD_2016)
#    - Census subdivision (CSD_2016; also used to create CDCSD_2016)
#    - Development/Economic Region (DR_2016)
#    - Census Metropolitan Area/Census Agglomeration (CMACA_2016)
#    - Census Tract (CT_2016)
#    - Designated Place (DPL_2016)
#    - Federal Electoral District (FED_2016)
#    - Population Centre (POPCTR_2016) *
#    - Community-Adjusted Municipal Name (COMM_MUN_NAME_2016) *
#    - Community-Adjusted Census subdivision (COMM_CDCSD_2016) *
#  - Health
#    - Community Health Service Area (CHSA) *
#    - Local Health Area (LHA)
#    - Health Service Delivery Area (HSDA)
#    - Health Authority (HA)
#  - MCFD
#    - Local Service Area (MCFD_LSA)
#    - Service Delivery Area (MCFD_SDA)
#    - MCFD Region (MCFD)
#  - Provincial Electoral Districts (PEDs)
#    - 1999 redistribution (PED_1999)
#    - 2009 redistribution (PED_2008)
#    - 2015 redistribution (PED_2015)
#  - Education
#    - School District (SD)
#    - Trustee Electoral Area (TEA) *
#  - College Region (CR)
#  - Tourism Region (TR)
#  - Service BC (SBC)
#  - RCMP Respondent Code Areas
#  - Timber Supply Areas (TSA)
#  - Games Zones (GZ)
#
# Each of the spatial joins that are performed used the doParallel package
# to perform the join in parallel. That is, using 2 CPUs rather than just 1.
# This speeds up the process by roughly 50% without compromising usage of
# the analyst's computer.
#
# Before each join, the new geography layer is read in and turned into a single-
# part polygon layer (as opposed to a multi-part polygon layer). Single-part
# shapes perform faster under spatial joins than multi-part shapes. Additionally,
# the new geography is projected to conform to that of the postal code geography
# in order to ensure correct joining. This prepreparation (identified by the
# parameter 'prepared = TRUE' within the spatial_join function) also improves 
# the processing speed of the spatial join.

prepare_sf <- function(x, filter = FALSE) {
  x %<>% filter(if(filter){PRUID == '59'} else {TRUE}) %>% # filtered to BC (for census data)
    # Turned warning off: repeating attributes for all sub-geometries for which they may not be constant
    st_cast('POLYGON', warn = FALSE) %>% # Make singlepart (if multipart)
    st_transform(epsg) # Project to match postal code geography
}

## This function is dependent on:
##  DMTI_combined
##  HD_point
##  temp
update_unmatched_pcs <- function(filter_col, new_cols, temp_cols, filter_exp = NULL){
  ## Identify unmatched PCs and remove from DMTI_combined
  if(!is.null(filter_exp)) {
    ## This is currently valid for MHAs only
    ungeocoded <- DMTI_combined %>% filter(is.na({{filter_col}}) & {{filter_exp}})
    DMTI_combined %<>% filter(!(is.na({{filter_col}}) & {{filter_exp}}))
    
  } else {
    ungeocoded <- DMTI_combined %>% filter(is.na({{filter_col}}))
    DMTI_combined %<>% filter(!is.na({{filter_col}}))
  }
  
  ## Project to match HD Point geography
  temp %<>% st_transform(crs = st_crs(HD_point)) 
  ungeocoded %<>% st_transform(crs = st_crs(HD_point)) 
  
  ## Join Dissemination Block points to current geography (temp)
  geo_join <- HD_point %>% 
    spatial_join(temp)
   
  # Remove DBs outside current geography
  non_join <- lapply(geo_join, length) 
  geo_join <- geo_join[non_join != 0]
  HD_point_temp <- HD_point[non_join != 0,]
  
  # Find the closest DB to the uncoded Postal Code point
  closest <- st_distance(ungeocoded, HD_point_temp) %>%
    apply(1, which.min)
  
  # Assign the matched current geog of the closest DB to the uncoded PCs
  update_col <- function(new_nm, old_nm) {
    ungeocoded <<- ungeocoded %>% 
      mutate({{new_nm}} := (temp %>% pull({{old_nm}}))[as.integer(geo_join[closest])])
  }
  
  purrr::walk2(new_cols, temp_cols, update_col)

  # Return the full DMTI_combined with the updated PC geogs
  DMTI_combined %<>% rbind(ungeocoded %>% st_transform(epsg)) %>%
    st_set_crs(epsg)
}

##### 3.0 Dissemination Block #####

# Some postal codes may lie outside of a geographic boundary. This happens under the following
# conditions/circumstances:
# - The geography does not cover the entire provincial landscape
# - The geography is not consistent and may have certain areas/edges that are not 
#   consistent/coincident with one another
# - Postal code placement may be inaccurate and use FSA centroids (for example). This is 
#   indicated by the POSITION flag within the DMTI source. Here are the values for that field
#   in order of level-of-accuracy:
#   1. Block-face representative point from CanMap® streets – Higher precision interpolated; 
#      the postal code address range geocoded to street segment(s) within the FSA and 
#      Municipal boundary.
#   2. Block-face representation from CanMap® streets – Lower precision interpolated; 
#      the postal code address range geocoded to closest address on street segment(s) and 
#      within the FSA boundary.
#   3. Postal Code placed to CanMap Postal CodeOM File - Local Delivery Unit (LDU) centroid.
#   4. Postal Code placed to CanMap Postal CodeOM File - Forward Sortation Area (FSA) Centroid.
#   5. Postal Code placed to CanMap Placename (PPN) centroid.
# In order to avoid issues with this, we use a high-density point file to geocode postal code
# misses.

print("Processing Dissemination Block...")

db <- read_sf(
  file.path(GEOFILES_source, 
         "Census2016","Block","ldb_000b16a_e.shp")) # 2016 Census DB

HD_point <- db %>% 
  filter(PRUID == '59') %>% # filtered to BC
  # Turned warning off: repeating attributes for all sub-geometries for which they may not be constant
  st_cast('POLYGON', warn = FALSE) %>% # Make singlepart (if multipart)
  select(DBUID) #%>% # Only interested in this data set as a high-definition point file

# explicitly define the attributes as constant throughout the geometry  
# This is done in prep of st_centroid which would warn it is making this assumption implicitly if not defined
st_agr(HD_point) <- "constant"
  
HD_point %<>%
  st_transform(distance_epsg) %>% # Project for accurate distance measurement, CRS must have easting/northing axes
  st_centroid()# %>% # Extract centroids only (used later in distance calculations)

temp <- db %>% # 2016 Census DB
  prepare_sf(filter = TRUE)

geo_join <- DMTI_combined %>% spatial_join(temp)

DMTI_combined %<>% mutate(DB_2016 = temp$DBUID[as.integer(geo_join)])

# Change all text columns to text
DMTI_combined %<>% mutate_at(vars(DB_2016), as.character)

# Assign postal codes with DB == NA to the closest DB
if(sum(is.na(DMTI_combined$DB_2016))) {
  DMTI_combined <- update_unmatched_pcs(DB_2016, quos(DB_2016), quos(DBUID))
}

remove(db)

##### 3.1 Census (2011) #####

print("Processing Census 2011...")

temp <- read_sf(
  file.path(GEOFILES_source, 
         "Census2011","DA","gda_000b11a_e.shp")) %>% # 2011 Census DA
  prepare_sf(filter = TRUE)

# The DA shape file loaded above contains the following useful geography identifiers:
# - DAUID (Dissemination Area UID) ~ DA
# - CDUID (Census Division UID) ~ CD
# - CSDUID (Census Subdivision UID) ~ CSD / CDCSD
# - CSDNAME (Census Subdivision Name) ~ MUN_NAME
# - ERUID (Economic Region UID) ~ DR
# - CMAUID (Census Metropolitan Area UID) ~ CMA
# - CTUID (Census Tract UID) ~ CT
geo_join <- DMTI_combined %>% spatial_join(temp)

DMTI_combined %<>% mutate(
  MUN_NAME_2011 = temp$CSDNAME[as.integer(geo_join)],
  CD_2011 = temp$CDUID[as.integer(geo_join)],
  CSD_2011 = temp$CSDUID[as.integer(geo_join)],
  CDCSD_2011 = temp$CSDUID[as.integer(geo_join)],
  CMACA_2011 = temp$CMAUID[as.integer(geo_join)],
  DA_2011 = temp$DAUID[as.integer(geo_join)],
  CT_2011 = temp$CTNAME[as.integer(geo_join)],
  DR_2011 = temp$ERUID[as.integer(geo_join)])

# Identify and update unmatched postal codes
if (sum(is.na(DMTI_combined$DA_2011))) {
  DMTI_combined <- update_unmatched_pcs(DA_2011, 
                                        quos(MUN_NAME_2011, CD_2011, CSD_2011, CDCSD_2011, CMACA_2011, DA_2011, CT_2011, DR_2011),
                                        quos(CSDNAME, CDUID, CSDUID, CSDUID, CMAUID, DAUID, CTNAME, ERUID))
}


temp <- read_sf(
  file.path(GEOFILES_source,
         "Census2011","DPL","gdpl000b11a_e.shp")) %>% # 2011 Census DPL
  prepare_sf(filter = TRUE)

# The DA shape file loaded above contains the following useful geography identifiers:
# - DPLUID (Dissemination Area UID) ~ DPL
geo_join <- DMTI_combined %>% spatial_join(temp)

DMTI_combined %<>% mutate(DPL_2011 = temp$DPLUID[as.integer(geo_join)])

temp <- read_sf(
  file.path(GEOFILES_source,
         "Census2011","FED","gfed000b11a_e.shp")) %>% # 2011 Census FED
  prepare_sf(filter = TRUE)

geo_join <- DMTI_combined %>% spatial_join(temp)

DMTI_combined$FED_2011 <- temp$FEDUID[as.integer(geo_join)]

# Identify and update unmatched postal codes
if (sum(is.na(DMTI_combined$FED_2011))) {
  DMTI_combined <- update_unmatched_pcs(FED_2011, 
                                        quos(FED_2011),
                                        quos(FEDUID))
}

# Change all text columns to text
DMTI_combined %<>% mutate_at(vars("MUN_NAME_2011", "CD_2011", "CSD_2011", "CDCSD_2011", "CMACA_2011", 
                                  "DA_2011", "CT_2011", "DR_2011", "DPL_2011", "FED_2011"),
                             as.character)

##### 3.2 Census (2016) #####

print("Processing Census 2016...")

temp <- read_sf(
  file.path(GEOFILES_source,
         "Census2016","DA","lda_000b16a_e.shp")) %>% # 2016 Census DA
  prepare_sf(filter = TRUE)

# The DA shape file loaded above contains the following useful geography identifiers:
# - DAUID (Dissemination Area UID) ~ DA
# - CDUID (Census Division UID) ~ CD
# - CSDUID (Census Subdivision UID) ~ CSD / CDCSD
# - CSDNAME (Census Subdivision Name) ~ MUN_NAME
# - ERUID (Economic Region UID) ~ DR
# - CMAUID (Census Metropolitan Area UID) ~ CMA
# - CTUID (Census Tract UID) ~ CT
geo_join <- DMTI_combined %>% spatial_join(temp)

DMTI_combined %<>% mutate(
  MUN_NAME_2016 = temp$CSDNAME[as.integer(geo_join)],
  CD_2016 = temp$CDUID[as.integer(geo_join)],
  CSD_2016 = temp$CSDUID[as.integer(geo_join)],
  CDCSD_2016 = temp$CSDUID[as.integer(geo_join)],
  CMACA_2016 = temp$CMAUID[as.integer(geo_join)],
  DA_2016 = temp$DAUID[as.integer(geo_join)],
  CT_2016 = temp$CTNAME[as.integer(geo_join)],
  DR_2016 = temp$ERUID[as.integer(geo_join)])

# Identify and update unmatched postal codes
if (sum(is.na(DMTI_combined$DA_2016))) {
  DMTI_combined <- update_unmatched_pcs(DA_2016, 
                                        quos(MUN_NAME_2016, CD_2016, CSD_2016, CDCSD_2016, CMACA_2016, DA_2016, CT_2016, DR_2016),
                                        quos(CSDNAME, CDUID, CSDUID, CSDUID, CMAUID, DAUID, CTNAME, ERUID))
}

temp <- read_sf(
  file.path(GEOFILES_source,
         "Census2016","DPL","ldpl000b16a_e.shp")) %>% # 2016 Census DPL
  prepare_sf(filter = TRUE)

geo_join <- DMTI_combined %>% spatial_join(temp)

DMTI_combined %<>% mutate(DPL_2016 = temp$DPLUID[as.integer(geo_join)])

temp <- read_sf(
  file.path(GEOFILES_source,
         "Census2016","FED","lfed000b16a_e.shp")) %>% # 2016 Census FED
  prepare_sf(filter = TRUE)

geo_join <- DMTI_combined %>% spatial_join(temp)

DMTI_combined %<>% mutate(FED_2016 = temp$FEDUID[as.integer(geo_join)])

# Identify and update unmatched postal codes
if (sum(is.na(DMTI_combined$FED_2016))) {
  DMTI_combined <- update_unmatched_pcs(FED_2016, 
                                        quos(FED_2016),
                                        quos(FEDUID))
}

# Change all text columns to text
DMTI_combined %<>% mutate_at(vars("MUN_NAME_2016", "CD_2016", "CSD_2016", "CDCSD_2016", "CMACA_2016",
                                  "DA_2016", "CT_2016", "DR_2016", "DPL_2016", "FED_2016"),
                             as.character)

## Population Centre

print("Processing Population Centres...")

temp <- read_sf(
  file.path(GEOFILES_source,
         "Census2016","PopulationCentre","lpc_000b16a_e.shp")) %>% # Popukation centre
  prepare_sf(filter = TRUE)

geo_join <- DMTI_combined %>% spatial_join(temp)

DMTI_combined %<>% mutate(POPCTR_2016 = temp$PCUID[as.integer(geo_join)])

# Don't need to update unmatched PC - they are ok, that means that the postal code is not in a Pop centre but is Rural

# Change all text columns to text
DMTI_combined %<>% mutate_at(vars(POPCTR_2016), as.character)

##### 3.3 Ministry of Health #####
# The Ministry of Health as asked us to geocode Community Health service Areas (CHSA) from DBs based on the DB-CHSA crosswalk. 
# DBs are already coded earlier in DMTI_combined, so we match them from the crosswalk
# Instead of reading in from 'L:/GEOFILES/Health/Data/Health boundaries 2019/Final/CHSA_2018/CHSA_2018.shp'
print("Processing CHSA...")

chsa_crosswalk <- read.csv('L:/GEOFILES/Health/Data/Health boundaries 2019/Final/DB-CHSA_2018.csv', stringsAsFactors = FALSE) %>%
  rename(DB_2016 = DBuid, CHSA = CHSA_CD, LHA1997 = LHA) %>%
  mutate(DB_2016 = as.character(DB_2016), CHSA = as.character(CHSA), LHA1997 = str_pad(as.character(LHA1997), 3, pad = "0"))

DMTI_combined %<>% 
  left_join(chsa_crosswalk %>% select(DB_2016, CHSA), by = "DB_2016") %>%
  mutate(
    LHA = substr(CHSA, 1, 3),  
    HSDA = substr(CHSA, 1, 2), 
    HA = substr(CHSA, 1, 1)) 

## (old) 2013 LHA areas (new LHAs were implemented when the CHSA were introduced in 2018-02)
## Also now matched using CHSA crosswalk instead of reading in from 'L:/GEOFILES/Health/Data/LHA_2013.shp'

print("Processing Pre 2018 LHA...")

DMTI_combined %<>% mutate(LHA_PRE_2018 = (DMTI_combined %>% left_join(chsa_crosswalk, by = "DB_2016"))$LHA1997)

## (old) MHAs are no longer relevant as of Mar 2018
temp <- read_sf(
  file.path(GEOFILES_source,
        "Health","Data","MHA_2013.shp")) %>% # Micro Health Area (MHA)
  prepare_sf()

geo_join <- DMTI_combined %>% spatial_join(temp)

DMTI_combined %<>% mutate(MHA = temp$MHA[as.integer(geo_join)])

# Identify and update unmatched postal codes
if (sum(is.na(DMTI_combined$MHA) &
        (DMTI_combined$LHA_PRE_2018 %in% MHA_LHA_list))) {
  DMTI_combined <- update_unmatched_pcs(MHA,
                                        quos(MHA),
                                        quos(MHA),
                                        DMTI_combined$LHA_PRE_2018 %in% MHA_LHA_list)
}

# Change all text columns to text
DMTI_combined %<>% mutate_at(vars("CHSA", "LHA", "HSDA", "HA", "MHA"), as.character)

remove(chsa_crosswalk)

##### 3.4 Ministry of Children and Families Districts (MCFD) #####

print("Processing MCFD...")

temp <- read_sf(
  file.path(GEOFILES_source,
         "MCFD","Data","MCFD_LSA_2016.shp")) %>% # MCFD Local Service Area (LSA)
  prepare_sf()

geo_join <- DMTI_combined %>% spatial_join(temp)

DMTI_combined %<>% mutate(
  MCFD_LSA = temp$LSA[as.integer(geo_join)],
  MCFD_SDA = temp$SDA_NUM[as.integer(geo_join)],
  MCFD = temp$MCFD_NUM[as.integer(geo_join)])

# Identify and update unmatched postal codes
if (sum(is.na(DMTI_combined$MCFD))) {
  DMTI_combined <- update_unmatched_pcs(MCFD,
                                        quos(MCFD_LSA, MCFD_SDA, MCFD),
                                        quos(LSA, SDA_NUM, MCFD_NUM))
}

# Change all text columns to text
DMTI_combined %<>% mutate_at(vars("MCFD_LSA", "MCFD_SDA", "MCFD"), as.character)

##### 3.5 Provincial Electoral Districts (PED) #####

print("Processing PED...")

temp <- read_sf(
  file.path(GEOFILES_source,
         "PED","Data","PED_1999.shp")) %>% # Provincial Electoral District (PED) 1999 Redistribution
  prepare_sf()

geo_join <- DMTI_combined %>% spatial_join(temp)

DMTI_combined %<>% mutate(PED_1999 = temp$PED_N[as.integer(lapply(geo_join, first))])

# Identify and update unmatched postal codes
if (sum(is.na(DMTI_combined$PED_1999))) {
  DMTI_combined <- update_unmatched_pcs(PED_1999,
                                        quos(PED_1999),
                                        quos(PED_N))
}

temp <- read_sf(
  file.path(GEOFILES_source,
         "PED","Data","PED_2008.shp")) %>% # Provincial Electoral District (PED) 2009 Redistribution
  prepare_sf()

geo_join <- DMTI_combined %>% spatial_join(temp)

DMTI_combined %<>% mutate(PED_2009 = temp$PED_NUM08[as.integer(geo_join)])

# Identify and update unmatched postal codes
if (sum(is.na(DMTI_combined$PED_2009))) {
  DMTI_combined <- update_unmatched_pcs(PED_2009,
                                        quos(PED_2009),
                                        quos(PED_NUM08))
}

temp <- read_sf(
  file.path(GEOFILES_source,
         "PED","Data","PED_2015.shp")) %>% # Provincial Electoral District (PED) 2015 Redistribution
  prepare_sf()

geo_join <- DMTI_combined %>% spatial_join(temp)

DMTI_combined %<>% mutate(PED_2015 = temp$PED15_NUM[as.integer(geo_join)])

# Identify and update unmatched postal codes
if (sum(is.na(DMTI_combined$PED_2015))) {
  DMTI_combined <- update_unmatched_pcs(PED_2015,
                                        quos(PED_2015),
                                        quos(PED15_NUM))
}

# Change all text columns to text
DMTI_combined %<>% mutate_at(vars("PED_1999", "PED_2009", "PED_2015"), as.character)

##### 3.6 Trustee Electoral Areas (TEA) and School Districts (SD) #####
## Trustee Elecotral Areas (TEA)
## No longer get School Disctricts from 'L:/GEOFILES/School_District/Data/SD_2008.shp'
## now we just take the first 2 character from TEA so they line up.

print("Processing Trustee Electoral Areas and School Districts...")

## TEAs
temp <- read_sf(
  file.path(GEOFILES_source, "School_District","Data","TEA_20190312.shp")) %>% # Trustee Electoral Areas
  prepare_sf()

geo_join <- DMTI_combined %>% spatial_join(temp)

DMTI_combined %<>% mutate(TEA = temp$SDTEAUID[as.integer(lapply(geo_join, first))])

# Identify and update unmatched postal codes
if (sum(is.na(DMTI_combined$TEA))) {
  DMTI_combined <- update_unmatched_pcs(TEA,
                                        quos(TEA),
                                        quos(SDTEAUID))
}

## SDs
DMTI_combined$SD <- substr(DMTI_combined$TEA, 1, 2)

# Change all text columns to text
DMTI_combined %<>% mutate_at(vars("TEA", "SD"), as.character)

##### 3.7 College Regions (CR) #####

print("Processing College Region...")

temp <- read_sf(
  file.path(GEOFILES_source,
         "CollegeRegion","Data","CR_2012.shp")) %>% # College Region
  prepare_sf()

geo_join <- DMTI_combined %>% spatial_join(temp)

DMTI_combined %<>% mutate(CR = temp$CR_NUM[as.integer(geo_join)])

# Identify and update unmatched postal codes
if (sum(is.na(DMTI_combined$CR))) {
  DMTI_combined <- update_unmatched_pcs(CR,
                                        quos(CR),
                                        quos(CR_NUM))
}

# Change all text columns to text
DMTI_combined %<>% mutate_at(vars("CR"), as.character)

##### 3.8 Tourism Regions #####

print("Processing Tourism Regions...")

temp <- read_sf(
  file.path(GEOFILES_source,
         "Tourism","Data","Tourism_2012.shp")) %>% # Tourism Regions
  prepare_sf()

geo_join <- DMTI_combined %>% spatial_join(temp)

DMTI_combined %<>% mutate(TOURISM = temp$TOURISM[as.integer(geo_join)])

# Identify and update unmatched postal codes
if (sum(is.na(DMTI_combined$TOURISM))) {
  DMTI_combined <- update_unmatched_pcs(TOURISM,
                                        quos(TOURISM),
                                        quos(TOURISM))
}

# Change all text columns to text
DMTI_combined %<>% mutate_at(vars("TOURISM"), as.character)

##### 3.9 Service BC Regions (SBC) #####

print("Processing Service BC Regions...")

temp <- read_sf(
  file.path(GEOFILES_source,
         "ServiceBC","Data","service_bc_2011.shp")) %>% # Service BC Regions
  prepare_sf()

geo_join <- DMTI_combined %>% spatial_join(temp)

DMTI_combined %<>% mutate(SBC = as.character(temp$SBC_NUM[as.integer(geo_join)]))

# Identify and update unmatched postal codes
if (sum(is.na(DMTI_combined$SBC))) {
  DMTI_combined <- update_unmatched_pcs(SBC,
                                        quos(SBC),
                                        quos(SBC_NUM))
}

# Change all text columns to text
DMTI_combined %<>% mutate_at(vars("SBC"), as.character)

##### 3.10 RCMP Respondent Code Areas #####

print("Processing RCMP areas...")

temp <- read_sf(
  file.path(GEOFILES_source,
         "Police","Data","Resp_Codes_Cartographic2014.shp")) %>% # RCMP Resp
  prepare_sf()

geo_join <- DMTI_combined %>% spatial_join(temp)

DMTI_combined %<>% mutate(RESP = temp$RESP[as.integer(geo_join)])

# Identify and update unmatched postal codes
if (sum(is.na(DMTI_combined$RESP))) {
  DMTI_combined <- update_unmatched_pcs(RESP,
                                        quos(RESP),
                                        quos(RESP))
}

# Change all text columns to text
DMTI_combined %<>% mutate_at(vars("RESP"), as.character)

##### 3.11 Timber Supply Areas (TSA) #####

print("Processing Timber Supply Area...")

temp <- read_sf(
  file.path(GEOFILES_source,
         "TSA","Data","BC_TSA_2017.shp")) %>% # Timber Supply Areas
  prepare_sf()

geo_join <- DMTI_combined %>% spatial_join(temp)

DMTI_combined %<>% mutate(TSA = temp$TSA_NUMBER[as.integer(lapply(geo_join, first))])

# Identify and update unmatched postal codes
if (sum(is.na(DMTI_combined$TSA))) {
  DMTI_combined <- update_unmatched_pcs(TSA,
                                        quos(TSA),
                                        quos(TSA_NUMBER))
}

# Change all text columns to text
DMTI_combined %<>% mutate_at(vars("TSA"), as.character)

##### 3.12 Games Zones (GZ) #####

print("Processing Games Zones...")

temp <- read_sf(
  file.path(GEOFILES_source,
         "Games_Zones","Data","BC_Games_Zones.shp")) %>% # BC Games Zones 
  prepare_sf()

geo_join <- DMTI_combined %>% spatial_join(temp)

DMTI_combined %<>% mutate(GZ = temp$ZONE_NUM[as.integer(lapply(geo_join, first))])

# Identify and update unmatched postal codes
if (sum(is.na(DMTI_combined$GZ))) {
  DMTI_combined <- update_unmatched_pcs(GZ,
                                        quos(GZ),
                                        quos(GZ_NUM))
}

# Change all text columns to text
DMTI_combined %<>% mutate_at(vars("GZ"), as.character)

##### 3.Z End of spatial joins #####

toc()# Stop timer

stopCluster(join_cluster)

remove(geo_join, temp, join_cluster, distance_epsg, p4s, HD_point)


##### 4. Fix census-related SGCs and pad with zeroes #####

## 2011 Census
DMTI_combined %<>% mutate(
  CD_2011    = str_sub(CD_2011, 3, 4), # CDCSD_2011: XXYYZZZ | XX = 59 = PROV, YY = CD, ZZZ = CSD
  CSD_2011   = str_sub(CSD_2011, 5, 7), # CDCSD_2011: XXYYZZZ | XX = 59 = PROV, YY = CD, ZZZ = CSD
  CDCSD_2011 = str_sub(CDCSD_2011, 3, 7), # CDCSD_2011: XXYYZZZ | XX = 59 = PROV, YY = CD, ZZZ = CSD
  DA_2011    = str_sub(DA_2011, 5, 8), # DA_2011: XXYYZZZZ | XX = 59 = PROV, YY = CD, ZZZZ = DA
  DR_2011    = str_sub(DR_2011, 3, 3), # DR_2011: XXYZ | XX = 59 = PROV, Y = DR, Z = 0
  DPL_2011   = str_sub(DPL_2011, 3, 6) # DPL_2011: XXYYYY | XX = 59 = PROV, YYYY = DPL
)

## 2016 Census
DMTI_combined %<>% mutate(
  CD_2016    = str_sub(CD_2016, 3, 4), # CDCSD_2016: XXYYZZZ | XX = 59 = PROV, YY = CD, ZZZ = CSD
  CSD_2016   = str_sub(CSD_2016, 5, 7), # CDCSD_2016: XXYYZZZ | XX = 59 = PROV, YY = CD, ZZZ = CSD
  CDCSD_2016 = str_sub(CDCSD_2016, 3, 7), # CDCSD_2016: XXYYZZZ | XX = 59 = PROV, YY = CD, ZZZ = CSD
  DA_2016    = str_sub(DA_2016, 5, 8), # DA_2016: XXYYZZZZ | XX = 59 = PROV, YY = CD, ZZZZ = DA
  DR_2016    = str_sub(DR_2016, 3, 3), # DR_2016: XXYZ | XX = 59 = PROV, Y = DR, Z = 0
  DPL_2016   = str_sub(DPL_2016, 3, 6) # DPL_2016: XXYYYY | XX = 59 = PROV, YYYY = DPL
)

## Pad entries with zero
DMTI_combined %<>% mutate(
  SD       = str_pad(SD, 2, side = 'left', pad = '0'),
  CR       = str_pad(CR, 2, side = 'left', pad = '0'),
  PED_1999 = str_pad(PED_1999, 2, side = 'left', pad = '0'),
  PED_2009 = str_pad(PED_2009, 2, side = 'left', pad = '0'),
  PED_2015 = str_pad(PED_2015, 2, side = 'left', pad = '0'),
  SBC      = str_pad(SBC, 2, side = 'left', pad = '0')
)

##### 5. Prepare final output #####

print("Preparing final output...")

DMTI_combined %<>% 
  # Update PROV and ACTIVE fields  
  mutate(PROV = 'BC') %>%
  mutate(ACTIVE = ifelse(RET_DATE == '19000001', 'Y', 'N'),
  # Update Longitude and Latitude fields using geometry field
  LONGITUDE = unlist(lapply(DMTI_combined$geometry, first)),
  LATITUDE = unlist(lapply(DMTI_combined$geometry, last)))

# Fix all NAs to be <blank>
DMTI_combined[is.na(DMTI_combined)] <- ''

# Sort data alphanumerically based on POSTALCODE
DMTI_combined %<>% arrange(POSTALCODE)

##### 6. Write GCS output #####

print("Writing GCS output...")

if (!dir.exists('C:/Temp/')) {
  dir.create('C:/Temp')
} else {
  if (file.exists('C:/Temp/temp.gpkg')) {
    file.remove('C:/Temp/temp.gpkg')
  }
  if (file.exists('C:/Temp/temp.csv')) {
    file.remove('C:/Temp/temp.csv')
  }
}

# Write geocoded postal codes to .gpkg format
st_write(
  obj = DMTI_combined,
  dsn = file.path('C:','Temp','temp.gpkg'),
  layer = str_replace(GCS_curr_name, '.gpkg', ''),
  factorsAsCharacter = TRUE
)
temp <- file.copy(
  file.path('C:','Temp','temp.gpkg'),
  file.path(GCS_source, GCS_curr_name),
  copy.mode = TRUE,
  copy.date = FALSE,
  overwrite = TRUE
)
if (temp) {
  file.remove(file.path('C:','Temp','temp.gpkg'))
}

# Write geocoded postal code data to .csv and .rds
write.csv(
  as.data.frame(DMTI_combined) %>% select(-geometry),
  file = file.path('C:','Temp','temp.csv'),
  row.names = FALSE,
  quote = TRUE # ...this ensures that zero-padding is properly saved
)
temp <- file.copy(
  file.path('C:','Temp','temp.csv'),
  file.path(GCS_output, str_replace(GCS_curr_name, '.gpkg', '.csv')),
  copy.mode = TRUE,
  copy.date = FALSE,
  overwrite = TRUE
)
saveRDS(as.data.frame(DMTI_combined) %>% select(-geometry),
        file = file.path(GCS_output, str_replace(GCS_curr_name, '.gpkg', '.rds'))
)
if (temp) {
  temp <- file.remove(file.path('C:','Temp','temp.csv'))
}

##### 7. Clean workspace #####

print("Cleaning Workspace...")

remove(
  epsg, temp,
  GCS_source, GEOFILES_source, GCS_output,
  GCS_curr_name, GCS_prev_name,
  DMTI_source,
  PCCF_source,
  root_source,
  MHA_LHA_list,
  spatial_join,
  prepare_sf,
  update_unmatched_pcs
)

print("Done...")



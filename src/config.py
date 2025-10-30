# Base CKAN instance for Toronto Open Data
BASE_URL = "https://ckan0.cf.opendata.inter.prod-toronto.ca"

# The package ("dataset") we’re querying
PACKAGE_ID_RIDERSHIP = "bike-share-toronto-ridership-data"
PACKAGE_ID_STATION = "bike-share-toronto"

# CKAN action endpoints we’ll hit
API = {
    "package_show": "/api/3/action/package_show",
    "resource_show": "/api/3/action/resource_show",
}


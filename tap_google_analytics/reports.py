PREMADE_REPORTS = [
    {
        "name": "Audience Overview",
        "metrics": [
            "ga:users",
            "ga:newUsers",
            "ga:sessions",
            "ga:sessionsPerUser",
            "ga:pageviews",
            "ga:pageviewsPerSession",
            "ga:avgSessionDuration",
            "ga:bounceRate"
        ],
        "dimensions": [
            "ga:date",
            "ga:language",
            "ga:country",
            "ga:city",
            "ga:browser",
            "ga:operatingSystem",
            "ga:screnResolution",
            "ga:year",
            "ga:month",
            "ga:hour",
            "ga:minute"
        ],
    },
    {
        "name": "Audience Geo Location",
        "metrics": [
            "ga:users",
            "ga:newUsers",
            "ga:sessions",
            "ga:pageviewsPerSession",
            "ga:avgSessionDuration",
            "ga:bounceRate"
        ],
        "dimensions": [
            "ga:date",
            "ga:year",
            "ga:month",
            "ga:hour",
            "ga:minute",
            "ga:country",
            "ga:city",
            "ga:continent",
            "ga:subContinent"
        ]
    },
    {
        "name": "Audience Technology",
        "metrics": [
            "ga:users",
            "ga:newUsers",
            "ga:sessions",
            "ga:pageviewsPerSession",
            "ga:avgSessionDuration",
            "ga:bounceRate"
        ],
        "dimensions": [
            "ga:date",
            "ga:year",
            "ga:month",
            "ga:hour",
            "ga:minute",
            "ga:browser",
            "ga:operatingSystem",
            "ga:screenResolution",
            "ga:screenColors",
            "ga:flashVersion",
            "ga:javaEnabled",
            "ga:hostname"
        ],
    },
    {
        "name": "Acquisition Overview",
        "metrics": [
            "ga:users",
            "ga:newUsers",
            "ga:sessions",
            "ga:pageviewsPerSession",
            "ga:avgSessionDuration",
            "ga:bounceRate"
        ],
        "dimensions": [
            "ga:acquisitionTrafficChannel",
            "ga:channelGrouping",
            "ga:acquisitionSource", # Had ga:source
            "ga:acquisitionSourceMedium", # Had ga:sourceMedium
            "ga:acquisitionMedium", # Had ga:medium
            "ga:date",
            "ga:year",
            "ga:month",
            "ga:hour",
            "ga:minute",
        ],
    },
    {
        "name": "Behavior Overview",
        "metrics": [
            "ga:pageviews",
            "ga:uniquePageviews",
            "ga:avgTimeOnPage",
            "ga:bounceRate",
            "ga:exitRate",
            "ga:exits"
        ],
        "dimensions": [
            "ga:date",
            "ga:year",
            "ga:month",
            "ga:hour",
            "ga:minute", # TODO: Not a thing?
            "ga:pagePath",
            "ga:pageTitle",
            "ga:searchKeyword",
            "ga:eventCategory"
        ],
    },
    {
        "name": "eCommerce", #TODO: Name me
        "metrics": ["ga:transactions"],
        "dimensions": [
            "ga:transactionId",
            "ga:campaign",
            "ga:source",
            "ga:medium",
            "ga:keyword",
            "ga:socialNetwork"
        ],
    },
]

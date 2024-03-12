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
        ],
        "default_dimensions": [
            "ga:date"
        ]
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
            "ga:country",
            "ga:city",
            "ga:continent",
            "ga:subContinent"
        ],
        "default_dimensions": [
            "ga:date",
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
            "ga:browser",
            "ga:operatingSystem",
            "ga:screenResolution",
            "ga:screenColors",
            "ga:hostname"
        ],
        "default_dimensions": [
            "ga:date",
            "ga:browser",
            "ga:operatingSystem",
        ]
    },
    {
        "name": "Acquisition Overview",
        "metrics": [
            "ga:sessions",
            "ga:users",
            "ga:newUsers",
            "ga:sessionDuration"
            "ga:avgSessionDuration",
            "ga:bounceRate",
            "ga:pageviews",
            "ga:pageviewsPerSession",
            "ga:goalConversionRateAll"
        ],
        "dimensions": [
            "ga:date",
            "ga:medium",
            "ga:source",
            "ga:sourceMedium",
            "ga:campaign",
            "ga:channelGrouping",
        ],
        "default_dimensions": [
            "ga:acquisitionTrafficChannel",
            "ga:acquisitionSource",
            "ga:acquisitionSourceMedium",
            "ga:acquisitionMedium",
        ]
    },
    {
        "name": "Behavior Overview",
        "metrics": [
            "ga:pageviews",
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
            "ga:pagePath",
            "ga:eventCategory",
            "ga:eventAction",
        ],
        "default_dimensions": [
            "ga:date",
            "ga:pagePath",
            "ga:pageTitle"
        ]
    },
    {
        "name": "Ecommerce Overview",
        "metrics": ["ga:transactions"],
        "dimensions": [
            "ga:transactionId",
            "ga:campaign",
            "ga:source",
            "ga:medium",
            "ga:keyword",
            "ga:socialNetwork"
        ],
        "default_dimensions": [
            "ga:transactionId",
            "ga:campaign",
            "ga:source",
            "ga:medium",
            "ga:keyword",
            "ga:socialNetwork"
        ]
    }
]

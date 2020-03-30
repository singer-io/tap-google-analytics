PREMADE_REPORTS = [
    {
        # Report: {name: "Audience Overview",
        #          metrics: [...],
        #          dimensions: [...],
        #          selected_by_default: [...]}
        # selected_by_default = metrics[:10] + dimensions[:7]
        # TODO: maybe selected by default when we get a use case for it?
        "name": "Audience Overview",
        "metrics": ["ga:adClicks"],
        "dimensions": ["ga:date"],
    }
]

# TODO: This should be used to handle error in the client
def has_insufficient_permission(response):
    return any([e for e in response.json().get("error", {}).get("errors", [])
                if e.get("reason") == "insufficientPermissions"])

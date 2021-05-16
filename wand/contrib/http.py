"""

Implement helpers around urllib to fetch or push 
data for HTTP server.

"""

import urllib

def send_request(url, path, json_body, username, password):
    """Send request with username and password. Leave json_body empty for a GET"""

    password_mgr = urllib.request.HTTPPasswordMgrWithDefaultRealm()
    password_mgr.add_password(None, url + path, username, password)
    handler = urllib.request.HTTPBasicAuthHandler(password_mgr)
    opener = urllib.request.build_opener(handler)
    opener.open(url + path)
    urllib.request.install_opener(opener)
    if json_body:
        data = urllib.parse.urlencode(json_body).encode("utf-8")
        req = urllib.request.Request(url + path, data=data)
    else:
        req = urllib.request.Request(url + path)
    response = urllib.request.urlopen(req)
    return response
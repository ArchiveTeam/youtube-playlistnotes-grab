dofile("table_show.lua")
dofile("urlcode.lua")
JSON = (loadfile "JSON.lua")()
local urlparse = require("socket.url")
local http = require("socket.http")

local item_value = os.getenv('item_value')
local item_type = os.getenv('item_type')
local item_dir = os.getenv('item_dir')
local warc_file_base = os.getenv('warc_file_base')

local url_count = 0
local tries = 0
local downloaded = {}
local addedtolist = {}
local abortgrab = false

local discovered = {}

local client_version = nil
local client_name = nil

for ignore in io.open("ignore-list", "r"):lines() do
  downloaded[ignore] = true
end

load_json_file = function(file)
  if file then
    return JSON:decode(file)
  else
    return nil
  end
end

read_file = function(file)
  if file then
    local f = assert(io.open(file))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

discover_item = function(type_, name, tries)
  if tries == nil then
    tries = 0
  end
  name = urlparse.escape(urlparse.unescape(name))
  item = type_ .. ':' .. name
  if discovered[item] then
    return true
  end
  io.stdout:write("Discovered item " .. item .. ".\n")
  io.stdout:flush()
  local body, code, headers, status = http.request(
    "http://blackbird-amqp.meo.ws:23038/youtube-playlistnotes-ankl22ks7jav/",
    item
  )
  if code == 200 or code == 409 then
    discovered[item] = true
    return true
  elseif code == 404 then
    io.stdout:write("Project key not found.\n")
    io.stdout:flush()
  elseif code == 400 then
    io.stdout:write("Bad format.\n")
    io.stdout:flush()
  else
    io.stdout:write("Could not queue discovered item. Retrying...\n")
    io.stdout:flush()
    if tries == 10 then
      io.stdout:write("Maximum retries reached for sending discovered item.\n")
      io.stdout:flush()
    else
      os.execute("sleep " .. math.pow(2, tries))
      return discover_item(type_, name, tries + 1)
    end
  end
  abortgrab = true
  return false
end

allowed = function(url, parenturl)
  if string.match(url, "^https?://m%.youtube%.com/")
    or string.match(url, "[%?&]sort=")
    or string.match(url, "[%?&]flow=")
    or string.match(url, "^https?://[^/]+/yts/")
    or not string.match(url, "^https?://[^/]*youtube%.com/") then
    return false
  end

  if item_type == "c" or item_type == "user" or item_type == "channel"
    or item_type == "profile" then
    if string.match(url, "^https?://[^/]+/[^/]+/?[^/]*/playlists")
      or string.match(url, "^https?://[^/]+/[^/]+/?[^/]*/channels") then
      return true
    end
    local match = string.match(url, "^https?://[^/]+/playlist%?list=([a-zA-Z0-9%-_]+)$")
    if match then
      discover_item("playlist", match)
    end
  end

  local sort, match = string.match(url, "^https?://[^/]+/([^/]+)/([^/%?&]+)$")
  if not sort or not match then
    sort = "profile"
    match = string.match(url, "^https?://[^/]+/([^/%?&]+)$")
  end
  if sort and match
    and (sort == "c" or sort == "channel" or sort == "user") then
    discover_item(sort, match)
  end

  if string.match(url, "/browse_ajax%?") or not parenturl then 
    return true
  end

  return false
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  return false
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}
  local html = nil
  
  downloaded[url] = true

  local function check(urla)
    local origurl = url
    local url = string.match(urla, "^([^#]+)")
    local url_ = string.match(url, "^(.-)%.?$")
    url_ = string.gsub(url_, "\\?u0026", "&")
    url_ = string.gsub(url_, "&amp;", "&")
    url_ = string.gsub(url_, "sort=[a-z]+&?", "")
    url_ = string.gsub(url_, "flow=[a-z]+&?", "")
    if (string.match(url_, "/channels") or string.match(url_, "/playlists"))
      and not string.match(url_, "disable_polymer") then
      if not string.match(url_, "%?") then
        url_ = url_ .. "?"
      elseif not string.match(url_, "&$") then
        url_ = url_ .. "&"
      end
      url_ = url_ .. "disable_polymer=1"
    end
    if (downloaded[url_] ~= true and addedtolist[url_] ~= true)
      and allowed(url_, origurl) then
      if string.match(url_, "/browse_ajax%?") then
        if not client_name or not client_version then
          io.stdout:write("No client_name or client_version set.\n")
          io.stdout:flush()
          abortgrab = True
          return None
        end
        table.insert(urls, {
          url=url_,
          headers={
            ["X-YouTube-Client-Name"]=client_name,
            ["X-YouTube-Client-Version"]=client_version,
            ["Accept-Language"]="en-US,en;q=0.7"
          }
        })
      else
        table.insert(urls, {
          url=url_,
          headers={["Accept-Language"]="en-US,en;q=0.7"}
        })
      end
      addedtolist[url_] = true
      addedtolist[url] = true
    end
  end

  local function checknewurl(newurl)
    if string.match(newurl, "^https?:////") then
      check(string.gsub(newurl, ":////", "://"))
    elseif string.match(newurl, "^https?://") then
      check(newurl)
    elseif string.match(newurl, "^https?:\\/\\?/") then
      check(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^//") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^/") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^%.%./") then
      if string.match(url, "^https?://[^/]+/[^/]+/") then
        check(urlparse.absolute(url, newurl))
      else
        checknewurl(string.match(newurl, "^%.%.(/.+)$"))
      end
    elseif string.match(newurl, "^%./") then
      check(urlparse.absolute(url, newurl))
    end
  end

  if allowed(url, nil) and status_code == 200 then
    html = read_file(file)
    if string.match(url, "/browse_ajax%?") then
      local data = load_json_file(html)
      if not data or not data["content_html"] or data["reload"] or data["errors"] then
        io.stdout:write("Bad browse_ajax response.\n")
        io.stdout:flush()
        abortgrab = true
      end
      html = string.gsub(html, "\\", "")
    end
    local match_client_version = string.match(html, 'INNERTUBE_CONTEXT_CLIENT_VERSION%s*:%s*"([^"]+)"')
    local match_client_name = string.match(html, 'INNERTUBE_CONTEXT_CLIENT_NAME%s*:%s*([0-9]+)')
    if match_client_version and match_client_name then
      client_version = match_client_version
      client_name = match_client_name
    end
    for newurl in string.gmatch(string.gsub(html, "&quot;", '"'), '([^"]+)') do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(string.gsub(html, "&#039;", "'"), "([^']+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, ">%s*([^<%s]+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, "href='([^']+)'") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, "[^%-]href='([^']+)'") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, '[^%-]href="([^"]+)"') do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, ":%s*url%(([^%)]+)%)") do
      checknewurl(newurl)
    end
  end

  return urls
end

wget.callbacks.write_to_warc = function(url, http_stat)
  if http_stat["statcode"] ~= 200 and http_stat["statcode"] ~= 404 then
    return false
  end
  return true
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]
  
  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. "  \n")
  io.stdout:flush()
  
  if status_code >= 200 and status_code <= 399 then
    downloaded[url["url"]] = true
  end

  if abortgrab == true then
    io.stdout:write("ABORTING...\n")
    io.stdout:flush()
    return wget.actions.ABORT
  end

  if status_code ~= 200 and status_code ~= 404 then
    io.stdout:write("Server returned " .. http_stat.statcode .. " (" .. err .. "). Sleeping.\n")
    io.stdout:flush()
    local maxtries = 12
    if not allowed(url["url"], nil) then
      maxtries = 3
    end
    if tries >= maxtries then
      io.stdout:write("I give up...\n")
      io.stdout:flush()
      tries = 0
      if maxtries == 3 then
        return wget.actions.EXIT
      else
        return wget.actions.ABORT
      end
    else
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
      return wget.actions.CONTINUE
    end
  end

  tries = 0

  local sleep_time = 0

  if sleep_time > 0.001 then
    os.execute("sleep " .. sleep_time)
  end

  return wget.actions.NOTHING
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if abortgrab == true then
    return wget.exits.IO_FAIL
  end
  return exit_status
end


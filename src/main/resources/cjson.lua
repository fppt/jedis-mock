-- global cjson object which simply delegates to dkjson

local json = require("dkjson")

return {
  encode = function(data)
    return json.encode(data)
  end,

  decode = function(data)
    return json.decode(data)
  end
}

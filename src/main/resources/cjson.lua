-- global cjson object which simply delegates to dkjson

local json = require("dkjson")

return {
  encode = function(data)
    local encoded = json.encode(data)
    if encoded == '[]' then
      return '{}'
    end
    return encoded
  end,

  decode = function(data)
    return json.decode(data)
  end
}

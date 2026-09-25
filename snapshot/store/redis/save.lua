local key = KEYS[1]
local new_rev = tonumber(ARGV[1])
local payload = ARGV[2]

local existing = redis.call('GET', key)
if existing then
    local ok, decoded = pcall(cjson.decode, existing)
    if ok and decoded and decoded.revision and tonumber(decoded.revision) > new_rev then
        return 'OK'
    end
end

redis.call('SET', key, payload)
return 'OK'

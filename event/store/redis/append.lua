local revision_key = KEYS[1]
local stream_key = KEYS[2]
local global_pos_key = KEYS[3]
local global_stream_key = KEYS[4]

local expected_revision = tonumber(ARGV[1])
local event_count = tonumber(ARGV[2])

local current_rev_str = redis.call('GET', revision_key)
local current_revision = 0
if current_rev_str then
    current_revision = tonumber(current_rev_str)
end

if current_revision ~= expected_revision then
    return redis.error_reply("ERR_CONCURRENCY: expected revision " .. expected_revision .. ", got " .. current_revision)
end

local new_revision = current_revision
for i = 1, event_count do
    local payload = ARGV[2 + i]
    new_revision = redis.call('INCR', revision_key)
    local new_global = redis.call('INCR', global_pos_key)

    local stream_id = tostring(new_revision) .. "-0"
    local global_id = tostring(new_global) .. "-0"

    redis.call('XADD', stream_key, stream_id, 'data', payload)
    redis.call('XADD', global_stream_key, global_id, 'data', payload)
end

return new_revision

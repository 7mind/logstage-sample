-- ALL LOGS for user id for GoodFellows

select timestamp, log->>'@message'
 from structured_logs where
 jsonb(log->>'@context')->>'ad' = 'GoodFellows'
 and  jsonb(log->>'@context')->>'user_id' = '1'

 -- All users where rate limit exceeded

 select timestamp, jsonb(log->>'@context')->>'user_id'
 from structured_logs where  jsonb(log->>'@context')->>'ad' = 'BadGuys' and log->>'@message' = 'Rate limit exceeded'
